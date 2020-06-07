package session

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
)

type dispatcher struct {
	interval time.Duration
	cli      *Client
	qos0     <-chan *common.Event
	qos1     <-chan *common.Event
	queue    chan *eventWrapper
	cache    sync.Map
	tomb     utils.Tomb
	log      *log.Logger

	wrap func(*common.Event) *eventWrapper
}

func newDispatcher(c *Client) *dispatcher {
	d := &dispatcher{
		interval: c.mgr.cfg.ResendInterval,
		cli:      c,
		qos0:     c.session.qos0.Chan(),
		qos1:     c.session.qos1.Chan(),
		queue:    make(chan *eventWrapper, c.mgr.cfg.MaxInflightQOS1Messages),
		log:      c.log.With(log.Any("session", "dispatcher")),
	}
	d.wrap = func(m *common.Event) *eventWrapper {
		return newEventWrapper(uint64(c.session.cnt.NextID()), 1, m)
	}
	d.tomb.Go(d.resending, d.sending)
	return d
}

func (d *dispatcher) close() {
	d.log.Info("dispatcher is closing")
	defer d.log.Info("dispatcher has closed")

	d.tomb.Kill(nil)
	d.tomb.Wait()
}

func (d *dispatcher) next(m *eventWrapper) time.Duration {
	return d.interval - time.Now().Sub(m.lst)
}

func (d *dispatcher) store(m *eventWrapper) error {
	if prev, ok := d.cache.LoadOrStore(m.id, m); ok && prev != m {
		prev.(*eventWrapper).Done()
		return ErrSessionClientPacketIDConflict
	}
	return nil
}

func (d *dispatcher) delete(id uint64) error {
	m, ok := d.cache.Load(id)
	if !ok {
		return ErrSessionClientPacketNotFound
	}
	d.cache.Delete(id)
	m.(*eventWrapper).Done()
	return nil
}

func (d *dispatcher) sending() error {
	d.log.Info("dispatcher starts to send messages")
	defer d.log.Info("dispatcher has stopped sending messages")

	var msg *eventWrapper
	for {
		if msg != nil {
			if err := d.cli.sendEvent(msg, false); err != nil {
				d.log.Debug("failed to send message", log.Error(err))
				return nil
			}
			if msg.qos == 1 {
				select {
				case d.queue <- msg:
				case <-d.tomb.Dying():
					return nil
				}
			}
		}
		select {
		case evt := <-d.qos0:
			if ent := d.log.Check(log.DebugLevel, "queue popped a message as qos 0"); ent != nil {
				ent.Write(log.Any("message", evt.String()))
			}
			msg = newEventWrapper(0, 0, evt)
		case evt := <-d.qos1:
			if ent := d.log.Check(log.DebugLevel, "queue popped a message as qos 1"); ent != nil {
				ent.Write(log.Any("message", evt.String()))
			}
			msg = d.wrap(evt)
			if err := d.store(msg); err != nil {
				d.log.Error(err.Error())
			}
		case <-d.tomb.Dying():
			return nil
		}
	}
}

func (d *dispatcher) resending() error {
	d.log.Info("dispatcher starts to resend messages", log.Any("interval", d.interval))
	defer d.log.Info("dispatcher has stopped resending messages")

	var msg *eventWrapper
	timer := time.NewTimer(d.interval)
	defer timer.Stop()
	for {
		if msg != nil {
			select {
			case <-timer.C:
			default:
			}
			for timer.Reset(d.next(msg)); msg.Wait(timer.C, d.tomb.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(d.interval) {
				if err := d.cli.sendEvent(msg, true); err != nil {
					d.log.Debug("failed to resend message", log.Error(err))
					return nil
				}
			}
		}
		select {
		case msg = <-d.queue:
		case <-d.tomb.Dying():
			return nil
		}
	}
}
