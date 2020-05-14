package session

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
)

type dispatcher struct {
	interval time.Duration
	session  *Session
	queue    chan *eventWrapper
	cache    sync.Map
	count    *mqtt.Counter
	tomb     utils.Tomb
	log      *log.Logger

	wrap func(*common.Event) *eventWrapper
}

func newDispatcher(s *Session) *dispatcher {
	d := &dispatcher{
		session:  s,
		interval: s.mgr.cfg.ResendInterval,
		queue:    make(chan *eventWrapper, s.mgr.cfg.MaxInflightQOS1Messages),
		log:      s.log.With(log.Any("session", "dispatcher"), log.Any("id", s.info.ID)),
	}
	d.wrap = func(m *common.Event) *eventWrapper {
		return newEventWrapper(uint64(s.cnt.NextID()), 1, m)
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

	var msgs []*eventWrapper
	var clis []interface{}
	qos0 := d.session.qos0
	qos1 := d.session.qos1

	for {
		clis = d.session.clients.copy()
		if len(clis) == 0 {
			d.log.Debug("no client")
			return nil
		}
	LB:
		for _, c := range clis {
			for len(msgs) != 0 {
				msg := msgs[0]
				msgs = msgs[1:]
				if err := c.(client).sendEvent(msg, false); err != nil {
					d.log.Debug("failed to send message", log.Error(err), log.Any("cid", c.(client).getID()))
					continue LB
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
			case <-qos0.Chan():
				buf, err := qos0.Pop()
				if err != nil {
					d.log.Error("failed to pop messages from qos0 queue", log.Error(err))
					continue
				}

				for _, evt := range buf {
					if ent := d.log.Check(log.DebugLevel, "queue popped a message as qos 0"); ent != nil {
						ent.Write(log.Any("message", evt.String()))
					}
					msgs = append(msgs, newEventWrapper(0, 0, evt))
				}
			case <-qos1.Chan():
				buf, err := qos1.Pop()
				if err != nil {
					d.log.Error("failed to pop messages from qos1 queue", log.Error(err))
					continue
				}

				for _, evt := range buf {
					if ent := d.log.Check(log.DebugLevel, "queue popped a message as qos 1"); ent != nil {
						ent.Write(log.Any("message", evt.String()))
					}
					wrap := d.wrap(evt)
					if err := d.store(wrap); err != nil {
						d.log.Error(err.Error())
					}
					msgs = append(msgs, wrap)
				}
			case <-d.tomb.Dying():
				return nil
			}
		}
	}
}

func (d *dispatcher) resending() error {
	d.log.Info("dispatcher starts to resend messages", log.Any("interval", d.interval))
	defer d.log.Info("dispatcher has stopped resending messages")

	var msg *eventWrapper
	var clis []interface{}
	timer := time.NewTimer(d.interval)
	defer timer.Stop()
	for {
		clis = d.session.clients.copy()
		if len(clis) == 0 {
			d.log.Debug("no client")
			return nil
		}
	LB:
		for _, c := range clis {
			if msg != nil {
				for timer.Reset(d.next(msg)); msg.Wait(timer.C, d.tomb.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(d.interval) {
					if err := c.(client).sendEvent(msg, true); err != nil {
						continue LB
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
}
