package session

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
)

type iqel struct {
	id  interface{}
	qos mqtt.QOS
	evt *common.Event
	lst time.Time // last send time
}

func newIQEL(id interface{}, qos mqtt.QOS, evt *common.Event) *iqel {
	return &iqel{
		id:  id,
		qos: qos,
		evt: evt,
		lst: time.Now(),
	}
}

func (i *iqel) wait(timeout <-chan time.Time, cancel <-chan struct{}) error {
	return i.evt.Wait(timeout, cancel)
}

func (i *iqel) message() *link.Message {
	return i.evt.Message
}

func (i *iqel) packet(dup bool) *mqtt.Publish {
	pkt := i.evt.Packet()
	pkt.ID = i.id.(mqtt.ID)
	pkt.Dup = dup
	pkt.Message.QOS = i.qos
	return pkt
}

type resender struct {
	d time.Duration
	c chan *iqel
	m sync.Map
	t *utils.Tomb
}

func newResender(c int, d time.Duration, t *utils.Tomb) *resender {
	return &resender{
		c: make(chan *iqel, c),
		d: d,
		t: t,
	}
}

func (r *resender) next(i *iqel) time.Duration {
	return r.d - time.Now().Sub(i.lst)
}

func (r *resender) store(i *iqel) error {
	if o, ok := r.m.LoadOrStore(i.id, i); ok {
		o.(*iqel).evt.Done()
		return ErrSessionClientPacketIDConflict
	}
	return nil
}

func (r *resender) delete(id interface{}) error {
	m, ok := r.m.Load(id)
	if !ok {
		return ErrSessionClientPacketNotFound
	}
	r.m.Delete(id)
	m.(*iqel).evt.Done()
	return nil
}

func (r *resender) resending(send func(*iqel) error) error {
	var err error
	var _iqel *iqel
	timer := time.NewTimer(r.d)
	defer timer.Stop()
	for {
		select {
		case _iqel = <-r.c:
			for timer.Reset(r.next(_iqel)); _iqel.wait(timer.C, r.t.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(r.d) {
				if err = send(_iqel); err != nil {
					return err
				}
			}
		case <-r.t.Dying():
			return nil
		}
	}
}
