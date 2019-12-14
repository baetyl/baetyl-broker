package session

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
)

type epl struct {
	e *common.Event
	p common.Publish
	l time.Time // last send time
}

func newEPL(id common.ID, qos common.QOS, e *common.Event) *epl {
	pkt := common.Publish{ID: id}
	pkt.Message.QOS = qos
	pkt.Message.Topic = e.Context.Topic
	pkt.Message.Payload = e.Content
	return &epl{
		e: e,
		p: pkt,
		l: time.Now(),
	}
}

type publisher struct {
	d time.Duration
	m sync.Map
	c chan *epl
	n *common.Counter // Only used by mqtt client
}

func newPublisher(d time.Duration, c int) *publisher {
	return &publisher{
		d: d,
		c: make(chan *epl, c),
		n: common.NewCounter(),
	}
}

func (c *ClientMQTT) publishQOS0(e *common.Event) error {
	pkt := &common.Publish{}
	pkt.Message.Topic = e.Context.Topic
	pkt.Message.Payload = e.Content
	return c.send(pkt, true)
}

func (c *ClientMQTT) publishQOS1(e *common.Event) error {
	id := c.publisher.n.NextID()
	_epl := newEPL(id, 1, e)
	if o, ok := c.publisher.m.LoadOrStore(id, _epl); ok {
		c.log.Error("packet id conflict, to acknowledge old one")
		o.(*epl).e.Done()
	}
	err := c.send(&_epl.p, true)
	if err != nil {
		return err
	}
	select {
	case c.publisher.c <- _epl:
		return nil
	case <-c.Dying():
		return ErrClientClosed
	}
}

func (c *ClientMQTT) republishQOS1(pkt common.Publish) error {
	pkt.Dup = true
	return c.send(&pkt, true)
}

func (c *ClientMQTT) acknowledge(p *common.Puback) {
	m, ok := c.publisher.m.Load(p.ID)
	if !ok {
		c.log.Warn("puback not found", log.Int("id", int(p.ID)))
		return
	}
	c.publisher.m.Delete(p.ID)
	m.(*epl).e.Done()
}

func (c *ClientMQTT) publishing() (err error) {
	c.log.Info("client starts to publish messages")
	defer c.log.Info("client has stopped publishing messages")

	var e *common.Event
	qos0 := c.session.qos0.Chan()
	qos1 := c.session.qos1.Chan()
	for {
		select {
		case e = <-qos0:
			if err := c.publishQOS0(e); err != nil {
				return err
			}
		case e = <-qos1:
			if err := c.publishQOS1(e); err != nil {
				return err
			}
		case <-c.Dying():
			return nil
		}
	}
}

func (c *ClientMQTT) republishing() error {
	c.log.Info("client starts to republish messages", log.Duration("interval", c.publisher.d))
	defer c.log.Info("client has stopped republishing messages")

	var _epl *epl
	timer := time.NewTimer(c.publisher.d)
	defer timer.Stop()
	for {
		select {
		case _epl = <-c.publisher.c:
			for timer.Reset(c.publisher.d - time.Now().Sub(_epl.l)); _epl.e.Wait(timer.C, c.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(c.publisher.d) {
				if err := c.republishQOS1(_epl.p); err != nil {
					return err
				}
			}
		case <-c.Dying():
			return nil
		}
	}
}

func (c *ClientMQTT) sendConnack(code common.ConnackCode, exists bool) error {
	ack := &common.Connack{
		SessionPresent: exists,
		ReturnCode:     code,
	}
	return c.send(ack, false)
}

func (c *ClientMQTT) send(pkt common.Packet, async bool) error {
	// TODO: remove lock
	c.Lock()
	err := c.connection.Send(pkt, async)
	c.Unlock()
	if err != nil {
		c.log.Error("failed to send packet", log.Error(err))
		c.die(err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client sent a packet"); ent != nil {
		ent.Write(log.String("packet", pkt.String()))
	}
	return nil
}
