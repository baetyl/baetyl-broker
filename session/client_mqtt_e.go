package session

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

type pm struct {
	e *common.Event
	p *common.Publish
	l time.Time // last send time
}

type publisher struct {
	d time.Duration
	m sync.Map
	c chan *pm
	n *common.Counter
}

func newPublisher(d time.Duration, c int) *publisher {
	return &publisher{
		d: d,
		c: make(chan *pm, c),
		n: common.NewCounter(),
	}
}

func (c *ClientMQTT) publish(e *common.Event) error {
	m := &pm{
		e: e,
		p: e.Packet(),
		l: time.Now(),
	}
	m.p.ID = c.publisher.n.NextID()
	if o, ok := c.publisher.m.LoadOrStore(m.p.ID, m); ok {
		log.Error("packet id conflict, to acknowledge old one", nil)
		o.(*pm).e.Done()
	}
	err := c.send(m.p, true)
	if err != nil {
		return err
	}
	select {
	case c.publisher.c <- m:
		return nil
	case <-c.Dying():
		return ErrClientClosed
	}
}

func (c *ClientMQTT) republish(m *pm) error {
	m.p.Dup = true
	m.l = time.Now()
	return c.send(m.p, true)
}

func (c *ClientMQTT) acknowledge(p *common.Puback) {
	m, ok := c.publisher.m.Load(p.ID)
	if !ok {
		log.Warnf("puback(pid=%d) not found", p.ID)
	}
	c.publisher.m.Delete(p.ID)
	m.(*pm).e.Done()
}

func (c *ClientMQTT) publishing() (err error) {
	defer c.kill()

	log.Infof("client (%s) starts to publish messages", c.session.ID())
	defer utils.Trace(log.Infof, "client (%s) has stopped publishing messages", c.session.ID())

	var e *common.Event
	qos0 := c.session.qos0.Chan()
	qos1 := c.session.qos1.Chan()
	for {
		select {
		case e = <-qos0:
			err = c.send(e.Packet(), false)
		case e = <-qos1:
			err = c.publish(e)
		case <-c.Dying():
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *ClientMQTT) waiting() error {
	defer c.kill()

	log.Infof("client (%s) starts to wait for message acknowledgement", c.session.ID())
	defer utils.Trace(log.Infof, "client (%s) has stopped waiting", c.session.ID())

	var m *pm
	timer := time.NewTimer(c.publisher.d)
	defer timer.Stop()
	for {
		select {
		case m = <-c.publisher.c:
			timer.Reset(c.publisher.d - time.Now().Sub(m.l))
			for !m.e.Wait(timer.C, c.Dying()) {
				timer.Reset(c.publisher.d)
				if err := c.republish(m); err != nil {
					return err
				}
			}
		case <-c.Dying():
			return nil
		}
	}
}

func (c *ClientMQTT) sendConnack(code common.ConnackCode, exists bool) error {
	ack := common.Connack{
		SessionPresent: exists,
		ReturnCode:     code,
	}
	return c.send(&ack, false)
}

func (c *ClientMQTT) send(pkt common.Packet, async bool) error {
	// TODO: remove lock
	c.Lock()
	defer c.Unlock()

	err := c.connection.Send(pkt, async)
	if err != nil {
		log.Error("failed to send packet", err)
		c.Close()
		return err
	}
	if ent := log.Check(log.DebugLevel, "sent packet"); ent != nil {
		ent.Write(log.String("packet", pkt.String()))
	}
	return nil
}
