package session

import (
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

func (c *ClientMQTT) publishQOS0(e *common.Event) error {
	pkt := e.Packet()
	pkt.Message.QOS = 0
	return c.send(pkt, true)
}

func (c *ClientMQTT) publishQOS1(e *common.Event) error {
	_iqel := newIQEL(c.counter.NextID(), 1, e)
	err := c.resender.store(_iqel)
	if err != nil {
		c.log.Error(err.Error())
	}
	err = c.send(_iqel.packet(false), true)
	if err != nil {
		return err
	}
	select {
	case c.resender.c <- _iqel:
		return nil
	case <-c.tomb.Dying():
		return nil
	}
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
		case <-c.tomb.Dying():
			return nil
		}
	}
}

func (c *ClientMQTT) republishing() error {
	c.log.Info("client starts to republish messages", log.Any("interval", c.resender.d))
	defer c.log.Info("client has stopped republishing messages")
	return c.resender.resending(func(i *iqel) error {
		return c.send(i.packet(true), true)
	})
}

func (c *ClientMQTT) sendConnack(code mqtt.ConnackCode, exists bool) error {
	ack := &mqtt.Connack{
		SessionPresent: exists,
		ReturnCode:     code,
	}
	return c.send(ack, false)
}

func (c *ClientMQTT) send(pkt mqtt.Packet, async bool) error {
	c.mu.Lock()
	err := c.connection.Send(pkt, async)
	c.mu.Unlock()
	if err != nil {
		c.die("failed to send packet", err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client sent a packet"); ent != nil {
		ent.Write(log.Any("packet", pkt.String()))
	}
	return nil
}
