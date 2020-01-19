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
	//fmt.Println("---- 1 msg:", _iqel, " msg:", _iqel.evt)
	err = c.send(_iqel.packet(false), true)
	//fmt.Println("---- 2 msg:", _iqel, " msg:", _iqel.evt)
	if err != nil {
		//fmt.Println("---- 3 msg:", _iqel, " msg:", _iqel.evt, " err:", err)
		return err
	}
	select {
	case c.resender.c <- _iqel:
		//fmt.Println("---- 4 msg:", _iqel, " msg:", _iqel.evt)
		return nil
	case <-c.tomb.Dying():
		return nil
	}
}

func (c *ClientMQTT) sending(evt *common.Event) error {
	switch evt.Context.QOS {
	case 0:
		if err := c.publishQOS0(evt); err != nil {
			return err
		}
	case 1:
		if err := c.publishQOS1(evt); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClientMQTT) resending(i *iqel) error {
	//c.log.Info("client starts to republish messages", log.Any("interval", c.resender.d))
	//defer c.log.Info("client has stopped republishing messages")
	return c.resender.resending(i, func(i *iqel) error {
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
