package session

import (
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

func (c *ClientMQTT) sending(i *iqel) (err error) {
	return c.send(i.packet(false), true)
}

func (c *ClientMQTT) resending(i *iqel) error {
	return c.send(i.packet(true), true)
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
