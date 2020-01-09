package session

import (
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

func (c *ClientMQTT) receiving() error {
	c.log.Info("client starts to receive messages")
	defer c.log.Info("client has stopped receiving messages")

	pkt, err := c.connection.Receive()
	if err != nil {
		c.die("failed to receive packet at first time", err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
		ent.Write(log.Any("packet", pkt.String()))
	}
	p, ok := pkt.(*mqtt.Connect)
	if !ok {
		c.die(ErrSessionClientPacketUnexpected.Error(), ErrSessionClientPacketUnexpected)
		return ErrSessionClientPacketUnexpected
	}
	if err = c.onConnect(p); err != nil {
		c.die("failed to hanle connect packet", err)
		return err
	}

	for {
		pkt, err = c.connection.Receive()
		if err != nil {
			c.die("failed to receive packet", err)
			return err
		}
		if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
			ent.Write(log.Any("packet", pkt.String()))
		}
		switch p := pkt.(type) {
		case *mqtt.Publish:
			err = c.onPublish(p)
		case *mqtt.Puback:
			err = c.onPuback(p)
		case *mqtt.Subscribe:
			err = c.onSubscribe(p)
		case *mqtt.Pingreq:
			err = c.onPingreq(p)
		case *mqtt.Unsubscribe:
			err = c.onUnsubscribe(p)
		case *mqtt.Pingresp:
			err = nil // just ignore
		case *mqtt.Disconnect:
			c.die("", nil)
			return nil
		case *mqtt.Connect:
			err = ErrSessionClientAlreadyConnecting
		default:
			err = ErrSessionClientPacketUnexpected
		}

		if err != nil {
			c.die("failed to handle packet", err)
			return err
		}
	}
}

func (c *ClientMQTT) onConnect(p *mqtt.Connect) error {
	si := Info{
		ID:           p.ClientID,
		CleanSession: p.CleanSession,
	}
	if p.Version != mqtt.Version31 && p.Version != mqtt.Version311 {
		c.sendConnack(mqtt.InvalidProtocolVersion, false)
		return ErrSessionProtocolVersionInvalid
	}
	if !checkClientID(si.ID) {
		c.sendConnack(mqtt.IdentifierRejected, false)
		return ErrSessionClientIDInvalid
	}
	if c.manager.authenticator != nil {
		// TODO: support tls bidirectional authentication, use CN as username

		// username/password authentication
		if p.Username == "" {
			c.sendConnack(mqtt.BadUsernameOrPassword, false)
			return ErrSessionUsernameNotSet
		}
		if p.Password == "" {
			c.sendConnack(mqtt.BadUsernameOrPassword, false)
			return ErrSessionPasswordNotSet
		}
		c.authorizer = c.manager.authenticator.AuthenticateAccount(p.Username, p.Password)
		if c.authorizer == nil {
			c.sendConnack(mqtt.BadUsernameOrPassword, false)
			return ErrSessionUsernameNotPermitted
		}
	}

	if p.Will != nil {
		if p.Will.QOS > 1 {
			return ErrSessionWillMessageQosNotSupported
		}
		if !c.manager.checker.CheckTopic(p.Will.Topic, false) {
			return ErrSessionWillMessageTopicInvalid
		}
		if !c.authorize(Publish, p.Will.Topic) {
			c.sendConnack(mqtt.NotAuthorized, false)
			return ErrSessionWillMessageTopicNotPermitted
		}
		si.Will = common.NewMessage(&mqtt.Publish{Message: *p.Will})
	}
	if si.ID == "" {
		si.ID = c.id
		si.CleanSession = true
	}

	exists, err := c.manager.initSession(&si, c, true)
	if err != nil {
		return err
	}
	c.tomb.Go(c.republishing, c.publishing)

	// TODO: Re-check subscriptions, if subscription not permit, log error and skip
	err = c.sendConnack(mqtt.ConnectionAccepted, exists)
	if err != nil {
		return err
	}

	c.log.Info("client is connected")
	return nil
}

func (c *ClientMQTT) onPublish(p *mqtt.Publish) error {
	// TODO: improvement, cache auth result
	if p.Message.QOS > 1 {
		return ErrSessionMessageQosNotSupported
	}
	if !c.manager.checker.CheckTopic(p.Message.Topic, false) {
		return ErrSessionMessageTopicInvalid
	}
	if !c.authorize(Publish, p.Message.Topic) {
		return ErrSessionMessageTopicNotPermitted
	}
	msg := common.NewMessage(p)
	if msg.Retain() {
		err := c.retainMessage(msg)
		if err != nil {
			return err
		}
		// change to normal message before exchange
		msg.Context.Type = link.Msg
	}
	cb := c.callback
	if p.Message.QOS == 0 {
		cb = nil
	}
	c.manager.exchange.Route(msg, cb)
	return nil
}

func (c *ClientMQTT) onPuback(p *mqtt.Puback) error {
	err := c.resender.delete(p.ID)
	if err != nil {
		c.log.Warn(err.Error(), log.Any("pid", int(p.ID)))
	}
	return nil
}

func (c *ClientMQTT) onSubscribe(p *mqtt.Subscribe) error {
	sa, subs := c.genSuback(p)
	err := c.manager.subscribe(c, subs)
	if err != nil {
		return err
	}
	err = c.send(sa, false)
	if err != nil {
		return err
	}
	return c.sendRetainMessage()
}

func (c *ClientMQTT) onUnsubscribe(p *mqtt.Unsubscribe) error {
	usa := mqtt.NewUnsuback()
	usa.ID = p.ID
	err := c.manager.unsubscribe(c, p.Topics)
	if err != nil {
		return err
	}
	return c.send(usa, false)
}

func (c *ClientMQTT) onPingreq(p *mqtt.Pingreq) error {
	return c.send(mqtt.NewPingresp(), false)
}

func (c *ClientMQTT) callback(id uint64) {
	c.send(&mqtt.Puback{ID: mqtt.ID(id)}, true)
}

func (c *ClientMQTT) genSuback(p *mqtt.Subscribe) (*mqtt.Suback, []mqtt.Subscription) {
	sa := &mqtt.Suback{
		ID:          p.ID,
		ReturnCodes: make([]mqtt.QOS, len(p.Subscriptions)),
	}
	subs := []mqtt.Subscription{}
	for i, sub := range p.Subscriptions {
		if !c.manager.checker.CheckTopic(sub.Topic, true) {
			c.log.Error("subscribe topic invalid", log.Any("topic", sub.Topic))
			sa.ReturnCodes[i] = mqtt.QOSFailure
		} else if sub.QOS > 1 {
			c.log.Error("subscribe QOS not supported", log.Any("qos", int(sub.QOS)))
			sa.ReturnCodes[i] = mqtt.QOSFailure
		} else if !c.authorize(Subscribe, sub.Topic) {
			c.log.Error("subscribe topic not permitted", log.Any("topic", sub.Topic))
			sa.ReturnCodes[i] = mqtt.QOSFailure
		} else {
			sa.ReturnCodes[i] = sub.QOS
			subs = append(subs, sub)
		}
	}
	return sa, subs
}
