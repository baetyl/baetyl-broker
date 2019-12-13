package session

import (
	"errors"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
)

func (c *ClientMQTT) receiving() error {
	c.log.Info("client starts to receive messages")
	defer c.log.Info("client has stopped receiving messages")

	var err error
	var pkt common.Packet

	pkt, err = c.connection.Receive()
	if err != nil {
		c.die(err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
		ent.Write(log.String("packet", pkt.String()))
	}
	p, ok := pkt.(*common.Connect)
	if !ok {
		c.die(ErrPacketUnexpected)
		return ErrPacketUnexpected
	}
	if err = c.onConnect(p); err != nil {
		c.die(err)
		return err
	}

	for {
		pkt, err = c.connection.Receive()
		if err != nil {
			c.die(err)
			return err
		}
		if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
			ent.Write(log.String("packet", pkt.String()))
		}
		switch p := pkt.(type) {
		case *common.Publish:
			err = c.onPublish(p)
		case *common.Puback:
			err = c.onPuback(p)
		case *common.Subscribe:
			err = c.onSubscribe(p)
		case *common.Pingreq:
			err = c.onPingreq(p)
		case *common.Unsubscribe:
			err = c.onUnsubscribe(p)
		case *common.Pingresp:
			err = nil // just ignore
		case *common.Disconnect:
			c.die(nil)
			return nil
		case *common.Connect:
			err = ErrConnectUnexpected
		default:
			err = ErrPacketUnexpected
		}

		if err != nil {
			c.die(err)
			return err
		}
	}
}

func (c *ClientMQTT) onConnect(p *common.Connect) error {
	si := Info{
		ID:           p.ClientID,
		CleanSession: p.CleanSession,
	}
	if p.Version != common.Version31 && p.Version != common.Version311 {
		c.sendConnack(common.InvalidProtocolVersion, false)
		return errors.New("protocol version invalid")
	}
	if !checkClientID(si.ID) {
		c.sendConnack(common.IdentifierRejected, false)
		return errors.New("client ID invalid")
	}
	if !c.anonymous && c.manager.auth != nil {
		// TODO: support tls bidirectional authentication, use CN as username

		// username/password authentication
		if p.Username == "" {
			c.sendConnack(common.BadUsernameOrPassword, false)
			return errors.New("username not set")
		}
		if p.Password == "" {
			c.sendConnack(common.BadUsernameOrPassword, false)
			return errors.New("password not set")
		}
		c.authorizer = c.manager.auth.AuthenticateAccount(p.Username, p.Password)
		if c.authorizer == nil {
			c.sendConnack(common.BadUsernameOrPassword, false)
			return errors.New("username or password not permitted")
		}
	}

	if p.Will != nil {
		if !common.CheckTopic(p.Will.Topic, false) {
			return errors.New("will topic invalid")
		}
		if !c.authorize(auth.Publish, p.Will.Topic) {
			c.sendConnack(common.NotAuthorized, false)
			return errors.New("will topic not permitted")
		}
		if p.Will.QOS > 1 {
			return errors.New("will QoS not supported")
		}
		si.Will = common.NewMessage(&common.Publish{Message: *p.Will})
	}
	if si.ID == "" {
		si.ID = c.id
		si.CleanSession = true
	}

	exists, err := c.manager.initSession(&si, c, true)
	if err != nil {
		return err
	}
	c.Go(c.republishing, c.publishing)

	// TODO: Re-check subscriptions, if subscription not permit, log error and skip
	err = c.sendConnack(common.ConnectionAccepted, exists)
	if err != nil {
		return err
	}

	c.log.Info("client is connected")
	return nil
}

func (c *ClientMQTT) onPublish(p *common.Publish) error {
	// TODO: improvement, cache auth result
	if p.Message.QOS > 1 {
		return errors.New("publish QOS not supported")
	}
	if !common.CheckTopic(p.Message.Topic, false) {
		return errors.New("publish topic invalid")
	}
	if !c.authorize(auth.Publish, p.Message.Topic) {
		return errors.New("publish topic not permitted")
	}
	msg := common.NewMessage(p)
	if msg.Retain() {
		err := c.retainMessage(msg)
		if err != nil {
			return err
		}
	}
	cb := c.callback
	if p.Message.QOS == 0 {
		cb = nil
	}
	c.manager.exchange.Route(msg, cb)
	return nil
}

func (c *ClientMQTT) onPuback(p *common.Puback) error {
	c.acknowledge(p)
	return nil
}

func (c *ClientMQTT) onSubscribe(p *common.Subscribe) error {
	sa, subs := c.genSuback(p)
	err := c.manager.subscribe(c, subs)
	if err != nil {
		return err
	}
	err = c.send(sa, true)
	if err != nil {
		return err
	}
	return c.sendRetainMessage()
}

func (c *ClientMQTT) onUnsubscribe(p *common.Unsubscribe) error {
	usa := &common.Unsuback{}
	usa.ID = p.ID
	err := c.manager.unsubscribe(c, p.Topics)
	if err != nil {
		return err
	}
	return c.send(usa, true)
}

func (c *ClientMQTT) onPingreq(p *common.Pingreq) error {
	return c.send(&common.Pingresp{}, true)
}

func (c *ClientMQTT) callback(id uint64) {
	c.send(&common.Puback{ID: common.ID(id)}, false)
}

func (c *ClientMQTT) genSuback(p *common.Subscribe) (*common.Suback, []common.Subscription) {
	sa := &common.Suback{
		ID:          p.ID,
		ReturnCodes: make([]common.QOS, len(p.Subscriptions)),
	}
	subs := []common.Subscription{}
	for i, sub := range p.Subscriptions {
		if !common.CheckTopic(sub.Topic, true) {
			c.log.Error("subscribe topic invalid", log.String("topic", sub.Topic))
			sa.ReturnCodes[i] = common.QOSFailure
		} else if sub.QOS > 1 {
			c.log.Error("subscribe QOS not supported", log.Int("qos", int(sub.QOS)))
			sa.ReturnCodes[i] = common.QOSFailure
		} else if !c.authorize(auth.Subscribe, sub.Topic) {
			c.log.Error("subscribe topic not permitted", log.String("topic", sub.Topic))
			sa.ReturnCodes[i] = common.QOSFailure
		} else {
			sa.ReturnCodes[i] = sub.QOS
			subs = append(subs, sub)
		}
	}
	return sa, subs
}
