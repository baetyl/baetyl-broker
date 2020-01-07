package session

import (
	"errors"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

func (c *ClientMQTT) receiving() error {
	c.log.Info("client starts to receive messages")
	defer c.log.Info("client has stopped receiving messages")

	pkt, err := c.connection.Receive()
	if err != nil {
		c.die(err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
		ent.Write(log.Any("packet", pkt.String()))
	}
	p, ok := pkt.(*mqtt.Connect)
	if !ok {
		c.die(ErrSessionClientUnexpectedPacket)
		return ErrSessionClientUnexpectedPacket
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
			c.die(nil)
			return nil
		case *mqtt.Connect:
			err = ErrSessionClientAlreadyConnecting
		default:
			err = ErrSessionClientUnexpectedPacket
		}

		if err != nil {
			c.die(err)
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
		return errors.New("protocol version invalid")
	}
	if !checkClientID(si.ID) {
		c.sendConnack(mqtt.IdentifierRejected, false)
		return errors.New("client ID invalid")
	}
	if !c.anonymous && c.manager.auth != nil {
		// username/password authentication
		if p.Username == "" {
			c.sendConnack(mqtt.BadUsernameOrPassword, false)
			return errors.New("username not set")
		}
		if p.Password != "" {
			c.authorizer = c.manager.auth.AuthenticateAccount(p.Username, p.Password)
			if c.authorizer == nil {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return errors.New("username or password not permitted")
			}
		} else if mqtt.IsBidirectionalAuthentication(c.connection) {
			// if IsBidirectionalAuthentication, will use certificate authentication
			if tlsConn, ok := getTLSConn(c.connection); ok {
				cn, err := getCommonName(tlsConn)
				if err != nil {
					// TODO: add BadClientCertificate error info in baetyl-go
					c.sendConnack(mqtt.BadUsernameOrPassword, false)
					return ErrGetCommonName
				}
				c.authorizer = c.manager.auth.AuthenticateCert(cn)
				if c.authorizer == nil {
					c.sendConnack(mqtt.BadUsernameOrPassword, false)
					return errors.New("username is not permitted over ssl/tls")
				}
			}
		} else {
			c.sendConnack(mqtt.BadUsernameOrPassword, false)
			return errors.New("password not set")
		}
	}

	if p.Will != nil {
		if !c.manager.checker.CheckTopic(p.Will.Topic, false) {
			return errors.New("will topic invalid")
		}
		if !c.authorize(auth.Publish, p.Will.Topic) {
			c.sendConnack(mqtt.NotAuthorized, false)
			return errors.New("will topic not permitted")
		}
		if p.Will.QOS > 1 {
			return errors.New("will QoS not supported")
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
	c.Go(c.republishing, c.publishing)

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
		return errors.New("publish QOS not supported")
	}
	if !c.manager.checker.CheckTopic(p.Message.Topic, false) {
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

func (c *ClientMQTT) onPuback(p *mqtt.Puback) error {
	c.acknowledge(p)
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
		} else if !c.authorize(auth.Subscribe, sub.Topic) {
			c.log.Error("subscribe topic not permitted", log.Any("topic", sub.Topic))
			sa.ReturnCodes[i] = mqtt.QOSFailure
		} else {
			sa.ReturnCodes[i] = sub.QOS
			subs = append(subs, sub)
		}
	}
	return sa, subs
}
