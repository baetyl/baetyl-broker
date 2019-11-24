package session

import (
	"errors"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
	"github.com/docker/distribution/uuid"
)

func (c *ClientMQTT) receiving() error {
	defer c.clean()

	c.log.Info("client starts to handle incoming messages")
	defer utils.Trace(c.log.Info, "client has stopped handling incoming messages")

	var err error
	var pkt common.Packet

	pkt, err = c.connection.Receive()
	if err != nil {
		if !c.Alive() {
			return nil
		}
		c.log.Warn("failed to receive packet", log.Error(err))
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client receives a packet"); ent != nil {
		ent.Write(log.String("packet", pkt.String()))
	}
	p, ok := pkt.(*common.Connect)
	if !ok {
		c.log.Error("only connect packet is allowed before auth", log.Error(err))
		return err
	}
	if err = c.onConnect(p); err != nil {
		c.log.Warn("failed to handle connect packet", log.Error(err))
		return err
	}

	for {
		if !c.Alive() {
			return nil
		}

		pkt, err = c.connection.Receive()
		if err != nil {
			if !c.Alive() {
				return nil
			}
			c.log.Warn("failed to receive packet", log.Error(err))
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
		case *common.Pingresp:
			err = nil // just ignore
		case *common.Disconnect:
			return nil
		case *common.Unsubscribe:
			err = c.onUnsubscribe(p)
		case *common.Connect:
			err = ErrConnectUnexpected
		default:
			err = ErrPacketUnexpected
		}

		if err != nil {
			c.log.Warn("failed to handle packet", log.Error(err))
			return err
		}
	}
}

func (c *ClientMQTT) onConnect(p *common.Connect) error {
	info := &Info{
		ID:           p.ClientID,
		CleanSession: p.CleanSession,
	}

	if p.Version != common.Version31 && p.Version != common.Version311 {
		c.sendConnack(common.InvalidProtocolVersion, false)
		return errors.New("protocol version invalid")
	}
	if !checkClientID(info.ID) {
		c.sendConnack(common.IdentifierRejected, false)
		return errors.New("client ID invalid")
	}
	if !c.anonymous && c.store.auth != nil {
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
		c.authorizer = c.store.auth.AuthenticateAccount(p.Username, p.Password)
		if c.authorizer == nil {
			c.sendConnack(common.BadUsernameOrPassword, false)
			return errors.New("username or password not permitted")
		}
	}

	if p.Will != nil {
		// TODO: remove?
		// if !common.PubTopicValidate(p.Will.Topic) {
		// 	return errors.New("will topic (%s) invalid", p.Will.Topic)
		// }
		// if !c.authorize(auth.Publish, p.Will.Topic) {
		// 	c.sendConnack(common.NotAuthorized)
		// 	return errors.New("will topic (%s) not permitted", p.Will.Topic)
		// }
		// if p.Will.QOS > 1 {
		// 	return errors.New("will QOS (%d) not supported", p.Will.QOS)
		// }
	}

	if info.ID == "" {
		info.ID = uuid.Generate().String()
		info.CleanSession = true
	}

	exists, err := c.store.initSession(info, c, true)
	if err != nil {
		return err
	}
	c.Go(c.waiting, c.publishing)

	// TODO: Re-check subscriptions, if subscription not permit, log error and skip
	// TODO: err = c.session.saveWillMessage(p)

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

	// TODO: err := c.retainMessage(&p.Message)
	// if err != nil {
	// 	return err
	// }

	msg := common.NewMessage(p)
	cb := c.callback
	if p.Message.QOS == 0 {
		cb = nil
	}
	c.store.exchange.Route(msg, cb)
	return nil
}

func (c *ClientMQTT) onPuback(p *common.Puback) error {
	c.acknowledge(p)
	return nil
}

func (c *ClientMQTT) onSubscribe(p *common.Subscribe) error {
	sa, subs := c.genSuback(p)
	err := c.store.subscribe(c.session, subs)
	if err != nil {
		return err
	}
	err = c.send(sa, true)
	if err != nil {
		return err
	}
	// TODO: return c.sendRetainMessage(p)
	return nil
}

func (c *ClientMQTT) onUnsubscribe(p *common.Unsubscribe) error {
	usa := &common.Unsuback{}
	usa.ID = p.ID
	err := c.store.unsubscribe(c.session, p.Topics)
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
			sa.ReturnCodes[i] = packet.QOSFailure
		} else if sub.QOS > 1 {
			c.log.Error("subscribe QOS not supported", log.Int("qos", int(sub.QOS)))
			sa.ReturnCodes[i] = packet.QOSFailure
		} else if !c.authorize(auth.Subscribe, sub.Topic) {
			c.log.Error("subscribe topic not permitted", log.String("topic", sub.Topic))
			sa.ReturnCodes[i] = packet.QOSFailure
		} else {
			sa.ReturnCodes[i] = sub.QOS
			subs = append(subs, sub)
		}
	}
	return sa, subs
}
