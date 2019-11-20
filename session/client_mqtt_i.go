package session

import (
	"fmt"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
	"github.com/docker/distribution/uuid"
)

func (c *ClientMQTT) receiving() error {
	defer c.clean()

	log.Infof("client starts to handle incoming messages")
	defer utils.Trace(log.Infof, "client has stopped handling incoming messages")

	var err error
	var pkt common.Packet

	pkt, err = c.connection.Receive()
	if err != nil {
		if !c.Alive() {
			return nil
		}
		return log.Warn("failed to receive packet", err)
	}
	if ent := log.Check(log.DebugLevel, "received packet"); ent != nil {
		ent.Write(log.String("packet", pkt.String()))
	}
	p, ok := pkt.(*common.Connect)
	if !ok {
		return log.Error("only connect packet is allowed before auth", nil)
	}
	if err = c.onConnect(p); err != nil {
		return log.Warn("failed to handle connect packet", err)
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
			return log.Warn("failed to receive packet", err)
		}
		if ent := log.Check(log.DebugLevel, "received packet"); ent != nil {
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
			return log.Warn("failed to handle packet", err)
		}
	}
}

func (c *ClientMQTT) onConnect(p *common.Connect) error {
	info := &Info{
		ID:           p.ClientID,
		CleanSession: p.CleanSession,
	}

	// TODO: check auth
	if p.Version != common.Version31 && p.Version != common.Version311 {
		c.sendConnack(common.InvalidProtocolVersion, false)
		return fmt.Errorf("[%s] mqtt protocol version (%d) invalid", info.ID, p.Version)
	}
	// username must set
	if p.Username == "" {
		c.sendConnack(common.BadUsernameOrPassword, false)
		return fmt.Errorf("[%s] username not set", info.ID)
	}
	if p.Password != "" {
		// // if password is set, to use account auth
		// c.authorizer = c.manager.auth.AuthenticateAccount(p.Username, p.Password)
		// if c.authorizer == nil {
		// 	c.sendConnack(common.BadUsernameOrPassword)
		// 	return fmt.Errorf("username (%s) or password not permitted", p.Username)
		// }
	} else if utils.IsBidirectionalAuthentication(c.connection) {
		// // if it is two-way tls, to use cert auth
		// c.authorizer = c.manager.auth.AuthenticateCert(p.Username)
		// if c.authorizer == nil {
		// 	c.sendConnack(common.BadUsernameOrPassword)
		// 	return fmt.Errorf("username (%s) is not permitted over tls", p.Username)
		// }
	} else {
		c.sendConnack(common.BadUsernameOrPassword, false)
		return fmt.Errorf("[%s] password not set", info.ID)
	}

	if !checkClientID(info.ID) {
		c.sendConnack(common.IdentifierRejected, false)
		return fmt.Errorf("[%s] client ID invalid", info.ID)
	}
	if p.Will != nil {
		// // TODO: remove?
		// if !common.PubTopicValidate(p.Will.Topic) {
		// 	return fmt.Errorf("will topic (%s) invalid", p.Will.Topic)
		// }
		// if !c.authorizer.Authorize(auth.Publish, p.Will.Topic) {
		// 	c.sendConnack(common.NotAuthorized)
		// 	return fmt.Errorf("will topic (%s) not permitted", p.Will.Topic)
		// }
		// if p.Will.QOS > 1 {
		// 	return fmt.Errorf("will QOS (%d) not supported", p.Will.QOS)
		// }
	}
	if info.ID == "" {
		info.ID = uuid.Generate().String()
		info.CleanSession = true
	}

	exists, err := c.store.initSession(info, c)
	if err != nil {
		return err
	}
	c.Go(c.waiting)
	c.Go(c.publishing)

	// TODO: Re-check subscriptions, if subscription not permit, log error and skip
	// TODO: err = c.session.saveWillMessage(p)
	err = c.sendConnack(common.ConnectionAccepted, exists)
	if err != nil {
		return err
	}

	log.Infof("[%s ] client is connected", info.ID)
	return nil
}

func (c *ClientMQTT) onPublish(p *common.Publish) error {
	// if _, ok := c.permittedPublishTopics[p.Message.Topic]; !ok {
	// 	// TODO: remove?
	// 	if !common.PubTopicValidate(p.Message.Topic) {
	// 		return fmt.Errorf("publish topic (%s) invalid", p.Message.Topic)
	// 	}
	// 	if !c.authorizer.Authorize(auth.Publish, p.Message.Topic) {
	// 		return fmt.Errorf("publish topic (%s) not permitted", p.Message.Topic)
	// 	}
	// 	c.permittedPublishTopics[p.Message.Topic] = struct{}{}
	// }
	if p.Message.QOS > 1 {
		return fmt.Errorf("publish QOS (%d) not supported", p.Message.QOS)
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
	err := c.store.Subscribe(c.session, subs)
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
	err := c.store.Unsubscribe(c.session, p.Topics)
	if err != nil {
		return err
	}
	return c.send(usa, true)
}

func (c *ClientMQTT) onPingreq(p *common.Pingreq) error {
	return c.send(&common.Pingresp{}, true)
}

func (c *ClientMQTT) callback(id uint64) {
	pa := &common.Puback{}
	pa.ID = common.ID(id)
	c.send(pa, true)
}

func (c *ClientMQTT) genSuback(p *common.Subscribe) (*common.Suback, []common.Subscription) {
	sa := &common.Suback{
		ID:          p.ID,
		ReturnCodes: make([]common.QOS, len(p.Subscriptions)),
	}
	subs := []common.Subscription{}
	for i, sub := range p.Subscriptions {
		// if !common.SubTopicValidate(sub.Topic) {
		// 	s.log.Errorf("subscribe topic (%s) invalid", sub.Topic)
		// 	rv[i] = packet.QOSFailure
		// } else if !s.authorizer.Authorize(auth.Subscribe, sub.Topic) {
		// 	s.log.Errorf("subscribe topic (%s) not permitted", sub.Topic)
		// 	rv[i] = packet.QOSFailure
		// } else if sub.QOS > 1 {
		// 	s.log.Errorf("subscribe QOS (%d) not supported", sub.QOS)
		// 	rv[i] = packet.QOSFailure
		// } else {
		sa.ReturnCodes[i] = sub.QOS
		subs = append(subs, sub)
		// }
	}
	return sa, subs
}
