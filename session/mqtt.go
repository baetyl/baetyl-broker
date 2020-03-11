package session

import (
	"io"
	"regexp"
	"strings"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/docker/distribution/uuid"
)

// ClientMQTT the client of MQTT
type ClientMQTT struct {
	id      string
	mgr     *Manager
	session *Session
	auth    *Authorizer
	conn    mqtt.Connection
	log     *log.Logger
	tomb    utils.Tomb
	mut     sync.Mutex
	once    sync.Once
}

// * connection handlers

// Handle the connection handler to create a new MQTT client
func (m *Manager) Handle(conn mqtt.Connection) {
	id := strings.ReplaceAll(uuid.Generate().String(), "-", "")
	c := &ClientMQTT{
		id:   id,
		mgr:  m,
		conn: conn,
		log:  log.With(log.Any("type", "mqtt"), log.Any("id", id)),
	}
	si := Info{
		ID:           id,
		Kind:         MQTT,
		CleanSession: true, // always true for random client
	}
	s, _, err := m.initSession(si)
	if err != nil {
		conn.Close()
		return
	}
	err = s.addClient(c, true)
	if err != nil {
		conn.Close()
		return
	}
	c.tomb.Go(c.receiving)
}

func (c *ClientMQTT) getID() string {
	return c.id
}

func (c *ClientMQTT) setSession(sid string, s *Session) {
	c.session = s
	if c.id != sid {
		c.log = c.log.With(log.Any("sid", sid))
	}
}

// closes client by session
func (c *ClientMQTT) close() error {
	if !c.tomb.Alive() {
		return nil
	}
	c.log.Info("client is closing")
	defer c.log.Info("client has closed")
	c.tomb.Kill(nil)
	c.once.Do(func() {
		c.conn.Close()
	})
	return nil
}

// closes client by itself
func (c *ClientMQTT) die(msg string, err error) {
	if !c.tomb.Alive() {
		return
	}
	c.log.Info("client is dying")
	defer c.log.Info("client has died")
	c.tomb.Kill(err)
	c.session.delClient(c)
	if err != nil {
		if err == io.EOF {
			c.log.Info("client disconnected", log.Error(err))
		} else {
			c.log.Error(msg, log.Error(err))
		}
		c.sendWillMessage()
	}
	c.once.Do(func() {
		c.conn.Close()
	})
}

func (c *ClientMQTT) authorize(action, topic string) bool {
	return c.auth == nil || c.auth.Authorize(action, topic)
}

// SendWillMessage sends will message
func (c *ClientMQTT) sendWillMessage() {
	if c.session == nil {
		return
	}
	msg := c.session.will()
	if msg == nil {
		return
	}
	if msg.Retain() {
		err := c.retainMessage(msg)
		if err != nil {
			c.log.Error("failed to retain will message", log.Any("topic", msg.Context.Topic))
		}
	}
	// change to normal message before exchange
	msg.Context.Type = link.Msg
	c.mgr.exch.Route(msg, c.callback)
}

func (c *ClientMQTT) retainMessage(msg *link.Message) error {
	if len(msg.Content) == 0 {
		return c.mgr.unretainMessage(msg.Context.Topic)
	}
	return c.mgr.retainMessage(msg.Context.Topic, msg)
}

// SendRetainMessage sends retain message
func (c *ClientMQTT) sendRetainMessage() error {
	if c.session == nil {
		return nil
	}
	msgs, err := c.mgr.listRetainedMessages()
	if err != nil || len(msgs) == 0 {
		return err
	}
	// TODO: improve
	for _, msg := range msgs {
		if ok, qos := c.session.matchQOS(msg.Context.Topic); ok {
			if msg.Context.QOS > qos {
				msg.Context.QOS = qos
			}
			e := common.NewEvent(msg, 0, nil)
			err = c.session.Push(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// * egress

func (c *ClientMQTT) send(pkt mqtt.Packet, async bool) error {
	if !c.tomb.Alive() {
		return ErrSessionClientAlreadyClosed
	}
	c.mut.Lock()
	err := c.conn.Send(pkt, async)
	c.mut.Unlock()
	if err != nil {
		c.die("failed to send packet", err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client sent a packet"); ent != nil {
		ent.Write(log.Any("packet", pkt.String()))
	}
	return nil
}

func (c *ClientMQTT) sendConnack(code mqtt.ConnackCode, exists bool) error {
	ack := &mqtt.Connack{
		SessionPresent: exists,
		ReturnCode:     code,
	}
	return c.send(ack, false)
}

func (c *ClientMQTT) sendEvent(m *eventWrapper, dup bool) (err error) {
	return c.send(m.packet(dup), true)
}

// * ingress

func (c *ClientMQTT) receiving() error {
	c.log.Info("client starts to receive messages")
	defer c.log.Info("client has stopped receiving messages")

	pkt, err := c.conn.Receive()
	if err != nil {
		c.die("failed to receive packet at first time", err)
		return err
	}
	if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
		data := pkt.String()
		if len(data) > 200 {
			data = data[:200]
		}
		ent.Write(log.Any("packet", data))
	}
	p, ok := pkt.(*mqtt.Connect)
	if !ok {
		c.die(ErrSessionClientPacketUnexpected.Error(), ErrSessionClientPacketUnexpected)
		return ErrSessionClientPacketUnexpected
	}
	if err = c.onConnect(p); err != nil {
		c.die("failed to handle connect packet", err)
		return err
	}

	for {
		pkt, err = c.conn.Receive()
		if err != nil {
			c.die("failed to receive packet", err)
			return err
		}
		if ent := c.log.Check(log.DebugLevel, "client received a packet"); ent != nil {
			data := pkt.String()
			if len(data) > 200 {
				data = data[:200]
			}
			ent.Write(log.Any("packet", data))
		}
		switch p := pkt.(type) {
		case *mqtt.Publish:
			err = c.onPublish(p)
		case *mqtt.Puback:
			c.session.acknowledge(uint64(p.ID))
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
		Kind:         MQTT,
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
	if c.mgr.auth != nil {
		if p.Password != "" {
			// username/password authentication
			if p.Username == "" {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return ErrSessionUsernameNotSet
			}
			c.auth = c.mgr.auth.AuthenticateAccount(p.Username, p.Password)
			if c.auth == nil {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return ErrSessionUsernameNotPermitted
			}
		} else {
			if cn, ok := mqtt.GetTLSCommonName(c.conn); ok {
				// if it is bidirectional authentication, will use certificate authentication
				c.auth = c.mgr.auth.AuthenticateCertificate(cn)
				if c.auth == nil {
					c.sendConnack(mqtt.BadUsernameOrPassword, false)
					return ErrSessionCertificateCommonNameNotPermitted
				}
			} else {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return ErrSessionCertificateCommonNameNotFound
			}
		}
	}

	if p.Will != nil {
		if len(p.Will.Payload) > int(c.mgr.cfg.MaxMessagePayloadSize) {
			return ErrSessionWillMessagePayloadSizeExceedsLimit
		}
		if p.Will.QOS > 1 {
			return ErrSessionWillMessageQosNotSupported
		}
		if !c.mgr.checker.CheckTopic(p.Will.Topic, false) {
			return ErrSessionWillMessageTopicInvalid
		}
		if !c.authorize(Publish, p.Will.Topic) {
			c.sendConnack(mqtt.NotAuthorized, false)
			return ErrSessionWillMessageTopicNotPermitted
		}
		si.WillMessage = common.NewMessage(&mqtt.Publish{Message: *p.Will})
	}

	var err error
	var exists bool
	var s *Session
	if si.ID != "" {
		// clean previous session
		if c.session != nil {
			c.session.delClient(c)
		}
		//  init new session
		s, exists, err = c.mgr.initSession(si)
		if err != nil {
			return err
		}
		err = s.addClient(c, true)
		if err != nil {
			return err
		}
	}
	err = c.sendConnack(mqtt.ConnectionAccepted, exists)
	if err != nil {
		return err
	}
	c.log.Info("client is connected")
	return nil
}

func (c *ClientMQTT) onPublish(p *mqtt.Publish) error {
	// TODO: improvement, cache auth result
	if len(p.Message.Payload) > int(c.mgr.cfg.MaxMessagePayloadSize) {
		return ErrSessionMessagePayloadSizeExceedsLimit
	}
	if p.Message.QOS > 1 {
		return ErrSessionMessageQosNotSupported
	}
	if !c.mgr.checker.CheckTopic(p.Message.Topic, false) {
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
		// change to normal message before exch
		msg.Context.Type = link.Msg
	}
	cb := c.callback
	if p.Message.QOS == 0 {
		cb = nil
	}
	c.mgr.exch.Route(msg, cb)
	return nil
}

func (c *ClientMQTT) onSubscribe(p *mqtt.Subscribe) error {
	sa, subs := c.genSuback(p)
	err := c.session.subscribe(subs)
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
	err := c.session.unsubscribe(p.Topics)
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
	var subs []mqtt.Subscription
	for i, sub := range p.Subscriptions {
		if !c.mgr.checker.CheckTopic(sub.Topic, true) {
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

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
