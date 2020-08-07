package session

import (
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/docker/distribution/uuid"
)

// Client the client of MQTT
type Client struct {
	id       string
	interval time.Duration
	manager  *Manager
	session  *Session
	auth     *Authorizer
	conn     mqtt.Connection
	log      *log.Logger
	tomb     utils.Tomb
	mut      sync.Mutex
	once     sync.Once

	wrap func(*common.Event) *eventWrapper
}

// * connection handlers

// Handle the connection handler to create a new MQTT client
func (m *Manager) Handle(conn mqtt.Connection) {
	id := strings.ReplaceAll(uuid.Generate().String(), "-", "")
	c := &Client{
		id:       id,
		interval: m.cfg.ResendInterval,
		manager:  m,
		conn:     conn,
		log:      log.With(log.Any("type", "mqtt"), log.Any("id", id)),
	}

	max := m.cfg.MaxClients
	if max > 0 && m.clients.count() >= max {
		c.log.Error("number of clients exceeds the limit", log.Any("max", m.cfg.MaxClients))
		conn.Close()
		return
	}
	c.tomb.Go(c.receiving)
}

func (c *Client) setSession(sid string, s *Session) {
	c.session = s
	if c.id != sid {
		c.log = c.log.With(log.Any("sid", sid))
	}
}

// closes client by session
func (c *Client) close() error {
	if !c.tomb.Alive() {
		return nil
	}

	c.log.Info("client is closing")
	defer c.log.Info("client has closed")

	c.tomb.Kill(nil)

	c.once.Do(func() {
		c.conn.Close()
	})

	c.tomb.Wait()
	return nil
}

// closes client by itself
func (c *Client) die(msg string, err error) {
	if !c.tomb.Alive() {
		return
	}

	c.log.Info("client is dying")
	defer c.log.Info("client has died")

	c.tomb.Kill(err)

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

	c.manager.delClient(c.session.info.ID)
}

func (c *Client) authorize(action, topic string) bool {
	return c.auth == nil || c.auth.Authorize(action, topic)
}

// SendWillMessage sends will message
func (c *Client) sendWillMessage() {
	msg := c.session.will()
	if msg == nil {
		return
	}
	if msg.Context.Flags&0x1 == 0x1 {
		err := c.retainMessage(msg)
		if err != nil {
			c.log.Error("failed to retain will message", log.Any("topic", msg.Context.Topic))
		}
	}
	// change to normal message before exchange
	msg.Context.Flags &^= 0x1
	c.manager.exch.Route(msg, c.callback)
}

func (c *Client) retainMessage(msg *mqtt.Message) error {
	if len(msg.Content) == 0 {
		return c.manager.unretainMessage(msg.Context.Topic)
	}
	return c.manager.retainMessage(msg)
}

// SendRetainMessage sends retain message
func (c *Client) sendRetainMessage() error {
	msgs, err := c.manager.listRetainedMessages()
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

// * ingress

func (c *Client) receiving() error {
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
			c.session.cleanWill()
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

func (c *Client) onConnect(p *mqtt.Connect) error {
	if p.ClientID == "" {
		if p.CleanSession == false {
			c.sendConnack(mqtt.IdentifierRejected, false)
			return ErrConnectionRefuse
		}
		p.ClientID = c.id
	}

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

	if c.manager.auth != nil {
		if p.Password != "" {
			// username/password authentication
			if p.Username == "" {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return ErrSessionUsernameNotSet
			}
			c.auth = c.manager.auth.AuthenticateAccount(p.Username, p.Password)
			if c.auth == nil {
				c.sendConnack(mqtt.BadUsernameOrPassword, false)
				return ErrSessionUsernameNotPermitted
			}
		} else {
			if cn, ok := mqtt.GetTLSCommonName(c.conn); ok {
				// if it is bidirectional authentication, will use certificate authentication
				c.auth = c.manager.auth.AuthenticateCertificate(cn)
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
		if len(p.Will.Payload) > int(c.manager.cfg.MaxMessagePayloadSize) {
			return ErrSessionWillMessagePayloadSizeExceedsLimit
		}
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
		si.WillMessage = common.NewMessage(&mqtt.Publish{Message: *p.Will})
	}

	s, exists, err := c.manager.addClient(si, c)
	if err != nil {
		return err
	}

	c.wrap = func(m *common.Event) *eventWrapper {
		return newEventWrapper(uint64(s.cnt.NextID()), 1, m)
	}

	c.tomb.Go(c.sending, c.resending)

	err = c.sendConnack(mqtt.ConnectionAccepted, exists)
	if err != nil {
		return err
	}
	c.log.Info("client is connected")
	return nil
}

func (c *Client) onPublish(p *mqtt.Publish) error {
	// TODO: improvement, cache auth result
	if len(p.Message.Payload) > int(c.manager.cfg.MaxMessagePayloadSize) {
		return ErrSessionMessagePayloadSizeExceedsLimit
	}
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
	if msg.Context.Flags&0x1 == 0x1 {
		err := c.retainMessage(msg)
		if err != nil {
			return err
		}
		// change to normal message before exch
		msg.Context.Flags &^= 0x1
	}
	cb := c.callback
	if p.Message.QOS == 0 {
		cb = nil
	}
	c.manager.exch.Route(msg, cb)
	return nil
}

func (c *Client) onSubscribe(p *mqtt.Subscribe) error {
	// MQTT-3.8.3-3: A SUBSCRIBE packet with no payload is a protocol violation
	if len(p.Subscriptions) == 0 {
		return ErrSessionSubscribePayloadEmpty
	}

	sa, subs := c.genSuback(p)
	c.session.subscribe(subs, c.authorize)
	err := c.send(sa, false)
	if err != nil {
		return err
	}
	return c.sendRetainMessage()
}

func (c *Client) onUnsubscribe(p *mqtt.Unsubscribe) error {
	usa := mqtt.NewUnsuback()
	usa.ID = p.ID
	c.session.unsubscribe(p.Topics)
	return c.send(usa, false)
}

func (c *Client) onPingreq(_ *mqtt.Pingreq) error {
	return c.send(mqtt.NewPingresp(), false)
}

func (c *Client) callback(id uint64) {
	c.send(&mqtt.Puback{ID: mqtt.ID(id)}, true)
}

func (c *Client) genSuback(p *mqtt.Subscribe) (*mqtt.Suback, []mqtt.Subscription) {
	sa := &mqtt.Suback{
		ID:          p.ID,
		ReturnCodes: make([]mqtt.QOS, len(p.Subscriptions)),
	}
	var subs []mqtt.Subscription
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

// * egress

func (c *Client) send(pkt mqtt.Packet, async bool) error {
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

func (c *Client) sendConnack(code mqtt.ConnackCode, exists bool) error {
	ack := &mqtt.Connack{
		SessionPresent: exists,
		ReturnCode:     code,
	}
	return c.send(ack, false)
}

func (c *Client) sendEvent(m *eventWrapper, dup bool) (err error) {
	return c.send(m.packet(dup), true)
}

func (c *Client) sending() error {
	c.log.Info("client starts to send messages")
	defer c.log.Info("client has stopped sending messages")

	var msg *eventWrapper
	qos0 := c.session.qos0.Chan()
	qos1 := c.session.qos1.Chan()
	queue := c.session.queue
	cache := c.session.cache
	for {
		if msg != nil {
			if err := c.sendEvent(msg, false); err != nil {
				c.log.Debug("failed to send message", log.Error(err))
				return nil
			}
			if msg.qos == 1 {
				select {
				case queue <- msg:
				case <-c.tomb.Dying():
					return nil
				}
			}
		}
		select {
		case evt := <-qos0:
			if ent := c.log.Check(log.DebugLevel, "queue popped a message as qos 0"); ent != nil {
				ent.Write(log.Any("message", evt.String()))
			}
			msg = newEventWrapper(0, 0, evt)
		case evt := <-qos1:
			if ent := c.log.Check(log.DebugLevel, "queue popped a message as qos 1"); ent != nil {
				ent.Write(log.Any("message", evt.String()))
			}
			msg = c.wrap(evt)
			if err := cache.store(msg); err != nil {
				c.log.Error(err.Error())
			}
		case <-c.tomb.Dying():
			return nil
		}
	}
}

func (c *Client) resending() error {
	c.log.Info("client starts to resend messages", log.Any("interval", c.interval))
	defer c.log.Info("client has stopped resending messages")

	var msg *eventWrapper
	queue := c.session.queue
	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	for {
		if msg != nil {
			select {
			case <-timer.C:
			default:
			}
			for timer.Reset(c.next(msg)); msg.Wait(timer.C, c.tomb.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(c.interval) {
				if err := c.sendEvent(msg, true); err != nil {
					c.log.Debug("failed to resend message", log.Error(err))
					return nil
				}
			}
		}
		select {
		case msg = <-queue:
		case <-c.tomb.Dying():
			return nil
		}
	}
}

func (c *Client) next(m *eventWrapper) time.Duration {
	return c.interval - time.Now().Sub(m.lst)
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
