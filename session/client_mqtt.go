package session

import (
	"errors"
	"regexp"
	"sync"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/docker/distribution/uuid"
)

var (
	// ErrSessionClientAlreadyClosed the session client is already closed
	ErrSessionClientAlreadyClosed = errors.New("session client is already closed")
	// ErrSessionClientAlreadyConnecting the session client is already connecting
	ErrSessionClientAlreadyConnecting = errors.New("session client is already connecting")
	// ErrSessionClientUnexpectedPacket the session client received unexpected packet
	ErrSessionClientUnexpectedPacket = errors.New("session client received unexpected packet")
)

// ClientMQTT the client of MQTT
type ClientMQTT struct {
	id         string
	manager    *Manager
	session    *Session
	publisher  *publisher
	connection mqtt.Connection
	anonymous  bool
	authorizer *auth.Authorizer
	log        *log.Logger
	utils.Tomb
	sync.Mutex
	sync.Once
}

// NewClientMQTT creates a new MQTT client
func newClientMQTT(m *Manager, connection mqtt.Connection, anonymous bool) *ClientMQTT {
	id := uuid.Generate().String()
	c := &ClientMQTT{
		id:         id,
		manager:    m,
		connection: connection,
		anonymous:  anonymous,
		publisher:  newPublisher(m.config.RepublishInterval, m.config.MaxInflightQOS1Messages),
		log:        log.With(log.Any("id", id)),
	}
	c.Go(c.receiving)
	return c
}

func (c *ClientMQTT) getID() string {
	return c.id
}

func (c *ClientMQTT) setSession(s *Session) {
	c.session = s
	c.log = c.log.With(log.Any("cid", s.ID))
}

func (c *ClientMQTT) getSession() *Session {
	return c.session
}

// Close closes client by session
func (c *ClientMQTT) Close() error {
	c.log.Info("client is closing by session")
	defer c.log.Info("client has closed by session")
	return c.close()
}

// closes client by itself
func (c *ClientMQTT) die(err error) {
	if !c.Alive() {
		return
	}
	go func() {
		c.log.Info("client is closing by itself", log.Error(err))
		if err != nil {
			c.sendWillMessage()
		}
		c.manager.delClient(c)
		c.close()
		c.log.Info("client has closed by itself")
	}()
}

func (c *ClientMQTT) close() error {
	c.Do(func() {
		c.Kill(nil)
		c.connection.Close()
	})
	return c.Wait()
}

func (c *ClientMQTT) authorize(action, topic string) bool {
	return c.authorizer == nil || c.authorizer.Authorize(action, topic)
}

// SendWillMessage sends will message
func (c *ClientMQTT) sendWillMessage() {
	if c.session == nil || c.session.Will == nil {
		return
	}
	msg := c.session.Will
	if msg.Retain() {
		err := c.retainMessage(msg)
		if err != nil {
			c.log.Error("failed to retain will message", log.Any("topic", msg.Context.Topic))
		}
	}
	c.manager.exchange.Route(msg, c.callback)
}

func (c *ClientMQTT) retainMessage(msg *common.Message) error {
	if len(msg.Content) == 0 {
		return c.manager.removeRetain(msg.Context.Topic)
	}
	return c.manager.setRetain(msg.Context.Topic, msg)
}

// SendRetainMessage sends retain message
func (c *ClientMQTT) sendRetainMessage() error {
	if c.session == nil {
		return nil
	}
	msgs, err := c.manager.getRetain()
	if err != nil || len(msgs) == 0 {
		return err
	}
	for _, msg := range msgs {
		if ok, qos := common.MatchTopicQOS(c.session.subs, msg.Context.Topic); ok {
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

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
