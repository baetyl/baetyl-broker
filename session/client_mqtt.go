package session

import (
	"errors"
	"regexp"
	"sync"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/docker/distribution/uuid"
)

// ErrClientClosed the client is closed
var ErrClientClosed = errors.New("client is closed")

// ErrConnectUnexpected the connect packet is not expected
var ErrConnectUnexpected = errors.New("packet (connect) is not expected after connected")

// ErrPacketUnexpected the packet type is not expected
var ErrPacketUnexpected = errors.New("packet type is not expected")

// ClientMQTT the client of MQTT
type ClientMQTT struct {
	id         string
	manager    *Manager
	session    *Session
	publisher  *publisher
	connection transport.Connection
	anonymous  bool
	authorizer *auth.Authorizer
	log        *log.Logger
	utils.Tomb
	sync.Mutex
	sync.Once
}

// NewClientMQTT creates a new MQTT client
func newClientMQTT(m *Manager, connection transport.Connection, anonymous bool) *ClientMQTT {
	id := uuid.Generate().String()
	c := &ClientMQTT{
		id:         id,
		manager:    m,
		connection: connection,
		anonymous:  anonymous,
		publisher:  newPublisher(m.config.RepublishInterval, m.config.MaxInflightQOS1Messages),
		log:        log.With(log.String("id", id)),
	}
	c.Go(c.receiving)
	return c
}

func (c *ClientMQTT) getID() string {
	return c.id
}

func (c *ClientMQTT) setSession(s *Session) {
	c.session = s
	c.log = c.log.With(log.String("sid", s.ID))
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
	c.Kill(nil)
	c.connection.Close()
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
	err := c.retainMessage(msg)
	if err != nil {
		c.log.Error("failed to retain will message", log.String("topic", msg.Topic))
	}
	p := packet.Publish{Message: *msg}
	c.manager.exchange.Route(common.NewMessage(&p), c.callback)
}

func (c *ClientMQTT) retainMessage(msg *packet.Message) error {
	if !msg.Retain {
		return nil
	}
	if len(msg.Payload) == 0 {
		return c.manager.removeRetain(msg.Topic)
	}
	return c.manager.setRetain(msg.Topic, msg)
}

// SendRetainMessage sends retain message
func (c *ClientMQTT) sendRetainMessage() error {
	msgs, err := c.manager.getRetain()
	if err != nil || len(msgs) == 0 {
		return err
	}
	for _, msg := range msgs {
		e := common.Event{Message: msg}
		err = c.session.Push(&e)
		if err != nil {
			return err
		}
		c.log.Info("retain message sent", log.String("topic", msg.Context.Topic))
	}
	return nil
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
