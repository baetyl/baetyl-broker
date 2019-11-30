package session

import (
	"errors"
	"regexp"
	"sync"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-go/utils/log"
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
	defer c.log.Info("client is has closed by session")
	return c.close()
}

// closes client by itself
func (c *ClientMQTT) die(err error) {
	if !c.Alive() {
		return
	}
	go func() {
		c.log.Info("client is closing by itself", log.Error(err))
		c.manager.delClient(c)
		c.close()
		c.log.Info("client is has closed by itself")
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

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
