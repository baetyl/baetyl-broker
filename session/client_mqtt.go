package session

import (
	"errors"
	"regexp"
	"sync"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// ErrClientClosed the client is closed
var ErrClientClosed = errors.New("client is closed")

// ErrConnectUnexpected the connect packet is not expected
var ErrConnectUnexpected = errors.New("packet (connect) is not expected after connected")

// ErrPacketUnexpected the packet type is not expected
var ErrPacketUnexpected = errors.New("packet type is not expected")

// ClientMQTT the client of MQTT
type ClientMQTT struct {
	store      *Store
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

func (c *ClientMQTT) setSession(ses *Session) {
	c.session = ses
	c.log = c.log.With(log.String("id", ses.ID))
}

func (c *ClientMQTT) getSession() *Session {
	return c.session
}

func (c *ClientMQTT) authorize(action, topic string) bool {
	return c.authorizer == nil || c.authorizer.Authorize(action, topic)
}

// notify session store to clean client and its session
func (c *ClientMQTT) clean() {
	c.Do(func() {
		c.store.putEvent(c)
	})
}

// Close closes client by session
func (c *ClientMQTT) Close() error {
	c.log.Info("client is closing")
	c.Kill(nil)
	c.Wait()
	err := c.connection.Close()
	c.log.Info("client is has closed", log.Error(err))
	return err
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
