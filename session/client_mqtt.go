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

func (c *ClientMQTT) authorize(action, topic string) bool {
	return c.authorizer == nil || c.authorizer.Authorize(action, topic)
}

func (c *ClientMQTT) clean() {
	c.store.delClient(c.session, c)
}

// Close closes mqtt client
func (c *ClientMQTT) Close() error {
	c.Do(func() {
		c.log.Info("client is closing")
		c.Kill(nil)
		err := c.connection.Close()
		c.log.Info("client has closed", log.Error(err))
	})
	return nil
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	return regexpClientID.MatchString(v)
}

var regexpClientID = regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
