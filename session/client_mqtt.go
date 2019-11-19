package session

import (
	"errors"
	"regexp"
	"sync"

	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// ErrClientClosed the session client is closed
var ErrClientClosed = errors.New("session client is closed")

// ErrConnectUnexpected the connect packet is not expected
var ErrConnectUnexpected = errors.New("packet (connect) is not expected after connected")

// ErrPacketUnexpected the packet type is not expected
var ErrPacketUnexpected = errors.New("packet type is not expected")

type republish struct {
}

// ClientMQTT the client of MQTT
type ClientMQTT struct {
	store      *Store
	session    *Session
	publisher  *publisher
	connection transport.Connection

	utils.Tomb
	sync.Once
	sync.Mutex
}

func (c *ClientMQTT) kill() {
	c.Do(func() {
		c.Kill(nil)
		log.Debug("client is killed")
	})
}

// Close closes mqtt client
func (c *ClientMQTT) Close() error {
	log.Debug("client is closing")
	defer log.Debug("client has closed")

	c.kill()
	return c.Wait()
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	r := regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
	return r.MatchString(v)
}
