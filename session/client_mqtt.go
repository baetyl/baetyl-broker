package session

import (
	"errors"
	"fmt"
	"regexp"
	"sync"

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
	utils.Tomb
	sync.Mutex
	sync.Once
}

func (c *ClientMQTT) id() string {
	if c.session != nil {
		return c.session.ID
	}
	return fmt.Sprintf("sessionless(%p)", c)
}

func (c *ClientMQTT) setSession(ses *Session) {
	c.session = ses
}

func (c *ClientMQTT) clean() {
	c.store.delClient(c.session, c)
}

// Close closes mqtt client
func (c *ClientMQTT) Close() error {
	c.Do(func() {
		log.Infof("client (%s) is closing", c.id())
		c.Kill(nil)

		log.Infof("client (%s) is waiting to close", c.id())
		err := c.connection.Close()
		if err != nil {
			log.Infof("client (%s) has closed with error: %s", c.id(), err.Error())
			return
		}
		log.Infof("client (%s) has closed", c.id())
	})
	return nil
}

// checkClientID checks clientID
func checkClientID(v string) bool {
	r := regexp.MustCompile("^[0-9A-Za-z_-]{0,128}$")
	return r.MatchString(v)
}
