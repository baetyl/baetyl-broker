package session

import (
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
)

type mockConfig struct {
	Session    Config
	Endpoints  []*mqtt.Endpoint
	Principals []auth.Principal
	SysTopics  []string
}

type mockBroker struct {
	t         *testing.T
	cfg       mockConfig
	manager   *Manager
	transport *mqtt.Transport
}

func newMockBroker(t *testing.T) *mockBroker {
	return newMockBrokerWith(t, 0)
}

func newMockBrokerWith(t *testing.T, maxConnections int) *mockBroker {
	log.Init(log.Config{Level: "debug", Format: "text"})
	dir, _ := ioutil.TempDir("", "broker")

	var err error
	b := &mockBroker{t: t}
	defaults.Set(&b.cfg.Session)
	b.cfg.Session.PersistenceLocation = dir
	b.cfg.Session.RepublishInterval = time.Millisecond * 200
	b.cfg.Session.MaxConnections = maxConnections
	b.cfg.Principals = []auth.Principal{{
		Username: "u1",
		Password: "p1",
		Permissions: []auth.Permission{{
			Action:  "sub",
			Permits: []string{"test", "talks", "talks1", "talks2", "$baidu/iot", "$link/data"},
		}, {
			Action:  "pub",
			Permits: []string{"test", "talks", "$baidu/iot", "$link/data"},
		}}}, {
		Username: "u2",
		Password: "p2",
		Permissions: []auth.Permission{{
			Action:  "pub",
			Permits: []string{"test", "talks", "talks1", "talks2"},
		}}}, {
		Username: "u3",
		Password: "p3",
		Permissions: []auth.Permission{{
			Action:  "sub",
			Permits: []string{"test", "talks"},
		}}}, {
		Username: "u4",
		Password: "p4",
		Permissions: []auth.Permission{{
			Action:  "sub",
			Permits: []string{"test", "talks"},
		}}}}
	b.manager, err = NewManager(b.cfg.Session, newMockExchange(b.cfg.SysTopics), auth.NewAuth(b.cfg.Principals))
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertClientCount(expect int) {
	assert.Len(b.t, b.manager.clients, expect)
}

func (b *mockBroker) assertBindingCount(sid string, expect int) {
	assert.Len(b.t, b.manager.bindings[sid], expect)
}

func (b *mockBroker) assertSession(id string, expect string) {
	ses, err := b.manager.backend.Get(id)
	assert.NoError(b.t, err)
	if expect == "" {
		assert.Nil(b.t, ses)
	} else {
		assert.Equal(b.t, expect, ses.String())
	}
}

func (b *mockBroker) assertExchangeCount(expect int) {
	count := 0
	for _, bind := range b.manager.exchange.(*mockExchange).bindings {
		count += bind.Count()
	}
	assert.Equal(b.t, expect, count)
}

func (b *mockBroker) close() {
	if b.transport != nil {
		b.transport.Close()
	}
	if b.manager != nil {
		b.manager.Close()
	}
	os.RemoveAll(b.cfg.Session.PersistenceLocation)
}

type mockConn struct {
	t      *testing.T
	c2s    chan mqtt.Packet
	s2c    chan mqtt.Packet
	err    chan error
	closed bool
	sync.RWMutex
}

func newMockConn(t *testing.T) *mockConn {
	return &mockConn{
		t:   t,
		c2s: make(chan mqtt.Packet, 10),
		s2c: make(chan mqtt.Packet, 10),
		err: make(chan error, 10),
	}
}

func (c *mockConn) SetMaxWriteDelay(t time.Duration) {
}

func (c *mockConn) Send(pkt mqtt.Packet, _ bool) error {
	c.s2c <- pkt
	return nil
}

func (c *mockConn) Receive() (mqtt.Packet, error) {
	select {
	case pkt := <-c.c2s:
		return pkt, nil
	case err := <-c.err:
		return nil, err
	}
}

func (c *mockConn) Close() error {
	c.Lock()
	c.closed = true
	c.Unlock()
	c.err <- ErrSessionClientAlreadyClosed
	return nil
}

func (c *mockConn) sendC2S(pkt mqtt.Packet) error {
	select {
	case c.c2s <- pkt:
		return nil
	case <-time.After(time.Second * 3):
		assert.Fail(c.t, "send common timeout")
		return nil
	}
}

func (c *mockConn) receiveS2C() mqtt.Packet {
	select {
	case pkt := <-c.s2c:
		return pkt
	case <-time.After(time.Second * 3):
		assert.Fail(c.t, "Receive common timeout")
		return nil
	}
}

func (c *mockConn) assertS2CPacket(expect string) {
	select {
	case pkt := <-c.s2c:
		assert.NotNil(c.t, pkt)
		assert.Equal(c.t, expect, pkt.String())
	case <-time.After(time.Second * 3):
		assert.Fail(c.t, "receive common timeout")
	}
}

func (c *mockConn) assertS2CPacketTimeout() {
	select {
	case pkt := <-c.s2c:
		assert.Fail(c.t, "receive unexpected packet: %s", pkt.String())
	case <-time.After(time.Millisecond * 100):
	}
}

func (c *mockConn) assertClosed(expect bool) {
	c.RLock()
	assert.Equal(c.t, expect, c.closed)
	c.RUnlock()
}

func (c *mockConn) SetReadLimit(limit int64) {}

func (c *mockConn) SetReadTimeout(timeout time.Duration) {}

func (c *mockConn) LocalAddr() net.Addr {
	return nil
}

func (c *mockConn) RemoteAddr() net.Addr {
	return nil
}

// mockExchange the message exchange
type mockExchange struct {
	bindings map[string]*mqtt.Trie
}

// NewExchange creates a new exchange
func newMockExchange(sysTopics []string) *mockExchange {
	ex := &mockExchange{
		bindings: make(map[string]*mqtt.Trie),
	}
	for _, v := range sysTopics {
		ex.bindings[v] = mqtt.NewTrie()
	}
	ex.bindings["/"] = mqtt.NewTrie()
	return ex
}

// Bind binds a new queue with a specify topic
func (b *mockExchange) Bind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		bind.Add(parts[1], queue)
		return
	}
	// common
	b.bindings["/"].Add(topic, queue)
}

// Unbind unbinds a queue from a specify topic
func (b *mockExchange) Unbind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		bind.Remove(parts[1], queue)
		return
	}
	// common
	b.bindings["/"].Remove(topic, queue)
}

// UnbindAll unbinds a queue from all topics
func (b *mockExchange) UnbindAll(queue common.Queue) {
	for _, bind := range b.bindings {
		bind.Clear(queue)
	}
}

// Route routes message to binding queues
func (b *mockExchange) Route(msg *common.Message, cb func(uint64)) {
	sss := make([]interface{}, 0)
	parts := strings.SplitN(msg.Context.Topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		sss = bind.Match(parts[1])
	} else {
		sss = b.bindings["/"].Match(msg.Context.Topic)
	}
	length := len(sss)
	if length == 0 {
		if cb != nil {
			cb(msg.Context.ID)
		}
		return
	}
	event := common.NewEvent(msg, int32(length), cb)
	for _, s := range sss {
		s.(common.Queue).Push(event)
	}
}
