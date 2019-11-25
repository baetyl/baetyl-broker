package session

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
)

type mockConfig struct {
	Session    Config
	Endpoints  []*transport.Endpoint
	Principals []auth.Principal
}

type mockBroker struct {
	t         *testing.T
	cfg       mockConfig
	store     *Store
	transport *transport.Transport
}

func newMockBroker(t *testing.T) *mockBroker {
	var err error
	b := &mockBroker{t: t}
	defaults.Set(&b.cfg.Session)
	b.cfg.Session.RepublishInterval = time.Millisecond * 200
	b.cfg.Principals = []auth.Principal{{
		Username: "u1",
		Password: "p1",
		Permissions: []auth.Permission{{
			Action:  "sub",
			Permits: []string{"test", "talks", "talks1", "talks2"},
		}, {
			Action:  "pub",
			Permits: []string{"test", "talks"},
		}}}, {
		Username: "u2",
		Password: "p2",
		Permissions: []auth.Permission{{
			Action:  "pub",
			Permits: []string{"test", "talks", "talks1", "talks2"},
		}}}}
	b.store, err = NewStore(b.cfg.Session, newMockExchange(), auth.NewAuth(b.cfg.Principals))
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertSession(id string, expect string) {
	ses, err := b.store.backend.Get(id)
	assert.NoError(b.t, err)
	if expect == "" {
		assert.Nil(b.t, ses)
	} else {
		assert.Equal(b.t, expect, ses.String())
	}
}

func (b *mockBroker) assertExchangeCount(expect int) {
	assert.Equal(b.t, expect, b.store.exchange.(*mockExchange).bindings.Count())
}

func (b *mockBroker) assertClientCount(session string, expect int) {
	assert.Len(b.t, b.store.clients[session], expect)
}

func (b *mockBroker) close() {
	if b.transport != nil {
		b.transport.Close()
	}
	if b.store != nil {
		b.store.Close()
	}
	os.RemoveAll("var")
}

type mockConn struct {
	t      *testing.T
	c2s    chan common.Packet
	s2c    chan common.Packet
	closed bool
}

func newMockConn(t *testing.T) *mockConn {
	return &mockConn{
		t:   t,
		c2s: make(chan common.Packet, 10),
		s2c: make(chan common.Packet, 10),
	}
}

func (c *mockConn) Send(pkt common.Packet, _ bool) error {
	select {
	case c.s2c <- pkt:
		return nil
	case <-time.After(time.Second * 3):
		assert.FailNow(c.t, "send common timeout")
		return nil
	}
}

func (c *mockConn) Receive() (common.Packet, error) {
	select {
	case pkt := <-c.c2s:
		return pkt, nil
	case <-time.After(time.Second * 3):
		assert.FailNow(c.t, "Receive common timeout")
		return nil, nil
	}
}

func (c *mockConn) sendC2S(pkt common.Packet) error {
	select {
	case c.c2s <- pkt:
		return nil
	case <-time.After(time.Second * 3):
		assert.FailNow(c.t, "send common timeout")
		return nil
	}
}

func (c *mockConn) receiveS2C() common.Packet {
	select {
	case pkt := <-c.s2c:
		return pkt
	case <-time.After(time.Second * 3):
		assert.FailNow(c.t, "Receive common timeout")
		return nil
	}
}

func (c *mockConn) assertS2CPacket(expect string) {
	select {
	case pkt := <-c.s2c:
		assert.NotNil(c.t, pkt)
		assert.Equal(c.t, expect, pkt.String())
	case <-time.After(time.Second * 3):
		assert.FailNow(c.t, "receive common timeout")
	}
}

func (c *mockConn) assertS2CPacketTimeout() {
	select {
	case pkt := <-c.s2c:
		assert.FailNow(c.t, "receive unexpected packet: %s", pkt.String())
	case <-time.After(time.Millisecond * 100):
	}
}

func (c *mockConn) assertClosed(expect bool) {
	assert.Equal(c.t, expect, c.closed)
}

func (c *mockConn) Close() error {
	c.closed = true
	return nil
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
	bindings *common.Trie
}

// NewExchange creates a new exchange
func newMockExchange() *mockExchange {
	return &mockExchange{bindings: common.NewTrie()}
}

// Bind binds a new queue with a specify topic
func (b *mockExchange) Bind(topic string, queue common.Queue) {
	b.bindings.Add(topic, queue)
}

// Unbind unbinds a queue from a specify topic
func (b *mockExchange) Unbind(topic string, queue common.Queue) {
	b.bindings.Remove(topic, queue)
}

// UnbindAll unbinds a queue from all topics
func (b *mockExchange) UnbindAll(queue common.Queue) {
	b.bindings.Clear(queue)
}

// Route routes message to binding queues
func (b *mockExchange) Route(msg *common.Message, cb func(uint64)) {
	sss := b.bindings.Match(msg.Context.Topic)
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
