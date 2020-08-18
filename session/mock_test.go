package session

import (
	"context"
	"io"
	"net"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/v2/listener"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

var (
	testConfDefault   = ""
	testConfResending = `
session:
  resendInterval: 2s
`
	testConfSession = `
session:
  sysTopics:
  - $link
  - $baidu
  maxClients: 3
  resendInterval: 1s

principals:
- username: u1
  password: p1
  permissions:
  - action: sub
    permit: [test, talks, talks1, talks2, '$baidu/iot', '$link/data', '$link/#', '#']
  - action: pub
    permit: [test, talks, '$baidu/iot', '$link/data']
- username: u2
  password: p2
  permissions:
  - action: pub
    permit: [test, talks, talks1, talks2]
- username: u3
  password: p3
  permissions:
  - action: sub
    permit: [test, talks]
- username: u4
  password: p4
  permissions:
  - action: sub
    permit: [test, talks, '$baidu/iot', '$link/data']
`
	testCleanExpiredMags = `
session:
  resendInterval: 100s
  maxInflightQOS1Messages: 1
  persistence:
    queue:
      expireTime: 2s
      cleanInterval: 1s
`
)

type mockBroker struct {
	t       *testing.T
	cfg     Config
	manager *Manager
	lis     *listener.Manager
}

func newMockBroker(t *testing.T, cfgStr string) *mockBroker {
	log.Init(log.Config{Level: "debug", Encoding: "console"})

	var cfg Config
	err := utils.UnmarshalYAML([]byte(cfgStr), &cfg)
	assert.NoError(t, err)
	os.RemoveAll(path.Dir(cfg.Persistence.Store.Source))
	b := &mockBroker{t: t, cfg: cfg}
	b.manager, err = NewManager(cfg)
	assert.NoError(t, err)
	return b
}

func newMockBrokerNotClean(t *testing.T, cfgStr string) *mockBroker {
	log.Init(log.Config{Level: "debug", Encoding: "console"})

	var cfg Config
	err := utils.UnmarshalYAML([]byte(cfgStr), &cfg)
	assert.NoError(t, err)
	b := &mockBroker{t: t, cfg: cfg}
	b.manager, err = NewManager(cfg)
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertSessionCount(expect int) {
	assert.Equal(b.t, expect, b.manager.sessions.count())
}

func (b *mockBroker) assertClientCount(expect int) {
	assert.Equal(b.t, expect, b.manager.clients.count())
}

func (b *mockBroker) assertSessionStore(id string, expect string, hasErr error) {
	var s Info
	err := b.manager.sessionBucket.GetKV(id, &s)
	if hasErr != nil {
		assert.Error(b.t, err)
		assert.Equal(b.t, err.Error(), hasErr.Error())
	} else {
		if expect == "" {
			assert.Nil(b.t, s)
		} else {
			assert.Equal(b.t, expect, s.String())
		}
	}
}

func (b *mockBroker) waitClientReady(sid string, isNil bool) {
	for {
		_, ok := b.manager.sessions.load(sid)
		if ok {
			_, _ok := b.manager.clients.load(sid)
			if isNil && !_ok {
				return
			}
			if !isNil && _ok {
				return
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (b *mockBroker) assertExchangeCount(expect int) {
	count := 0
	for _, bind := range b.manager.exch.Bindings() {
		count += bind.Count()
	}
	assert.Equal(b.t, expect, count)
}

func (b *mockBroker) close() {
	if b.lis != nil {
		b.lis.Close()
	}
	if b.manager != nil {
		b.manager.Close()
	}
}

func (b *mockBroker) closeAndClean() {
	b.close()
	os.RemoveAll("./var")
}

// * mqtt mock

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
		c2s: make(chan mqtt.Packet, 20),
		s2c: make(chan mqtt.Packet, 20),
		err: make(chan error, 10),
	}
}

func (c *mockConn) Send(pkt mqtt.Packet, _ bool) error {
	select {
	case c.s2c <- pkt:
		// default:
		// 	assert.FailNow(c.t, "s2c channel is full")
	}
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
	c.err <- io.EOF
	return nil
}

func (c *mockConn) SetMaxWriteDelay(t time.Duration)     {}
func (c *mockConn) SetReadLimit(limit int64)             {}
func (c *mockConn) SetReadTimeout(timeout time.Duration) {}
func (c *mockConn) LocalAddr() net.Addr                  { return nil }
func (c *mockConn) RemoteAddr() net.Addr                 { return nil }

func (c *mockConn) sendC2S(pkt mqtt.Packet) error {
	select {
	case c.c2s <- pkt:
		return nil
	case <-time.After(time.Minute):
		assert.Fail(c.t, "send common timeout")
		return nil
	}
}

func (c *mockConn) receiveS2C() mqtt.Packet {
	select {
	case pkt := <-c.s2c:
		return pkt
	case <-time.After(time.Minute):
		assert.Fail(c.t, "Receive common timeout")
		return nil
	}
}

func (c *mockConn) assertS2CPacket(expect string) {
	select {
	case pkt := <-c.s2c:
		assert.NotNil(c.t, pkt)
		assert.Equal(c.t, expect, pkt.String())
	case <-time.After(time.Second * 10):
		assert.Fail(c.t, "receive common timeout")
	}
}

func (c *mockConn) assertS2CPacketTimeout() {
	select {
	case pkt := <-c.s2c:
		assert.Fail(c.t, "receive unexpected packet:", pkt.String())
	case <-time.After(time.Millisecond * 100):
	}
}

func (c *mockConn) assertClosed(expect bool) {
	c.RLock()
	assert.Equal(c.t, expect, c.closed)
	c.RUnlock()
}

// * link mock

type mockStream struct {
	t      *testing.T
	md     metadata.MD
	c2s    chan *mqtt.Message
	s2c    chan *mqtt.Message
	err    chan error
	closed bool
	sync.RWMutex
}

func (c *mockStream) Send(msg *mqtt.Message) error {
	select {
	case c.s2c <- msg:
		// default:
		// 	assert.FailNow(c.t, "s2c channel is full")
	}
	return nil
}

func (c *mockStream) Recv() (*mqtt.Message, error) {
	select {
	case msg := <-c.c2s:
		return msg, nil
	case err := <-c.err:
		return nil, err
	}
}

func (c *mockStream) Context() context.Context {
	return metadata.NewIncomingContext(context.Background(), c.md)
}

func (c *mockStream) SetHeader(metadata.MD) error  { return nil }
func (c *mockStream) SendHeader(metadata.MD) error { return nil }
func (c *mockStream) SetTrailer(metadata.MD)       {}
func (c *mockStream) SendMsg(m interface{}) error  { return nil }
func (c *mockStream) RecvMsg(m interface{}) error  { return nil }

func (c *mockStream) Close() {
	c.Lock()
	c.closed = true
	c.Unlock()
	c.err <- io.EOF
}

func (c *mockStream) isClosed() bool {
	c.RLock()
	defer c.RUnlock()
	return c.closed
}

func (c *mockStream) sendC2S(msg *mqtt.Message) error {
	select {
	case c.c2s <- msg:
		return nil
	case <-time.After(time.Minute):
		assert.Fail(c.t, "send common timeout")
		return nil
	}
}

func (c *mockStream) receiveS2C() *mqtt.Message {
	select {
	case msg := <-c.s2c:
		return msg
	case <-time.After(time.Minute):
		assert.Fail(c.t, "Receive common timeout")
		return nil
	}
}

func (c *mockStream) assertClosed(expect bool) {
	c.RLock()
	assert.Equal(c.t, expect, c.closed)
	c.RUnlock()
}

func (c *mockStream) assertS2CMessage(expect string) {
	select {
	case msg := <-c.s2c:
		assert.NotNil(c.t, msg)
		assert.Equal(c.t, expect, msg.String())
	case <-time.After(time.Minute):
		assert.Fail(c.t, "receive common timeout")
	}
}

func (c *mockStream) assertS2CMessageTimeout() {
	select {
	case msg := <-c.s2c:
		assert.Fail(c.t, "receive unexpected packet", msg.String())
	case <-time.After(time.Millisecond * 100):
	}
}
