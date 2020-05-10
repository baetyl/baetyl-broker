package session

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/listener"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

var (
	testConfDefault = ""
	testConfSession = `
session:
  sysTopics:
  - $link
  - $baidu
  maxSessions: 3
  maxClientsPerSession: 2
  resendInterval: 200ms

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
    permit: [test, talks]
`
)

type mockBroker struct {
	t   *testing.T
	cfg Config
	ses *Manager
	lis *listener.Manager
}

func newMockBroker(t *testing.T, cfgStr string) *mockBroker {
	err := os.MkdirAll("var/lib/baetyl", os.ModePerm)
	defer os.Remove("var/lib/baetyl")
	assert.NoError(t, err)

	log.Init(log.Config{Level: "debug", Encoding: "console"})

	var cfg Config
	err = utils.UnmarshalYAML([]byte(cfgStr), &cfg)
	assert.NoError(t, err)
	b := &mockBroker{t: t, cfg: cfg}
	b.ses, err = NewManager(cfg)
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertSessionCount(expect int) {
	assert.Equal(b.t, expect, b.ses.sessions.count())
}

func (b *mockBroker) assertSessionStore(id string, expect string, hasErr error) {
	var s Info
	err := b.ses.sessionBucket.GetKV(id, &s)
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

func (b *mockBroker) assertSessionState(sid string, expect state) {
	v, ok := b.ses.sessions.load(sid)
	assert.True(b.t, ok)
	if !ok {
		return
	}
	s := v.(*Session)
	s.mut.RLock()
	defer s.mut.RUnlock()
	assert.Equal(b.t, expect, s.stat)
	if expect == STATE0 {
		assert.Nil(b.t, s.disp)
		assert.Nil(b.t, s.qos0)
		assert.Nil(b.t, s.qos1)
		assert.Zero(b.t, s.subs.Count())
		assert.Zero(b.t, len(s.info.Subscriptions))
	} else if expect == STATE1 {
		assert.NotNil(b.t, s.disp)
		assert.NotNil(b.t, s.qos0)
		assert.NotNil(b.t, s.qos1)
		assert.NotZero(b.t, s.clients.count())
		assert.NotZero(b.t, s.subs.Count())
		assert.NotZero(b.t, len(s.info.Subscriptions))
	} else if expect == STATE2 {
		assert.Nil(b.t, s.disp)
		assert.Nil(b.t, s.qos0)
		assert.NotNil(b.t, s.qos1)
		assert.Zero(b.t, s.clients.count())
		assert.NotZero(b.t, s.subs.Count())
		assert.NotZero(b.t, len(s.info.Subscriptions))
	}
}

func (b *mockBroker) waitClientReady(sid string, cnt int) {
	for {
		v, ok := b.ses.sessions.load(sid)
		if !ok || v.(*Session).clients.count() != cnt {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
}

func (b *mockBroker) assertExchangeCount(expect int) {
	count := 0
	for _, bind := range b.ses.exch.Bindings() {
		count += bind.Count()
	}
	assert.Equal(b.t, expect, count)
}

func (b *mockBroker) close() {
	if b.lis != nil {
		b.lis.Close()
	}
	if b.ses != nil {
		b.ses.Close()
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
		c2s: make(chan mqtt.Packet, 10),
		s2c: make(chan mqtt.Packet, 10),
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
	case <-time.After(time.Minute):
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
	c2s    chan *link.Message
	s2c    chan *link.Message
	err    chan error
	closed bool
	sync.RWMutex
}

func newMockStream(t *testing.T, linkid string) *mockStream {
	return &mockStream{
		t:   t,
		md:  metadata.New(map[string]string{"linkid": linkid}),
		c2s: make(chan *link.Message, 100),
		s2c: make(chan *link.Message, 100),
		err: make(chan error, 100),
	}
}

func (c *mockStream) Send(msg *link.Message) error {
	select {
	case c.s2c <- msg:
		// default:
		// 	assert.FailNow(c.t, "s2c channel is full")
	}
	return nil
}

func (c *mockStream) Recv() (*link.Message, error) {
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

func (c *mockStream) sendC2S(msg *link.Message) error {
	select {
	case c.c2s <- msg:
		return nil
	case <-time.After(time.Minute):
		assert.Fail(c.t, "send common timeout")
		return nil
	}
}

func (c *mockStream) receiveS2C() *link.Message {
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

func assertClientClosed(expect int, cs ...*mockStream) string {
	var count int
	var active *mockStream
	for _, c := range cs {
		if c.isClosed() {
			count++
		} else {
			active = c
		}
	}
	assert.Equal(active.t, expect, count)
	return active.md.Get("linkid")[0]
}

func assertS2CMessageLB(subc1, subc2 *mockStream, expect string) *mockStream {
	select {
	case msg := <-subc1.s2c:
		assert.NotNil(subc1.t, msg)
		assert.Equal(subc1.t, expect, msg.String())
		fmt.Println("--> subc1 receive message:", msg)
		return subc1
	case msg := <-subc2.s2c:
		assert.NotNil(subc2.t, msg)
		assert.Equal(subc2.t, expect, msg.String())
		fmt.Println("--> subc2 receive message:", msg)
		return subc2
	case <-time.After(time.Minute):
		assert.Fail(subc1.t, "receive common timeout")
		return nil
	}
}
