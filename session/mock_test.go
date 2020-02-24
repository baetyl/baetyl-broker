package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
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
  persistence:
  location: testdata
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
	t          *testing.T
	cfg        Config
	mgr        *Manager
	trans      *mqtt.Transport
	linkserver *grpc.Server
}

func newMockBroker(t *testing.T, cfgStr string) *mockBroker {
	log.Init(log.Config{Level: "debug", Format: "text"})

	var cfg Config
	err := utils.UnmarshalYAML([]byte(cfgStr), &cfg)
	assert.NoError(t, err)
	b := &mockBroker{t: t, cfg: cfg}
	b.mgr, err = NewManager(cfg)
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertSessionCount(expect int) {
	b.mgr.mut.RLock()
	defer b.mgr.mut.RUnlock()

	assert.Len(b.t, b.mgr.sessions, expect)
}

func (b *mockBroker) assertSessionStore(id string, expect string) {
	ses, err := b.mgr.sstore.Get(id)
	assert.NoError(b.t, err)
	if expect == "" {
		assert.Nil(b.t, ses)
	} else {
		assert.Equal(b.t, expect, ses.String())
	}
}

func (b *mockBroker) assertSessionState(id string, expect state) {
	b.mgr.mut.RLock()
	defer b.mgr.mut.RUnlock()

	ses, ok := b.mgr.sessions[id]
	assert.True(b.t, ok)
	assert.Equal(b.t, expect, ses.stat)
	if expect == STATE0 {
		assert.Nil(b.t, ses.disp)
		assert.Nil(b.t, ses.qos0)
		assert.Nil(b.t, ses.qos1)
		assert.Nil(b.t, ses.subs)
	} else if expect == STATE1 {
		assert.NotNil(b.t, ses.disp)
		assert.NotNil(b.t, ses.qos0)
		assert.NotNil(b.t, ses.qos1)
		assert.NotNil(b.t, ses.subs)
	} else if expect == STATE2 {
		assert.Nil(b.t, ses.disp)
		assert.Nil(b.t, ses.qos0)
		assert.NotNil(b.t, ses.qos1)
		assert.NotNil(b.t, ses.subs)
	}
}

func (b *mockBroker) waitClientReady(sid string, cnt int) {
	for {
		b.mgr.mut.RLock()
		clis := b.mgr.sessions[sid].copyClients()
		b.mgr.mut.RUnlock()
		if len(clis) != cnt {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
}

func (b *mockBroker) assertClientCount(sid string, expect int) {
	b.mgr.mut.RLock()
	clis := b.mgr.sessions[sid].copyClients()
	b.mgr.mut.RUnlock()

	assert.Len(b.t, clis, expect)
}

func (b *mockBroker) assertExchangeCount(expect int) {
	count := 0
	for _, bind := range b.mgr.exch.Bindings() {
		count += bind.Count()
	}
	assert.Equal(b.t, expect, count)
}

func (b *mockBroker) close() {
	if b.trans != nil {
		b.trans.Close()
	}
	if b.mgr != nil {
		b.mgr.Close()
	}
}

func (b *mockBroker) closeAndClean() {
	if b.trans != nil {
		b.trans.Close()
	}
	if b.mgr != nil {
		b.mgr.Close()
	}
	os.RemoveAll(b.cfg.Persistence.Location)
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
	c.err <- errors.New("closed")
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
	if c.isClosed() {
		return errors.New("stream has closed")
	}
	select {
	case c.s2c <- msg:
		// default:
		// 	assert.FailNow(c.t, "s2c channel is full")
	}
	return nil
}

func (c *mockStream) Recv() (*link.Message, error) {
	if c.isClosed() {
		return nil, errors.New("stream has closed")
	}
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
	defer c.Unlock()
	c.closed = true
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

func TestCMAP(t *testing.T) {
	clis := cmap.New()
	for c := range clis.IterBuffered() {
		t.Log(c)
	}
}
