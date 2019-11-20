package session

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
)

type mockConfig struct {
	Session   Config
	Endpoints []*transport.Endpoint
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
	b.store, err = NewStore(b.cfg.Session, exchange.NewExchange())
	assert.NoError(t, err)
	return b
}

func (b *mockBroker) assertSession(id string, expect string) {
	ses, err := b.store.backend.Get(id)
	assert.NoError(b.t, err)
	assert.Equal(b.t, expect, ses.String())
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

// func (c *mockConn) assertConnect(name, u, p string, v byte, ca common.ConnackCode) {
// 	conn := &common.Connect{ClientID: name, Username: u, Password: p, Version: v}
// 	c.sendC2S(conn)
// 	pkt := c.receiveS2C()
// 	connack := common.Connack{ReturnCode: ca}
// 	assert.Equal(c.t, connack.String(), pkt.String())
// }

// func (c *mockConn) assertOnConnect(name, u, p string, v byte, m string, ca common.ConnackCode) {
// 	conn := &common.Connect{ClientID: name, Username: u, Password: p, Version: v}
// 	err := c.session.onConnect(conn)
// 	if m == "" {
// 		assert.NoError(c.t, err)
// 	} else {
// 		assert.EqualError(c.t, err, m)
// 	}
// 	pkt := c.receiveS2C()
// 	connack := common.Connack{ReturnCode: ca}
// 	assert.Equal(c.t, connack.String(), pkt.String())
// }

// func (c *mockConn) assertOnConnectSuccess(name string, cs bool, will *common.Message) {
// 	conn := &common.Connect{ClientID: name, Username: "u1", Password: "p1", Version: 3, CleanSession: cs, Will: will}
// 	err := c.session.onConnect(conn)
// 	assert.NoError(c.t, err)
// 	pkt := c.receiveS2C()
// 	connack := common.Connack{ReturnCode: common.ConnectionAccepted}
// 	assert.Equal(c.t, connack.String(), pkt.String())
// }

// func (c *mockConn) assertOnConnectFailure(name string, will *common.Message, e string) {
// 	conn := &common.Connect{ClientID: name, Username: "u1", Password: "p1", Version: 3, Will: will}
// 	err := c.session.onConnect(conn)
// 	assert.EqualError(c.t, err, e)
// }

// func (c *mockConn) assertOnConnectFail(name string, will *common.Message) {
// 	conn := &common.Connect{ClientID: name, Username: "u1", Password: "p1", Version: 3, Will: will}
// 	err := c.session.onConnect(conn)
// 	assert.EqualError(c.t, err, fmt.Sprintf("will topic (%s) not permitted", will.Topic))
// 	pkt := c.receiveS2C()
// 	connack := common.Connack{ReturnCode: common.NotAuthorized}
// 	assert.Equal(c.t, connack.String(), pkt.String())
// }

// func (c *mockConn) assertSubscribe(subs []common.Subscription, codes []common.QOS) {
// 	sub := &common.Subscribe{ID: 11, Subscriptions: subs}
// 	c.Send(sub, true)
// 	pkt := c.receiveS2C()
// 	suback := common.Suback{ReturnCodes: codes, ID: 11}
// 	assert.Equal(c.t, suback.String(), pkt.String())
// }

// func (c *mockConn) assertOnSubscribe(subs []common.Subscription, codes []common.QOS) {
// 	sub := &common.Subscribe{ID: 12, Subscriptions: subs}
// 	err := c.session.onSubscribe(sub)
// 	assert.NoError(c.t, err)
// 	pkt := c.receiveS2C()
// 	suback := common.Suback{ReturnCodes: codes, ID: 12}
// 	assert.Equal(c.t, suback.String(), pkt.String())
// }

// func (c *mockConn) assertOnSubscribeSuccess(subs []common.Subscription) {
// 	sub := &common.Subscribe{ID: 11, Subscriptions: subs}
// 	err := c.session.onSubscribe(sub)
// 	assert.NoError(c.t, err)
// 	pkt := c.receiveS2C()
// 	codes := make([]common.QOS, len(subs))
// 	for i, s := range subs {
// 		codes[i] = s.QOS
// 	}
// 	suback := common.Suback{ReturnCodes: codes, ID: 11}
// 	assert.Equal(c.t, suback.String(), pkt.String())
// }

// func (c *mockConn) assertOnUnsubscribe(topics []string) {
// 	unsub := &common.Unsubscribe{Topics: topics, ID: 124}
// 	err := c.session.onUnsubscribe(unsub)
// 	assert.NoError(c.t, err)
// 	pkt := c.receiveS2C()
// 	unsuback := common.Unsuback{ID: 124}
// 	assert.Equal(c.t, unsuback.String(), pkt.String())
// }

// func (c *mockConn) assertPublish(topic string, qos common.QOS, payload []byte, retain bool) {
// 	msg := common.Message{Topic: topic, QOS: qos, Payload: payload, Retain: retain}
// 	pub := &common.Publish{Message: msg, Dup: false, ID: 123}
// 	c.Send(pub, true)
// 	if qos == 1 {
// 		pkt := c.receiveS2C()
// 		puback := common.Puback{ID: 123}
// 		assert.Equal(c.t, puback.String(), pkt.String())
// 	}
// }

// func (c *mockConn) assertOnPublish(topic string, qos common.QOS, payload []byte, retain bool) {
// 	msg := common.Message{Topic: topic, QOS: qos, Payload: payload, Retain: retain}
// 	pub := &common.Publish{Message: msg, Dup: false, ID: 124}
// 	err := c.session.onPublish(pub)
// 	assert.NoError(c.t, err)
// 	if qos == 1 {
// 		pkt := c.receiveS2C()
// 		puback := common.Puback{ID: 124}
// 		assert.Equal(c.t, puback.String(), pkt.String())
// 	}
// }

// func (c *mockConn) assertOnPublishError(topic string, qos common.QOS, payload []byte, e string) {
// 	msg := common.Message{Topic: topic, QOS: qos, Payload: payload}
// 	pub := &common.Publish{Message: msg, Dup: false, ID: 123}
// 	err := c.session.onPublish(pub)
// 	assert.EqualError(c.t, err, e)
// }

// func (c *mockConn) assertOnPuback(pid common.ID) {
// 	puback := common.NewPuback()
// 	puback.ID = pid
// 	err := c.session.onPuback(puback)
// 	assert.NoError(c.t, err)
// }

// func (c *mockConn) assertPersistedSubscriptions(l int) {
// 	subs, err := c.session.manager.recorder.getSubs(c.session.id)
// 	assert.NoError(c.t, err)
// 	assert.Len(c.t, subs, l)
// }

// func (c *mockConn) assertPersistedWillMessage(expected *common.Message) {
// 	will, err := c.session.manager.recorder.getWill(c.session.id)
// 	assert.NoError(c.t, err)
// 	if expected == nil {
// 		assert.Nil(c.t, will)
// 		return
// 	}
// 	assert.Equal(c.t, expected.QOS, will.QOS)
// 	assert.Equal(c.t, expected.Topic, will.Topic)
// 	assert.Equal(c.t, expected.Retain, will.Retain)
// 	assert.Equal(c.t, expected.Payload, will.Payload)
// }

// func (c *mockConn) assertRetainedMessage(cid string, qos common.QOS, topic string, pld []byte) {
// 	retained, err := c.session.manager.recorder.getRetained()
// 	assert.NoError(c.t, err)
// 	if cid == "" {
// 		assert.Len(c.t, retained, 0)
// 	} else {
// 		assert.Len(c.t, retained, 1)
// 		assert.Equal(c.t, qos, retained[0].QOS)
// 		assert.Equal(c.t, topic, retained[0].Topic)
// 		assert.Equal(c.t, pld, retained[0].Payload)
// 		assert.True(c.t, retained[0].Retain)
// 	}
// }

// func (c *mockConn) assertReceive(pid int, qos common.QOS, topic string, pld []byte, retain bool) {
// 	select {
// 	case pkt := <-c.s2c:
// 		assert.NotNil(c.t, pkt)
// 		assert.Equal(c.t, common.PUBLISH, pkt.Type())
// 		p, ok := pkt.(*common.Publish)
// 		assert.True(c.t, ok)
// 		assert.False(c.t, p.Dup)
// 		assert.Equal(c.t, retain, p.Message.Retain)
// 		assert.Equal(c.t, common.ID(pid), p.ID)
// 		assert.Equal(c.t, topic, p.Message.Topic)
// 		assert.Equal(c.t, qos, p.Message.QOS)
// 		assert.Equal(c.t, pld, p.Message.Payload)
// 		if qos == 1 {
// 			c.assertOnPuback(p.ID)
// 		}
// 	case <-time.After(time.Second * 3):
// 		assert.FailNow(c.t, "receive common timeout")
// 	}
// }

// func (c *mockConn) assertReceiveTimeout() {
// 	select {
// 	case pkt := <-c.c:
// 		assert.FailNow(c.t, "common is not expected : %s", pkt.String())
// 		return
// 	case <-time.After(time.Second):
// 		return
// 	}
// }
