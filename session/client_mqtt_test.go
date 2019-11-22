package session

import (
	"math/rand"
	"testing"

	"github.com/baetyl/baetyl-broker/common"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestSessionConnect(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// connect
	c := newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// disconnect
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again
	c = newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again after connect
	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again anonymous
	c = newMockConn(t)
	b.store.NewClientMQTT(c, true)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again cleansession=true
	c = newMockConn(t)
	b.store.NewClientMQTT(c, true)

	c.sendC2S(&common.Connect{ClientID: t.Name(), CleanSession: true, Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "")

}

func TestSessionConnectException(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// connect again with wrong version
	c := newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 0})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=1>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with wrong client id
	c = newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: "~!@#$%^&*()_+", Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=2>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with wrong password
	c = newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1x", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with empty username
	c = newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with empty password
	c = newMockConn(t)
	b.store.NewClientMQTT(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	b.assertSession(t.Name(), "")
}

// func TestSessionSub(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	defer c.close()
// 	c.assertOnConnectSuccess(t.Name(), false, nil)
// 	// Round 0
// 	c.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test", QOS: 0}})
// 	// Round 1
// 	c.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "talks"}, {Topic: "talks1", QOS: 1}, {Topic: "talks2", QOS: 1}})
// 	// Round 2
// 	c.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "talks", QOS: 1}, {Topic: "talks1"}, {Topic: "talks2", QOS: 1}})
// 	// Round 3
// 	c.assertOnSubscribe([]packet.Subscription{{Topic: "test", QOS: 2}}, []packet.QOS{128})
// 	// Round 4
// 	c.assertOnSubscribe(
// 		[]packet.Subscription{{Topic: "talks", QOS: 2}, {Topic: "talks1", QOS: 0}, {Topic: "talks2", QOS: 1}},
// 		[]packet.QOS{128, 0, 1},
// 	)
// 	// Round 5
// 	c.assertOnSubscribe(
// 		[]packet.Subscription{{Topic: "talks", QOS: 2}, {Topic: "talks1", QOS: 0}, {Topic: "temp", QOS: 1}},
// 		[]packet.QOS{128, 0, 128},
// 	)
// 	// Round 6
// 	c.assertOnSubscribe(
// 		[]packet.Subscription{{Topic: "talks", QOS: 2}, {Topic: "talks1#/", QOS: 0}, {Topic: "talks2", QOS: 1}},
// 		[]packet.QOS{128, 128, 1},
// 	)
// 	// Round 7
// 	c.assertOnSubscribe(
// 		[]packet.Subscription{{Topic: "talks", QOS: 2}, {Topic: "talks1#/", QOS: 0}, {Topic: "temp", QOS: 1}},
// 		[]packet.QOS{128, 128, 128},
// 	)
// }

// func TestSessionPub(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	defer c.close()
// 	c.assertOnConnectSuccess(t.Name(), false, nil)
// 	// Round 0
// 	c.assertOnPublish("test", 0, []byte("hello"), false)
// 	// Round 1
// 	c.assertOnPublish("test", 1, []byte("hello"), false)
// 	// Round 2
// 	c.assertOnPublishError("test", 2, []byte("hello"), "publish QOS (2) not supported")
// 	// Round 2
// 	c.assertOnPublishError("haha", 1, []byte("hello"), "publish topic (haha) not permitted")
// }

// func TestSessionUnSub(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	//sub client
// 	subC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	defer subC.close()
// 	subC.assertOnConnectSuccess("subC", false, nil)
// 	//unsub
// 	subC.assertOnUnsubscribe([]string{"test"})
// 	//sub
// 	subC.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test"}})
// 	//pub client
// 	pubC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	defer subC.close()
// 	pubC.assertOnConnectSuccess("pubC", false, nil)
// 	// pub
// 	pubC.assertOnPublish("test", 0, []byte("hello"), false)
// 	// receive message
// 	subC.assertReceive(0, 0, "test", []byte("hello"), false)
// 	// unsub
// 	subC.assertOnUnsubscribe([]string{"test"})
// 	// pub
// 	pubC.assertOnPublish("test", 0, []byte("hello"), false)
// 	subC.assertReceiveTimeout()
// }

// func TestSessionRetain(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	pubC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, pubC)
// 	defer pubC.close()
// 	pubC.assertOnConnectSuccess("pubC", false, nil)
// 	//pub [retain = false]
// 	pubC.assertOnPublish("test", 1, []byte("hello"), false)
// 	pubC.assertRetainedMessage("", 0, "", nil)
// 	//pub [retain = true]
// 	pubC.assertOnPublish("talks", 1, []byte("hello"), true)
// 	pubC.assertRetainedMessage("pubC", 1, "talks", []byte("hello"))
// 	//sub client1
// 	subC1 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC1)
// 	defer subC1.close()
// 	subC1.assertOnConnectSuccess("subC1", false, nil)
// 	// sub
// 	subC1.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "talks"}})
// 	subC1.assertReceive(0, 0, "talks", []byte("hello"), true)
// 	subC1.assertReceiveTimeout()

// 	// sub client2
// 	subC2 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC2)
// 	defer subC2.close()
// 	subC2.assertOnConnectSuccess("subC2", false, nil)
// 	// sub
// 	subC2.assertOnSubscribe([]packet.Subscription{{Topic: "talks", QOS: 1}}, []packet.QOS{1})
// 	subC2.assertReceive(65535, 1, "talks", []byte("hello"), true)
// 	subC2.assertReceiveTimeout()

// 	// pub remove retained message
// 	pubC.assertOnPublish("talks", 1, nil, true)
// 	pubC.assertRetainedMessage("", 0, "", nil)
// 	subC1.assertReceive(0, 0, "talks", nil, false) // ?
// 	subC2.assertReceive(3, 1, "talks", nil, false) // ?

// 	// sub client3
// 	subC3 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC3)
// 	defer subC3.close()
// 	subC3.assertOnConnectSuccess("subC3", false, nil)
// 	// sub
// 	subC3.assertOnSubscribe([]packet.Subscription{{Topic: "talks", QOS: 1}}, []packet.QOS{1})
// 	subC3.assertReceiveTimeout()
// }

// func TestSessionWill_v1(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	// Round 0 [connect without will msg]
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	c.assertOnConnectSuccess(t.Name()+"_1", false, nil)
// 	c.assertPersistedWillMessage(nil)
// 	c.close()
// 	// Round 1 [connect with will msg, retain flag = false]
// 	subC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	defer subC.close()
// 	subC.assertOnConnectSuccess(t.Name()+"_sub", false, nil)
// 	subC.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test"}})
// 	// connect with will msg
// 	c = newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	will := &packet.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: false}
// 	c.assertOnConnectSuccess(t.Name()+"_2", false, will)
// 	c.assertPersistedWillMessage(will)
// 	// c sends will message
// 	c.close()
// 	// subC receive will message
// 	subC.assertReceive(0, 0, will.Topic, will.Payload, false)
// }

// func TestSessionWill_v2(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	// sub will msg
// 	sub1C := newMockConn(t, r.sessions)
// 	assert.NotNil(t, sub1C)
// 	defer sub1C.close()
// 	sub1C.assertOnConnectSuccess(t.Name()+"_sub1", false, nil)
// 	sub1C.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test"}})

// 	// connect with will msg, retain flag = true
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	will := &packet.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: true}
// 	c.assertOnConnectSuccess(t.Name(), false, will)
// 	c.assertPersistedWillMessage(will)
// 	// crash
// 	c.close()
// 	c.assertRetainedMessage(t.Name(), will.QOS, will.Topic, will.Payload)
// 	sub1C.assertReceive(0, 0, will.Topic, will.Payload, false)

// 	sub2C := newMockConn(t, r.sessions)
// 	assert.NotNil(t, sub2C)
// 	defer sub2C.close()
// 	sub2C.assertOnConnectSuccess(t.Name()+"_sub2", false, nil)
// 	sub2C.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test"}})
// 	sub2C.assertReceive(0, 0, will.Topic, will.Payload, true)
// }

// func TestSessionWill_v3(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	// sub will msg
// 	sub1C := newMockConn(t, r.sessions)
// 	assert.NotNil(t, sub1C)
// 	defer sub1C.close()
// 	sub1C.assertOnConnectSuccess(t.Name()+"_sub1", false, nil)
// 	sub1C.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test"}})

// 	// connect with will msg, retain flag = true
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	defer c.close()
// 	will := &packet.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: true}
// 	c.assertOnConnectSuccess(t.Name(), false, will)
// 	c.assertPersistedWillMessage(will)
// 	//session receive disconnect packet
// 	c.session.close(false)
// 	c.assertPersistedWillMessage(nil)
// 	c.assertRetainedMessage("", 0, "", nil)
// 	sub1C.assertReceiveTimeout()
// }

// func TestSessionWill_v4(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	// connect with will msg, but no pub permission
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	defer c.close()
// 	will := &packet.Message{Topic: "haha", QOS: 1, Payload: []byte("hello"), Retain: false}
// 	c.assertOnConnectFail(t.Name(), will)
// }

// func TestCleanSession(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	pubC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, pubC)
// 	defer pubC.close()
// 	pubC.assertOnConnect("pubC", "u2", "p2", 3, "", packet.ConnectionAccepted)

// 	//Round 0 [clean session = false]
// 	subC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess("subC", false, nil)
// 	subC.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test", QOS: 1}})
// 	assert.Len(t, subC.session.subs, 1)
// 	assert.Contains(t, subC.session.subs, "test")
// 	assert.Equal(t, packet.QOS(1), subC.session.subs["test"].QOS)

// 	pubC.assertOnPublish("test", 0, []byte("hello"), false)
// 	subC.assertReceive(0, 0, "test", []byte("hello"), false)
// 	subC.close()

// 	//Round 1 [clean session from false to false]
// 	subC = newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess("subC", false, nil)
// 	assert.Len(t, subC.session.subs, 1)
// 	assert.Contains(t, subC.session.subs, "test")
// 	assert.Equal(t, packet.QOS(1), subC.session.subs["test"].QOS)

// 	pubC.assertOnPublish("test", 1, []byte("hello"), false)
// 	subC.assertReceive(1, 1, "test", []byte("hello"), false)
// 	subC.close()

// 	//Round 2 [clean session from false to true]
// 	subC = newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess("subC", true, nil)
// 	assert.Len(t, subC.session.subs, 0)

// 	pubC.assertOnPublish("test", 0, []byte("hello"), false)
// 	subC.assertReceiveTimeout()

// 	subC.close()

// 	//Round 2 [clean session from true to false]
// 	subC = newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess("subC", false, nil)
// 	assert.Len(t, subC.session.subs, 0)

// 	pubC.assertOnPublish("test", 0, []byte("hello"), false)
// 	subC.assertReceiveTimeout()

// 	subC.close()
// }

// func TestConnectWithSameClientID(t *testing.T) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	// client1
// 	subC1 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC1)
// 	subC1.assertOnConnectSuccess(t.Name(), false, nil)
// 	subC1.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test", QOS: 1}})

// 	// client 2
// 	subC2 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC2)
// 	subC2.assertOnConnectSuccess(t.Name(), false, nil)
// 	subC2.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test", QOS: 1}})

// 	nop := subC1.receive()
// 	assert.Nil(t, nop)
// 	subC2.assertReceiveTimeout()

// 	// pub
// 	pubC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, pubC)
// 	pubC.assertOnConnectSuccess(t.Name()+"_pub", false, nil)
// 	pubC.assertOnPublish("test", 1, []byte("hello"), false)

// 	nop = subC1.receive()
// 	assert.Nil(t, nop)
// 	subC2.assertReceive(1, 1, "test", []byte("hello"), false)
// 	subC2.close()
// }

// func TestSub0Pub0(t *testing.T) {
// 	testSubPub(t, 0, 0)
// }

// func TestSub0Pub1(t *testing.T) {
// 	testSubPub(t, 0, 1)
// }

// func TestSub1Pub0(t *testing.T) {
// 	testSubPub(t, 1, 0)
// }

// func TestSub1Pub1(t *testing.T) {
// 	testSubPub(t, 1, 1)
// }

// func testSubPub(t *testing.T, subQos packet.QOS, pubQos packet.QOS) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	//sub
// 	subC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess(t.Name()+"_sub", false, nil)
// 	subC.assertOnSubscribeSuccess([]packet.Subscription{{Topic: "test", QOS: subQos}})

// 	//pub
// 	pubC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, pubC)
// 	pubC.assertOnConnectSuccess(t.Name()+"_pub", false, nil)
// 	pubC.assertOnPublish("test", pubQos, []byte("hello"), false)

// 	tqos := subQos
// 	if subQos > pubQos {
// 		tqos = pubQos
// 	}
// 	pkt := subC.receive()
// 	publish, ok := pkt.(*packet.Publish)
// 	assert.True(t, ok)
// 	assert.False(t, publish.Message.Retain)
// 	assert.Equal(t, "test", publish.Message.Topic)
// 	assert.Equal(t, tqos, publish.Message.QOS)
// 	assert.Equal(t, []byte("hello"), publish.Message.Payload)
// 	if tqos != 1 {
// 		assert.Equal(t, packet.ID(0), publish.ID)
// 		assert.Equal(t, subC.session.pids.Size(), 0)
// 		return
// 	}
// 	assert.Equal(t, packet.ID(1), publish.ID)
// 	assert.Equal(t, subC.session.pids.Size(), 1)

// 	// subC resends message since client does not publish ack
// 	pkt = subC.receive()
// 	publish, ok = pkt.(*packet.Publish)
// 	assert.True(t, ok)
// 	assert.True(t, publish.Dup)
// 	assert.False(t, publish.Message.Retain)
// 	assert.Equal(t, packet.ID(1), publish.ID)
// 	assert.Equal(t, "test", publish.Message.Topic)
// 	assert.Equal(t, tqos, publish.Message.QOS)
// 	assert.Equal(t, []byte("hello"), publish.Message.Payload)
// 	// subC publishes ack
// 	subC.assertOnPuback(1)
// 	subC.assertReceiveTimeout()
// }

func TestCheckClientID(t *testing.T) {
	assert.True(t, checkClientID(""))
	assert.False(t, checkClientID(" "))
	assert.True(t, checkClientID("-"))
	assert.True(t, checkClientID("_"))
	assert.True(t, checkClientID(genRandomString(0)))
	assert.True(t, checkClientID(genRandomString(1)))
	assert.True(t, checkClientID(genRandomString(128)))
	assert.False(t, checkClientID(genRandomString(129)))
}

func genRandomString(n int) string {
	c := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
	b := make([]byte, n)
	for i := range b {
		b[i] = c[rand.Intn(len(c))]
	}
	return string(b)
}
