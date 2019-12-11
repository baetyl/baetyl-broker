package session

import (
	"fmt"
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
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":null}")

	// disconnect
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again after connect
	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again anonymous
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, true)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again cleansession=true
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, true)

	c.sendC2S(&common.Connect{ClientID: t.Name(), CleanSession: true, Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSession(t.Name(), "")

}

func TestConnectWithSameClientID(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// client to publish
	pub := newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)
	pub.sendC2S(&common.Connect{ClientID: "pub", Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertClientCount(1)
	b.assertBindingCount("pub", 1)
	b.assertExchangeCount(0)

	// client 1
	c1 := newMockConn(t)
	b.manager.ClientMQTTHandler(c1, false)
	c1.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	c1.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	c1.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestConnectWithSameClientID\",\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
	b.assertClientCount(2)
	b.assertBindingCount(t.Name(), 1)
	b.assertExchangeCount(1)

	pkt := &common.Publish{}
	pkt.Message.Topic = "test"
	pkt.Message.Payload = []byte("hi")
	pub.sendC2S(pkt)
	c1.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	// client 2
	c2 := newMockConn(t)
	b.manager.ClientMQTTHandler(c2, false)
	c2.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c2.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c2.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	c2.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestConnectWithSameClientID\",\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
	b.assertClientCount(2)
	b.assertBindingCount(t.Name(), 1)
	b.assertExchangeCount(1)

	pub.sendC2S(pkt)
	c2.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	// 'c1' is closed during 'c2' connecting
	c1.assertClosed(true)
	c2.assertClosed(false)
}

func TestSessionConnectException(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// connect again with wrong version
	c := newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 0})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=1>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with wrong client id
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: "~!@#$%^&*()_+", Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=2>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with wrong password
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1x", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with empty username
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// connect again with empty password
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	b.assertSession(t.Name(), "")
}

func TestSessionSubscribe(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	c := newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)
	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertExchangeCount(0)

	// subscribe test
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
	b.assertExchangeCount(1)

	// subscribe talk
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "talks"}, {Topic: "talks1", QOS: 1}, {Topic: "talks2", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0, 1, 1]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":0,\"talks1\":1,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe talk again
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "talks", QOS: 1}, {Topic: "talks1", QOS: 1}, {Topic: "talks2", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1, 1, 0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":1,\"talks2\":0,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe wrong qos
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 2}, {Topic: "talks1", QOS: 0}, {Topic: "talks2", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 0, 1]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe with exceptions: wrong qos, no permit, wrong topic
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 2}, {Topic: "temp", QOS: 1}, {Topic: "talks1#/", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 128, 128]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// unsubscribe test
	c.sendC2S(&common.Unsubscribe{ID: 1, Topics: []string{"test"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1}}")
	b.assertExchangeCount(3)

	// subscribe test
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// unsubscribe nonexists
	c.sendC2S(&common.Unsubscribe{ID: 1, Topics: []string{"test", "nonexists"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1}}")
	b.assertExchangeCount(3)

	c.sendC2S(&common.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertExchangeCount(3)
	b.assertBindingCount(t.Name(), 0)

	// anonymous
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, true)
	c.sendC2S(&common.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertExchangeCount(3)
	b.assertBindingCount(t.Name(), 1)

	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "temp", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
}

func TestSessionPublish(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	c := newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)
	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertExchangeCount(0)

	// subscribe test
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionPublish\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	// publish test qos 0
	pkt := &common.Publish{}
	pkt.Message.Topic = "test"
	pkt.Message.Payload = []byte("hi")
	c.sendC2S(pkt)
	c.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	// publish test qos 1
	pkt.ID = 2
	pkt.Message.QOS = 1
	pkt.Message.Topic = "test"
	pkt.Message.Payload = []byte("hi")
	c.sendC2S(pkt)
	c.assertS2CPacket("<Puback ID=2>")
	c.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=false>")
	c.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=true>")
	c.sendC2S(&common.Puback{ID: 1})
	c.assertS2CPacketTimeout()

	// publish with wrong qos
	pkt.Message.QOS = 2
	c.sendC2S(pkt)
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)
	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")

	// publish without permit
	pkt.Message.QOS = 1
	pkt.Message.Topic = "no-permit"
	c.sendC2S(pkt)
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	// anonymous
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, true)
	c.sendC2S(&common.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertExchangeCount(1)
	b.assertBindingCount(t.Name(), 1)

	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "#", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")

	c.sendC2S(pkt)
	c.assertS2CPacket("<Puback ID=2>")
	c.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"no-permit\" QOS=1 Retain=false Payload=[104 105]> Dup=false>")
	c.assertClosed(false)
}

func TestCleanSession(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	pub := newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)
	pub.sendC2S(&common.Connect{ClientID: "pub", Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	sub := newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	pkt := &common.Publish{}
	pkt.Message.Topic = "test"
	pkt.Message.Payload = []byte("hi")
	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(1)

	fmt.Println("--> clean session from false to false <--")

	sub = newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	// * auto subscribe when cleansession=false
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(1)

	fmt.Println("--> clean session from false to true <--")

	sub = newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", CleanSession: true, Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession("sub", "")
	b.assertExchangeCount(1)

	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertSession("sub", "")
	b.assertExchangeCount(0)

	fmt.Println("--> clean session from true to true <--")

	sub = newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", CleanSession: true, Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSession("sub", "")
	b.assertExchangeCount(0)

	pub.sendC2S(pkt)
	sub.assertS2CPacketTimeout()
	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub", "")
	b.assertExchangeCount(1)

	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(0)

	fmt.Println("--> clean session from true to false <--")

	sub = newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":null}")
	b.assertExchangeCount(0)

	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(1)

	// publish message during 'sub' offline
	pub.sendC2S(pkt)

	sub = newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	// 'sub' can receive offline message when cleanession=false
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)

	pub.sendC2S(&common.Disconnect{})
	pub.assertS2CPacketTimeout()
	pub.assertClosed(true)
}

func TestPubSubQOS(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	pub := newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)
	pub.sendC2S(&common.Connect{ClientID: "pub", Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	sub := newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("--> pub qos 1 --> sub qos 1 <--")

	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(1)

	pkt := &common.Publish{}
	pkt.ID = 1
	pkt.Message.QOS = 1
	pkt.Message.Topic = "test"
	pkt.Message.Payload = []byte("hi")
	pub.sendC2S(pkt)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&common.Puback{ID: 1})
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 0 --> sub qos 1 <--")

	pkt.ID = 0
	pkt.Message.QOS = 0
	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 0 --> sub qos 0 <--")

	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
	b.assertExchangeCount(1)

	pub.sendC2S(pkt)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 1 --> sub qos 0 <--")

	pkt.ID = 2
	pkt.Message.QOS = 1
	pub.sendC2S(pkt)
	pub.assertS2CPacket("<Puback ID=2>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()
}

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
// 	subC1.assertOnSubscribeSuccess([]common.Subscription{{Topic: "talks"}})
// 	subC1.assertReceive(0, 0, "talks", []byte("hello"), true)
// 	subC1.assertReceiveTimeout()

// 	// sub client2
// 	subC2 := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC2)
// 	defer subC2.close()
// 	subC2.assertOnConnectSuccess("subC2", false, nil)
// 	// sub
// 	subC2.assertOnSubscribe([]common.Subscription{{Topic: "talks", QOS: 1}}, []common.QOS{1})
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
// 	subC3.assertOnSubscribe([]common.Subscription{{Topic: "talks", QOS: 1}}, []common.QOS{1})
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
// 	subC.assertOnSubscribeSuccess([]common.Subscription{{Topic: "test"}})
// 	// connect with will msg
// 	c = newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	will := &common.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: false}
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
// 	sub1C.assertOnSubscribeSuccess([]common.Subscription{{Topic: "test"}})

// 	// connect with will msg, retain flag = true
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	will := &common.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: true}
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
// 	sub2C.assertOnSubscribeSuccess([]common.Subscription{{Topic: "test"}})
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
// 	sub1C.assertOnSubscribeSuccess([]common.Subscription{{Topic: "test"}})

// 	// connect with will msg, retain flag = true
// 	c := newMockConn(t, r.sessions)
// 	assert.NotNil(t, c)
// 	defer c.close()
// 	will := &common.Message{Topic: "test", QOS: 1, Payload: []byte("hello"), Retain: true}
// 	c.assertOnConnectSuccess(t.Name(), false, will)
// 	c.assertPersistedWillMessage(will)
// 	//session receive disconnect common
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
// 	will := &common.Message{Topic: "haha", QOS: 1, Payload: []byte("hello"), Retain: false}
// 	c.assertOnConnectFail(t.Name(), will)
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

// func testSubPub(t *testing.T, subQos common.QOS, pubQos common.QOS) {
// 	b := newMockBroker(t)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, r)
// 	defer b.close()

// 	//sub
// 	subC := newMockConn(t, r.sessions)
// 	assert.NotNil(t, subC)
// 	subC.assertOnConnectSuccess(t.Name()+"_sub", false, nil)
// 	subC.assertOnSubscribeSuccess([]common.Subscription{{Topic: "test", QOS: subQos}})

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
// 	publish, ok := pkt.(*common.Publish)
// 	assert.True(t, ok)
// 	assert.False(t, publish.Message.Retain)
// 	assert.Equal(t, "test", publish.Message.Topic)
// 	assert.Equal(t, tqos, publish.Message.QOS)
// 	assert.Equal(t, []byte("hello"), publish.Message.Payload)
// 	if tqos != 1 {
// 		assert.Equal(t, common.ID(0), publish.ID)
// 		assert.Equal(t, subC.session.pids.Size(), 0)
// 		return
// 	}
// 	assert.Equal(t, common.ID(1), publish.ID)
// 	assert.Equal(t, subC.session.pids.Size(), 1)

// 	// subC resends message since client does not publish ack
// 	pkt = subC.receive()
// 	publish, ok = pkt.(*common.Publish)
// 	assert.True(t, ok)
// 	assert.True(t, publish.Dup)
// 	assert.False(t, publish.Message.Retain)
// 	assert.Equal(t, common.ID(1), publish.ID)
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
