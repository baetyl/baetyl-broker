package session

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/256dpi/gomqtt/packet"
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
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":null}")

	// connect again
	c = newMockConn(t)
	b.manager.ClientMQTTHandler(c, false)

	c.sendC2S(&common.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":null}")

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
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionConnect\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":null}")

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
	b.assertSession(t.Name(), "{\"ID\":\"TestConnectWithSameClientID\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
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
	b.assertSession(t.Name(), "{\"ID\":\"TestConnectWithSameClientID\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
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
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
	b.assertExchangeCount(1)

	// subscribe talk
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "talks"}, {Topic: "talks1", QOS: 1}, {Topic: "talks2", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0, 1, 1]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":0,\"talks1\":1,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe talk again
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "talks", QOS: 1}, {Topic: "talks1", QOS: 1}, {Topic: "talks2", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1, 1, 0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":1,\"talks2\":0,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe wrong qos
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 2}, {Topic: "talks1", QOS: 0}, {Topic: "talks2", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 0, 1]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// subscribe with exceptions: wrong qos, no permit, wrong topic
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 2}, {Topic: "temp", QOS: 1}, {Topic: "talks1#/", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 128, 128]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// unsubscribe test
	c.sendC2S(&common.Unsubscribe{ID: 1, Topics: []string{"test"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1}}")
	b.assertExchangeCount(3)

	// subscribe test
	c.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1,\"test\":0}}")
	b.assertExchangeCount(4)

	// unsubscribe nonexists
	c.sendC2S(&common.Unsubscribe{ID: 1, Topics: []string{"test", "nonexists"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionSubscribe\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"talks\":1,\"talks1\":0,\"talks2\":1}}")
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
	b.assertSession(t.Name(), "{\"ID\":\"TestSessionPublish\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":null}")
	b.assertExchangeCount(0)

	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
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
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")
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

func TestSessionWill(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// connect with will message
	pub := newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)
	pub.sendC2S(&common.Connect{ClientID: "pub", Will: &packet.Message{Topic: "test", QOS: 0, Payload: []byte("just for test"), Retain: false}, Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	sub := newMockConn(t)
	b.manager.ClientMQTTHandler(sub, false)
	sub.sendC2S(&common.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("--> pub client disconnect normally <--")

	// sub topic test
	sub.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSession("sub", "{\"ID\":\"sub\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")

	// pub client disconnect normally
	pub.sendC2S(&common.Disconnect{})
	pub.assertS2CPacketTimeout()
	pub.assertClosed(true)
	b.assertSession("pub", "{\"ID\":\"pub\",\"Will\":{\"Topic\":\"test\",\"Payload\":\"anVzdCBmb3IgdGVzdA==\",\"QOS\":0,\"Retain\":false},\"CleanSession\":false,\"Subscriptions\":null}")

	// sub client failed to receive message
	b.assertExchangeCount(1)
	sub.assertS2CPacketTimeout()
	sub.assertClosed(false)

	fmt.Println("--> pub client disconnect abnormally <--")
	// pub client reconnect again
	pub = newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)

	pub.sendC2S(&common.Connect{ClientID: "pub", Will: &packet.Message{Topic: "test", QOS: 0, Payload: []byte("just for test"), Retain: false}, Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSession("pub", "{\"ID\":\"pub\",\"Will\":{\"Topic\":\"test\",\"Payload\":\"anVzdCBmb3IgdGVzdA==\",\"QOS\":0,\"Retain\":false},\"CleanSession\":false,\"Subscriptions\":null}")

	// pub client disconnect abnormally
	pub.Close()
	pub.assertClosed(true)
	b.assertSession("pub", "{\"ID\":\"pub\",\"Will\":{\"Topic\":\"test\",\"Payload\":\"anVzdCBmb3IgdGVzdA==\",\"QOS\":0,\"Retain\":false},\"CleanSession\":false,\"Subscriptions\":null}")

	// sub client received will message
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[106 117 115 116 32 102 111 114 32 116 101 115 116]> Dup=false>")
	sub.sendC2S(&common.Puback{ID: 1})
	sub.assertS2CPacketTimeout()
}

func TestSessionRetain(t *testing.T) {
	b := newMockBroker(t)
	defer b.close()

	// client1 to publish
	pub := newMockConn(t)
	b.manager.ClientMQTTHandler(pub, false)
	pub.sendC2S(&common.Connect{ClientID: "pub", Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("\n--> 1. client1 publish topic 'test' with retain is true --> client2 subscribe topic 'test' --> client2 receive message of topic 'test' <--")

	// client1 publish message("online") to topic "test" with retain is true
	pkt := &common.Publish{}
	pkt.ID = 0
	pkt.Message.QOS = 1
	pkt.Message.Payload = []byte("online")
	pkt.Message.Topic = "test"
	pkt.Message.Retain = true
	pub.sendC2S(pkt)
	pub.assertS2CPacket("<Puback ID=0>")
	pub.assertS2CPacketTimeout()

	// client2 to connect
	sub1 := newMockConn(t)
	b.manager.ClientMQTTHandler(sub1, false)
	sub1.sendC2S(&common.Connect{ClientID: "sub1", Username: "u1", Password: "p1", Version: 3})
	sub1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client2 to subscribe topic test
	sub1.sendC2S(&common.Subscribe{ID: 1, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub1.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSession("sub1", "{\"ID\":\"sub1\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")

	// client2 to receive message
	sub1.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[111 110 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&common.Puback{ID: 1})
	sub1.assertS2CPacketTimeout()

	fmt.Println("\n--> 2. client1 republish topic 'test' with retain is true --> client2 receive message of topic 'test' <--")

	// client1 publish message("offline") to topic "test" with retain is true
	pkt.ID = 1
	pkt.Message.QOS = 0
	pkt.Message.Payload = []byte("offline")
	pub.sendC2S(pkt)

	// client2 to receive message, because it is already subscribed
	sub1.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[111 102 102 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&common.Puback{ID: 0})

	fmt.Println("\n--> 3. client3 subscribe topic 'test' --> client3 receive message of topic 'test' <--")

	// client3 to connect
	sub2 := newMockConn(t)
	b.manager.ClientMQTTHandler(sub2, false)
	sub2.sendC2S(&common.Connect{ClientID: "sub2", Username: "u3", Password: "p3", Version: 3})
	sub2.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client3 subscribe topic test
	sub2.sendC2S(&common.Subscribe{ID: 3, Subscriptions: []common.Subscription{{Topic: "test", QOS: 0}}})
	sub2.assertS2CPacket("<Suback ID=3 ReturnCodes=[0]>")
	b.assertSession("sub2", "{\"ID\":\"sub2\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":0}}")

	// client3 to receive retain message("offline")
	sub2.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[111 102 102 108 105 110 101]> Dup=false>")

	fmt.Println("\n--> 4. client1 republish message to topic 'test' --> client2 && client3 receive message of topic 'test' <--")

	// client1 republish message("online") to topic test with retain is false(latest retain message is "offline")
	pkt.ID = 2
	pkt.Message.QOS = 1
	pkt.Message.Payload = []byte("online")
	pkt.Message.Retain = false
	pub.sendC2S(pkt)
	pub.assertS2CPacket("<Puback ID=2>")
	pub.assertS2CPacketTimeout()

	// client2 && client3 receive message(already subscribed)
	sub1.assertS2CPacket("<Publish ID=2 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[111 110 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&common.Puback{ID: 2})
	sub2.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[111 110 108 105 110 101]> Dup=false>")

	fmt.Println("\n--> 5. client4 subscribe topic 'test' --> client4 receive message of topic 'test' <--")

	// client4 to connect
	sub3 := newMockConn(t)
	b.manager.ClientMQTTHandler(sub3, false)
	sub3.sendC2S(&common.Connect{ClientID: "sub3", Username: "u4", Password: "p4", Version: 3})
	sub3.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client4 subscribe topic test
	sub3.sendC2S(&common.Subscribe{ID: 4, Subscriptions: []common.Subscription{{Topic: "test", QOS: 1}}})
	sub3.assertS2CPacket("<Suback ID=4 ReturnCodes=[1]>")
	b.assertSession("sub3", "{\"ID\":\"sub3\",\"Will\":null,\"CleanSession\":false,\"Subscriptions\":{\"test\":1}}")
	b.assertExchangeCount(3)

	// client4 to receive retain message(latest retain message is "offline")
	sub3.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[111 102 102 108 105 110 101]> Dup=false>")
	sub3.sendC2S(&common.Puback{ID: 0})
	sub3.assertS2CPacketTimeout()
}

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
