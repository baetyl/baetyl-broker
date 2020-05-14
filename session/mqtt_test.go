package session

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
)

func TestSessionMqttConnect(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	// connect
	c := newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionCount(1)

	// disconnect
	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.waitClientReady(t.Name(), 0)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionCount(1)

	// connect again
	c = newMockConn(t)
	b.ses.Handle(c)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionCount(1)

	// connect again after connect
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.waitClientReady(t.Name(), 0)
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionCount(1)

	// connect again cleansession=true
	c = newMockConn(t)
	b.ses.Handle(c)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), CleanSession: true, Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionCount(1)

	fmt.Println("--> cleansession=true: send disconnect")

	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertSessionCount(0)
}

func TestSessionMqttConnectSameClientID(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	// client to publish
	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(&mqtt.Connect{ClientID: "pub", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.waitClientReady("pub", 1)
	b.assertSessionCount(1)
	b.assertExchangeCount(0)

	// client 1
	c1 := newMockConn(t)
	b.ses.Handle(c1)
	c1.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	c1.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	c1.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":0}}", nil)
	b.assertSessionCount(2)
	b.assertExchangeCount(1)

	pktpub := &mqtt.Publish{}
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("hi")
	pub.sendC2S(pktpub)
	c1.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	// client 2
	c2 := newMockConn(t)
	b.ses.Handle(c2)
	c2.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c2.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	c2.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	c2.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":0}}", nil)
	b.assertSessionCount(2)
	b.assertExchangeCount(1)

	pub.sendC2S(pktpub)
	c2.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	// 'c1' is closed during 'c2' connecting
	c1.assertClosed(true)
	c2.assertClosed(false)
}

func TestSessionMqttConnectException(t *testing.T) {
	b := newMockBroker(t, testConfSession)
	defer b.closeAndClean()

	// connect again with wrong version
	c := newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 0})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=1>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionCount(0)

	// connect again with wrong client id
	c = newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: "~!@#$%^&*()_+", Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=2>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionCount(0)

	// connect again with wrong password
	c = newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Password: "p1x", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionCount(0)

	// connect again with empty username
	c = newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionCount(0)

	// connect again with empty password
	c = newMockConn(t)
	b.ses.Handle(c)
	b.assertSessionCount(1)

	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=4>")
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	b.assertSessionCount(0)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
}

func TestSessionMqttMaxSessions(t *testing.T) {
	b := newMockBroker(t, testConfSession)
	defer b.closeAndClean()

	c1 := newMockConn(t)
	b.ses.Handle(c1)
	b.assertSessionCount(1)
	c1.assertClosed(false)

	c2 := newMockConn(t)
	b.ses.Handle(c2)
	b.assertSessionCount(2)
	c2.assertClosed(false)

	c3 := newMockConn(t)
	b.ses.Handle(c3)
	b.assertSessionCount(3)
	c3.assertClosed(false)

	c4 := newMockConn(t)
	b.ses.Handle(c4)
	b.assertSessionCount(3)
	c4.assertClosed(true)

	// c1 sends connect with cleansession=false
	c1.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertSessionCount(3)

	// c1 sends disconnect, but session still exists
	c1.sendC2S(&mqtt.Disconnect{})
	c1.assertS2CPacketTimeout()
	c1.assertClosed(true)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertSessionCount(3)

	// c4 connects again, but it is refused
	c4 = newMockConn(t)
	b.ses.Handle(c4)
	b.assertSessionCount(3)
	c4.assertClosed(true)

	// c2 sends connect with cleansession=true
	c2.sendC2S(&mqtt.Connect{ClientID: "c2", CleanSession: true, Username: "u1", Password: "p1", Version: 3})
	c2.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionStore("c2", "", errors.New("No data found for this key"))
	b.assertSessionCount(3)

	// c1 sends disconnect, but session still exists
	c2.sendC2S(&mqtt.Disconnect{})
	c2.assertS2CPacketTimeout()
	c2.assertClosed(true)
	b.assertSessionStore("c2", "", errors.New("No data found for this key"))
	b.assertSessionCount(2)

	// c4 connects again
	c4 = newMockConn(t)
	b.ses.Handle(c4)
	b.assertSessionCount(3)
	c4.assertClosed(false)
}

func TestSessionMqttSubscribe(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	c := newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionState(t.Name(), STATE0)
	b.assertExchangeCount(0)

	// subscribe test
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(1)

	// subscribe talk
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "talks"}, {Topic: "$baidu/iot", QOS: 1}, {Topic: "$link/data", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0, 128, 1]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":0,\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(3)

	// subscribe talk again
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "talks", QOS: 1}, {Topic: "$baidu/iot", QOS: 1}, {Topic: "$link/data", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1, 128, 0]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":0,\"talks\":1,\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(3)

	// subscribe wrong qos
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 2}, {Topic: "$baidu/iot", QOS: 0}, {Topic: "$link/data", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 128, 1]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":1,\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(3)

	// subscribe with exceptions: wrong qos, no permit, wrong topic
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 2}, {Topic: "temp", QOS: 1}, {Topic: "talks1#/", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[128, 1, 128]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":1,\"temp\":1,\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(4)

	// unsubscribe test
	c.sendC2S(&mqtt.Unsubscribe{ID: 1, Topics: []string{"test"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":1,\"temp\":1}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(3)

	// subscribe test
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":1,\"temp\":1,\"test\":0}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(4)

	// unsubscribe nonexists
	c.sendC2S(&mqtt.Unsubscribe{ID: 1, Topics: []string{"test", "nonexists"}})
	c.assertS2CPacket("<Unsuback ID=1>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$link/data\":1,\"talks\":1,\"temp\":1}}", nil)
	b.assertSessionState(t.Name(), STATE1)
	b.assertExchangeCount(3)

	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.waitClientReady(t.Name(), 0)
	b.assertSessionState(t.Name(), STATE2)
	b.assertSessionCount(1)
	b.assertExchangeCount(3)

	// again
	c = newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.waitClientReady(t.Name(), 1)
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionCount(1)
	b.assertExchangeCount(3)

	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "temp", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
}

func TestSessionMqttPublish(t *testing.T) {
	b := newMockBroker(t, testConfSession)
	defer b.closeAndClean()

	c := newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertExchangeCount(0)

	// subscribe test
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}, {Topic: "$baidu/iot", QOS: 1}, {Topic: "$link/data", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1, 1, 1]>")
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"$baidu/iot\":1,\"$link/data\":1,\"test\":1}}", nil)
	b.assertExchangeCount(3)

	fmt.Println("--> publish topic test qos 0 <--")

	pktpub := &mqtt.Publish{}
	pktpub.Message.QOS = 0
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("hi")
	c.sendC2S(pktpub)
	c.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")

	fmt.Println("--> publish topic test qos 1 <--")

	pktpub.ID = 2
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("hi")
	c.sendC2S(pktpub)
	c.assertS2CPacket("<Puback ID=2>")
	c.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=false>")
	c.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=true>")
	c.sendC2S(&mqtt.Puback{ID: 1})
	c.assertS2CPacketTimeout()

	fmt.Println("--> publish topic $baidu/iot qos 1 <--")

	pktpub.ID = 3
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "$baidu/iot"
	pktpub.Message.Payload = []byte("baidu iot test")
	c.sendC2S(pktpub)
	c.assertS2CPacket("<Puback ID=3>")
	c.assertS2CPacket("<Publish ID=2 Message=<Message Topic=\"$baidu/iot\" QOS=1 Retain=false Payload=[98 97 105 100 117 32 105 111 116 32 116 101 115 116]> Dup=false>")
	c.assertS2CPacket("<Publish ID=2 Message=<Message Topic=\"$baidu/iot\" QOS=1 Retain=false Payload=[98 97 105 100 117 32 105 111 116 32 116 101 115 116]> Dup=true>")
	c.sendC2S(&mqtt.Puback{ID: 2})
	c.assertS2CPacketTimeout()

	// publish $link/data topic qos 0
	pktpub.ID = 4
	pktpub.Message.QOS = 0
	pktpub.Message.Topic = "$link/data"
	pktpub.Message.Payload = []byte("module link test")
	c.sendC2S(pktpub)
	c.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"$link/data\" QOS=0 Retain=false Payload=[109 111 100 117 108 101 32 108 105 110 107 32 116 101 115 116]> Dup=false>")

	// publish with wrong qos
	pktpub.Message.QOS = 2
	c.sendC2S(pktpub)
	c.assertS2CPacketTimeout()
	c.assertClosed(true)

	c = newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Username: "u1", Password: "p1", Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")

	// publish without permit
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "no-permit"
	c.sendC2S(pktpub)
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
}

func TestSessionMqttCleanSession(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(&mqtt.Connect{ClientID: "pub", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	sub := newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	pktpub0 := &mqtt.Publish{}
	pktpub0.Message.Topic = "test"
	pktpub0.Message.Payload = []byte("hi0")
	pktpub1 := &mqtt.Publish{}
	pktpub1.ID = 1
	pktpub1.Message.QOS = 1
	pktpub1.Message.Topic = "test"
	pktpub1.Message.Payload = []byte("hi1")
	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105 48]> Dup=false>")
	sub.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105 49]> Dup=false>")
	sub.sendC2S(&mqtt.Suback{ID: 1})
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(1)

	fmt.Println("--> clean session from false to false <--")

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	// * auto subscribe when cleansession=false
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105 48]> Dup=false>")
	sub.assertS2CPacket("<Publish ID=2 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105 49]> Dup=false>")
	sub.sendC2S(&mqtt.Suback{ID: 1})
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(1)

	fmt.Println("--> clean session from false to true <--")

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", CleanSession: true, Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSessionStore("sub", "", errors.New("No data found for this key"))
	b.assertExchangeCount(1)

	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105 48]> Dup=false>")
	sub.assertS2CPacket("<Publish ID=3 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105 49]> Dup=false>")
	sub.sendC2S(&mqtt.Suback{ID: 1})
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertSessionStore("sub", "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)

	fmt.Println("--> clean session from true to true <--")

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", CleanSession: true, Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionStore("sub", "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)

	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacketTimeout()

	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "", errors.New("No data found for this key"))
	b.assertExchangeCount(1)

	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105 48]> Dup=false>")
	sub.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105 49]> Dup=false>")
	sub.sendC2S(&mqtt.Suback{ID: 1})
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertExchangeCount(0)

	fmt.Println("--> clean session from true to false <--")

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionStore("sub", "{\"id\":\"sub\"}", nil)
	b.assertExchangeCount(0)

	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.waitClientReady("sub", 0)
	b.assertSessionState("sub", STATE2)
	b.assertExchangeCount(1)

	// publish message during 'sub' offline
	pub.sendC2S(pktpub0)
	pub.sendC2S(pktpub1)
	pub.assertS2CPacket("<Puback ID=1>")

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.waitClientReady("sub", 1)
	b.assertSessionState("sub", STATE1)
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// 'sub' can only receive offline message with qos 1 when cleanession=false
	sub.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105 49]> Dup=false>")
	sub.sendC2S(&mqtt.Suback{ID: 1})
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)

	pub.sendC2S(&mqtt.Disconnect{})
	pub.assertS2CPacketTimeout()
	pub.assertClosed(true)
}

func TestSessionMqttAllStates(t *testing.T) {
	b := newMockBroker(t, testConfDefault)

	// [cleansession=false] c connects, session is in state0
	c := newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionState(t.Name(), STATE0)
	b.assertExchangeCount(0)

	// [cleansession=false] c subscribes, session is in state1
	c.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// [cleansession=false] c unsubscribes, session is in state0
	c.sendC2S(&mqtt.Unsubscribe{ID: 2, Topics: []string{"test"}})
	c.assertS2CPacket("<Unsuback ID=2>")
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\"}", nil)
	b.assertExchangeCount(0)

	// [cleansession=false] c subscribes again, session is in state1
	c.sendC2S(&mqtt.Subscribe{ID: 3, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=3 ReturnCodes=[1]>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// [cleansession=false] c disconnects, session is in state2
	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionState(t.Name(), STATE2)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// broker closes unexpected, session queue data is already stored
	b.close()

	// broker restarts, persisted session will be started in state2
	b = newMockBroker(t, testConfDefault)
	b.assertSessionState(t.Name(), STATE2)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)

	// [cleansession=false] c connects again, session is in state1
	c = newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// [cleansession=false] c disconnects, session is in state2
	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionState(t.Name(), STATE2)
	b.assertSessionStore(t.Name(), "{\"id\":\""+t.Name()+"\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	// [cleansession=true] c connects again, session is in state1
	c = newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), CleanSession: true, Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(1)

	// [cleansession=true] c unsubscribes, session is removed
	c.sendC2S(&mqtt.Unsubscribe{ID: 4, Topics: []string{"test"}})
	c.assertS2CPacket("<Unsuback ID=4>")
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)

	// [cleansession=true] c subscribes again, session is in state1
	c.sendC2S(&mqtt.Subscribe{ID: 5, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=5 ReturnCodes=[1]>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(1)

	// broker closes unexpected, session queue date is deleted
	b.close()

	// broker start, connect send disconnect packet, queue data will be deleted when cleanSession is true
	b = newMockBroker(t, testConfDefault)
	defer b.closeAndClean()
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)

	// [cleansession=true] c connects again, session is in state1
	c = newMockConn(t)
	b.ses.Handle(c)
	c.sendC2S(&mqtt.Connect{ClientID: t.Name(), CleanSession: true, Version: 3})
	c.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	b.assertSessionState(t.Name(), STATE0)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)

	// [cleansession=true] c subscribes again, session is in state1
	c.sendC2S(&mqtt.Subscribe{ID: 6, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	c.assertS2CPacket("<Suback ID=6 ReturnCodes=[1]>")
	b.assertSessionState(t.Name(), STATE1)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(1)

	// [cleansession=true] c disconnects, session is removed
	c.sendC2S(&mqtt.Disconnect{})
	c.assertS2CPacketTimeout()
	c.assertClosed(true)
	b.assertSessionStore(t.Name(), "", errors.New("No data found for this key"))
	b.assertExchangeCount(0)
}

func TestSessionMqttPubSubQOS(t *testing.T) {
	b := newMockBroker(t, testConfSession)
	defer b.closeAndClean()

	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(&mqtt.Connect{ClientID: "pub", Username: "u2", Password: "p2", Version: 3})
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	sub := newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("--> pub qos 1 --> sub qos 1 <--")

	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	pktpub := &mqtt.Publish{}
	pktpub.ID = 1
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("hi")
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=1>")
	sub.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[104 105]> Dup=false>")
	sub.sendC2S(&mqtt.Puback{ID: 1})
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 0 --> sub qos 1 <--")

	pktpub.ID = 0
	pktpub.Message.QOS = 0
	pub.sendC2S(pktpub)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 0 --> sub qos 0 <--")

	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":0}}", nil)
	b.assertExchangeCount(1)

	pub.sendC2S(pktpub)
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()

	fmt.Println("--> pub qos 1 --> sub qos 0 <--")

	pktpub.ID = 2
	pktpub.Message.QOS = 1
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=2>")
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	sub.assertS2CPacketTimeout()
}

func TestSessionMqttSystemTopicIsolation(t *testing.T) {
	b := newMockBroker(t, testConfSession)
	defer b.closeAndClean()

	// pubc connect to broker
	pubc := newMockConn(t)
	b.ses.Handle(pubc)
	pubc.sendC2S(&mqtt.Connect{ClientID: "pubc", Username: "u1", Password: "p1", Version: 4})
	pubc.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// subc connect to broker
	subc := newMockConn(t)
	b.ses.Handle(subc)
	subc.sendC2S(&mqtt.Connect{ClientID: "subc", Username: "u1", Password: "p1", Version: 4})
	subc.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// subc subscribe topic #
	pktsub := &mqtt.Subscribe{}
	pktsub.ID = 1
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "#", QOS: 0}}
	subc.sendC2S(pktsub)
	subc.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore("subc", "{\"id\":\"subc\",\"subs\":{\"#\":0}}", nil)
	b.assertExchangeCount(1)

	fmt.Println("\n--> pubc publish message with topic test, subc will receive message <--")

	pktpub := &mqtt.Publish{}
	pktpub.ID = 1
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("hi")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacket("<Puback ID=1>")
	subc.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[104 105]> Dup=false>")
	subc.assertS2CPacketTimeout()

	fmt.Println("\n--> pubc publish message with topic $link/data, subc will not receive message <--")

	pktpub = &mqtt.Publish{}
	pktpub.ID = 2
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "$link/data"
	pktpub.Message.Payload = []byte("hello")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacket("<Puback ID=2>")
	subc.assertS2CPacketTimeout()

	// subc unsubscribe topic #
	pktunsub := &mqtt.Unsubscribe{}
	pktunsub.ID = 1
	pktunsub.Topics = []string{"#"}
	subc.sendC2S(pktunsub)
	subc.assertS2CPacket("<Unsuback ID=1>")
	b.assertSessionStore("subc", "{\"id\":\"subc\"}", nil)
	b.assertExchangeCount(0)

	// subc subscribe topic $link/#
	pktsub = &mqtt.Subscribe{}
	pktsub.ID = 2
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "$link/#", QOS: 0}}
	subc.sendC2S(pktsub)
	subc.assertS2CPacket("<Suback ID=2 ReturnCodes=[0]>")
	b.assertSessionStore("subc", "{\"id\":\"subc\",\"subs\":{\"$link/#\":0}}", nil)
	b.assertExchangeCount(1)

	fmt.Println("\n--> pubc publish message with topic test, subc will not receive message <--")

	pktpub = &mqtt.Publish{}
	pktpub.ID = 3
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("test")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacket("<Puback ID=3>")
	subc.assertS2CPacketTimeout()

	fmt.Println("\n--> pubc publish message with topic $baidu/data, subc will not receive message <--")

	pktpub = &mqtt.Publish{}
	pktpub.ID = 4
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "$baidu/iot"
	pktpub.Message.Payload = []byte("iot test")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacket("<Puback ID=4>")
	subc.assertS2CPacketTimeout()

	fmt.Println("\n--> pubc publish message with topic  $link/data, subc will receive message <--")

	pktpub = &mqtt.Publish{}
	pktpub.ID = 5
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "$link/data"
	pktpub.Message.Payload = []byte("hello")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacket("<Puback ID=5>")
	subc.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"$link/data\" QOS=0 Retain=false Payload=[104 101 108 108 111]> Dup=false>")
	subc.assertS2CPacketTimeout()

	fmt.Println("\n--> pubc publish message with topic $SYS/data (not configured),  pubc will be closed<--")

	pktpub = &mqtt.Publish{}
	pktpub.ID = 6
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "$SYS/data"
	pktpub.Message.Payload = []byte("system data")
	pubc.sendC2S(pktpub)
	pubc.assertS2CPacketTimeout()
	pubc.assertClosed(true)

	// client subscribe unspecified sysTopic $SYS/data, client will not subscribe successfully
	pktsub = &mqtt.Subscribe{}
	pktsub.ID = 3
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "$SYS/data", QOS: 0}}
	subc.sendC2S(pktsub)
	subc.assertS2CPacket("<Suback ID=3 ReturnCodes=[128]>")
	b.assertSessionStore("subc", "{\"id\":\"subc\",\"subs\":{\"$link/#\":0}}", nil)
	subc.assertClosed(false)
}

func TestSessionMqttReCheckInvalidTopic(t *testing.T) {
	var testSessionConf = `
session:
 sysTopics:
 - $link
 - $baidu
`
	b := newMockBroker(t, testSessionConf)

	sub := newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "$baidu/iot", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"$baidu/iot\":1}}", nil)
	b.assertExchangeCount(1)

	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.close()

	// broker restart with new configuration
	b = newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	// load the stored session
	b.assertSessionStore("sub", "{\"id\":\"sub\"}", nil)
	b.assertExchangeCount(0)

	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	// * auto subscribe when cleansession=false
	b.assertSessionStore("sub", "{\"id\":\"sub\"}", nil)
	b.assertExchangeCount(0)

	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
}

func TestSessionMqttReCheckNonPermittedTopic(t *testing.T) {
	b := newMockBroker(t, testConfSession)

	sub := newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")
	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 1}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)

	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.close()

	// broker restart with new configuration
	var testSessionConf = `
principals:
- username: u1
  password: p1
  permissions:
  - action: sub
    permit: [talks, talks1, talks2, '$baidu/iot', '$link/data', '$link/#']
  - action: pub
    permit: [test, talks, '$baidu/iot', '$link/data']
`
	b = newMockBroker(t, testSessionConf)
	defer b.closeAndClean()

	// load the stored session
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":1}}", nil)
	b.assertExchangeCount(1)
	sub = newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(&mqtt.Connect{ClientID: "sub", Username: "u1", Password: "p1", Version: 3})
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	// * auto subscribe when cleansession=false
	b.assertSessionStore("sub", "{\"id\":\"sub\"}", nil)
	b.assertExchangeCount(0)

	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
}

func TestSessionMqttWill(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	// connect packet
	pktcon := &mqtt.Connect{}
	pktcon.Version = 3
	pktcon.Will = nil

	// sub client connect without Will message
	pktcon.ClientID = "sub"
	sub := newMockConn(t)
	b.ses.Handle(sub)
	sub.sendC2S(pktcon)
	sub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// sub client subscribe topic test
	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":0}}", nil)

	// pub client connect with Will message, retain is false
	pktwill := mqtt.NewPublish()
	pktwill.Message.Topic = "test"
	pktwill.Message.Retain = false
	pktwill.Message.Payload = []byte("will retain is false")
	pktcon.ClientID = "pub-will-retain-false-1"
	pktcon.Will = &pktwill.Message
	pub1 := newMockConn(t)
	b.ses.Handle(pub1)
	pub1.sendC2S(pktcon)
	pub1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("--> 1. pub client disconnect normally <--")

	// pub client disconnect normally
	pub1.sendC2S(&mqtt.Disconnect{})
	pub1.assertS2CPacketTimeout()
	pub1.assertClosed(true)
	b.assertSessionStore("pub-will-retain-false-1", "{\"id\":\"pub-will-retain-false-1\",\"will\":{\"Context\":{\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgZmFsc2U=\"}}", nil)
	b.assertExchangeCount(1)

	// sub client failed to receive message
	sub.assertS2CPacketTimeout()
	sub.assertClosed(false)

	fmt.Println("--> 2. pub client disconnect abnormally <--")

	// pub client reconnect again
	pub1 = newMockConn(t)
	b.ses.Handle(pub1)
	pub1.sendC2S(pktcon)
	pub1.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSessionStore("pub-will-retain-false-1", "{\"id\":\"pub-will-retain-false-1\",\"will\":{\"Context\":{\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgZmFsc2U=\"}}", nil)

	// pub client disconnect abnormally
	pub1.Close()
	pub1.assertClosed(true)
	b.assertSessionStore("pub-will-retain-false-1", "{\"id\":\"pub-will-retain-false-1\",\"will\":{\"Context\":{\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgZmFsc2U=\"}}", nil)

	// sub client received Will message
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[119 105 108 108 32 114 101 116 97 105 110 32 105 115 32 102 97 108 115 101]> Dup=false>")

	// pub client connect with will message, retain is true
	pub2 := newMockConn(t)
	b.ses.Handle(pub2)
	pktcon.ClientID = "pub-will-retain-true-1"
	pktwill.Message.Payload = []byte("will retain is true")
	pktwill.Message.Retain = true
	pktcon.Will = &pktwill.Message
	pub2.sendC2S(pktcon)
	pub2.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	fmt.Println("--> 1. pub client disconnect normally <--")

	// pub client disconnect normally
	pub2.sendC2S(&mqtt.Disconnect{})
	pub2.assertS2CPacketTimeout()
	pub2.assertClosed(true)
	b.assertSessionStore("pub-will-retain-true-1", "{\"id\":\"pub-will-retain-true-1\",\"will\":{\"Context\":{\"Flags\":1,\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgdHJ1ZQ==\"}}", nil)
	b.assertExchangeCount(1)

	// sub client failed to receive message
	sub.assertS2CPacketTimeout()
	sub.assertClosed(false)

	fmt.Println("--> 2. pub client disconnect abnormally <--")

	// pub client reconnect again
	pub2 = newMockConn(t)
	b.ses.Handle(pub2)
	pub2.sendC2S(pktcon)
	pub2.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")
	b.assertSessionStore("pub-will-retain-true-1", "{\"id\":\"pub-will-retain-true-1\",\"will\":{\"Context\":{\"Flags\":1,\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgdHJ1ZQ==\"}}", nil)

	// pub client disconnect abnormally
	pub2.Close()
	pub2.assertClosed(true)
	b.assertSessionStore("pub-will-retain-true-1", "{\"id\":\"pub-will-retain-true-1\",\"will\":{\"Context\":{\"Flags\":1,\"Topic\":\"test\"},\"Content\":\"d2lsbCByZXRhaW4gaXMgdHJ1ZQ==\"}}", nil)

	// sub client received Will message
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=false Payload=[119 105 108 108 32 114 101 116 97 105 110 32 105 115 32 116 114 117 101]> Dup=false>")

	// sub client disconnect normally
	sub.sendC2S(&mqtt.Disconnect{})
	sub.assertS2CPacketTimeout()
	sub.assertClosed(true)
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":0}}", nil)

	// sub client reconnect, will receive message("will retain is true")
	sub = newMockConn(t)
	b.ses.Handle(sub)
	pktcon.Will = nil
	pktcon.ClientID = "sub"
	sub.sendC2S(pktcon)
	sub.assertS2CPacket("<Connack SessionPresent=true ReturnCode=0>")

	// sub client subscribe topic test
	sub.sendC2S(&mqtt.Subscribe{ID: 1, Subscriptions: []mqtt.Subscription{{Topic: "test", QOS: 0}}})
	sub.assertS2CPacket("<Suback ID=1 ReturnCodes=[0]>")
	b.assertSessionStore("sub", "{\"id\":\"sub\",\"subs\":{\"test\":0}}", nil)

	// sub client receive message("will retain is true"), retain flag is true
	sub.assertS2CPacket("<Publish ID=0 Message=<Message Topic=\"test\" QOS=0 Retain=true Payload=[119 105 108 108 32 114 101 116 97 105 110 32 105 115 32 116 114 117 101]> Dup=false>")
}

func TestSessionMqttRetain(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	pktcon := &mqtt.Connect{}
	pktcon.Version = 3
	pktcon.Will = nil

	pktpub := &mqtt.Publish{}
	pktsub := &mqtt.Subscribe{}

	fmt.Println("\n--> 1. client1 publish topic 'test' and 'talks' with retain is true --> client2 subscribe topic 'test' --> client2 receive message of topic 'test' as retain<--")

	// client1 to connect
	pktcon.ClientID = "pub"
	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(pktcon)
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client1 publish message("online") to topic "test" with retain is true
	pktpub.ID = 0
	pktpub.Message.QOS = 1
	pktpub.Message.Payload = []byte("online")
	pktpub.Message.Topic = "test"
	pktpub.Message.Retain = true
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=0>")
	pub.assertS2CPacketTimeout()

	// client1 publish message("hi") to topic "talks" with retain is true
	pktpub.ID = 1
	pktpub.Message.Payload = []byte("hi")
	pktpub.Message.Topic = "talks"
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=1>")
	pub.assertS2CPacketTimeout()

	// check retain message
	msgs, err := b.ses.listRetainedMessages()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(msgs))

	// client2 to connect
	pktcon.ClientID = "sub1"
	sub1 := newMockConn(t)
	b.ses.Handle(sub1)
	sub1.sendC2S(pktcon)
	sub1.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client2 to subscribe topic test
	pktsub.ID = 1
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "test", QOS: 1}}
	sub1.sendC2S(pktsub)
	sub1.assertS2CPacket("<Suback ID=1 ReturnCodes=[1]>")
	b.assertSessionStore("sub1", "{\"id\":\"sub1\",\"subs\":{\"test\":1}}", nil)

	// client2 to receive message
	sub1.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=true Payload=[111 110 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&mqtt.Puback{ID: 1})
	sub1.assertS2CPacketTimeout()

	fmt.Println("\n--> 2. client1 republish topic 'test' with retain is false --> client2 receive message of topic 'test' <--")

	pktpub.ID = 2
	pktpub.Message.Topic = "test"
	pktpub.Message.Payload = []byte("offline")
	pktpub.Message.Retain = false
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=2>")
	pub.assertS2CPacketTimeout()

	// client2 to receive message of topic test as normal message, because it is already subscribed
	sub1.assertS2CPacket("<Publish ID=2 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[111 102 102 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&mqtt.Puback{ID: 2})

	fmt.Println("\n--> 3. client1 republish topic 'test' with retain is ture --> client2 receive message of topic 'test' <--")

	// client1 publish message("offline") to topic "test" with retain is false
	pktpub.ID = 3
	pktpub.Message.Retain = true
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=3>")
	pub.assertS2CPacketTimeout()

	// client2 to receive message of topic test as normal message, because it is already subscribed
	sub1.assertS2CPacket("<Publish ID=3 Message=<Message Topic=\"test\" QOS=1 Retain=false Payload=[111 102 102 108 105 110 101]> Dup=false>")
	sub1.sendC2S(&mqtt.Puback{ID: 2})

	fmt.Println("\n--> 4. client3 subscribe topic 'test' --> client3 Will receive message of topic 'test' <--")

	// client3 to connect
	pktcon.ClientID = "sub2"
	sub2 := newMockConn(t)
	b.ses.Handle(sub2)
	sub2.sendC2S(pktcon)
	sub2.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client3 subscribe topic test. If retain message of topic test exists, client3 Will receive the message.
	pktsub.ID = 4
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "test", QOS: 1}}
	sub2.sendC2S(pktsub)
	sub2.assertS2CPacket("<Suback ID=4 ReturnCodes=[1]>")
	b.assertSessionStore("sub2", "{\"id\":\"sub2\",\"subs\":{\"test\":1}}", nil)

	// client3 Will receive retain message("online")
	sub2.assertS2CPacket("<Publish ID=1 Message=<Message Topic=\"test\" QOS=1 Retain=true Payload=[111 102 102 108 105 110 101]> Dup=false>")
	sub2.sendC2S(&mqtt.Puback{ID: 1})
	sub2.assertS2CPacketTimeout()

	fmt.Println("\n--> 5. clear retain message of topic 'test' --> client4 subscribe topic 'test' --> client4 Will not receive message of topic 'test'<--")

	// clear retain message of topic test
	pktpub.ID = 5
	pktpub.Message.Payload = nil
	pktpub.Message.Retain = true

	// client1 republish message with topic test of retain, and set the payload is nil
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=5>")
	pub.assertS2CPacketTimeout()

	// client4 to connect
	pktcon.ClientID = "sub3"
	sub3 := newMockConn(t)
	b.ses.Handle(sub3)
	sub3.sendC2S(pktcon)
	sub3.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// client4 subscribe topic test. If retain message of topic test exists, client4 Will receive the message
	pktsub.ID = 6
	pktsub.Subscriptions = []mqtt.Subscription{{Topic: "test", QOS: 1}}
	sub3.sendC2S(pktsub)
	sub3.assertS2CPacket("<Suback ID=6 ReturnCodes=[1]>")
	b.assertSessionStore("sub3", "{\"id\":\"sub3\",\"subs\":{\"test\":1}}", nil)

	// the retain message only has the message of topic talks, so client4 Will not receive retain message of topic test
	msgs, err = b.ses.listRetainedMessages()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(msgs))
	assert.Equal(t, "talks", msgs[0].Context.Topic)
	assert.Equal(t, uint32(1), msgs[0].Context.QOS)
	assert.Equal(t, []byte("hi"), msgs[0].Content)
}

func TestSessionMqttDefaultMaxMessagePayload(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	defer b.closeAndClean()

	// connect packet
	pktcon := &mqtt.Connect{}
	pktcon.Version = 3
	pktcon.Will = nil

	// publish packet
	pktpub := &mqtt.Publish{}

	// pub client connect without Will message
	pktcon.ClientID = "pub"
	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(pktcon)
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	pktpub.ID = 0
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Retain = false

	// pub client publish message with payload length is 32768(32KB, the default max message payload)
	pktpub.Message.Payload = []byte(genRandomString(32768))
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=0>")

	// pub client publish message with payload length is larger than 32768(32KB, the default max message payload)
	pktpub.ID = 1
	pktpub.Message.Payload = []byte(genRandomString(32769)) // exceeds the max limit
	pub.sendC2S(pktpub)
	pub.assertS2CPacketTimeout()
	pub.assertClosed(true)

	// pub client connect with Will message
	pktwill := mqtt.NewPublish()
	pktwill.Message.Topic = "test"
	pktwill.Message.Retain = false
	pktcon.ClientID = "pub-with-will"

	// will message payload is 32768(32KB, the default max message payload)
	pktwill.Message.Payload = []byte(genRandomString(32768))
	pktcon.Will = &pktwill.Message
	pubWill := newMockConn(t)
	b.ses.Handle(pubWill)
	pubWill.sendC2S(pktcon)
	pubWill.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// will message payload is larger than 32768(32KB, the default max message payload)
	pktwill.Message.Payload = []byte(genRandomString(32769)) // exceeds the max limit
	pktcon.ClientID = "pub-with-will-overflow"
	pktcon.Will = &pktwill.Message
	pubWillOverFlow := newMockConn(t)
	b.ses.Handle(pubWillOverFlow)
	pubWillOverFlow.sendC2S(pktcon)
	pubWillOverFlow.assertS2CPacketTimeout()
	pubWillOverFlow.assertClosed(true)
}

func TestSessionMqttCustomizeMaxMessagePayload(t *testing.T) {
	b := newMockBroker(t, testConfDefault)
	b.ses.cfg.MaxMessagePayloadSize = utils.Size(256) // set the max message payload
	defer b.closeAndClean()

	// connect packet
	pktcon := &mqtt.Connect{}
	pktcon.Version = 3
	pktcon.Will = nil

	// publish packet
	pktpub := &mqtt.Publish{}

	// pub client connect without Will message
	pktcon.ClientID = "pub"
	pub := newMockConn(t)
	b.ses.Handle(pub)
	pub.sendC2S(pktcon)
	pub.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	pktpub.ID = 0
	pktpub.Message.QOS = 1
	pktpub.Message.Topic = "test"
	pktpub.Message.Retain = false

	// pub client publish message with payload length is 256(the configured max message payload)
	pktpub.Message.Payload = []byte(genRandomString(256))
	pub.sendC2S(pktpub)
	pub.assertS2CPacket("<Puback ID=0>")

	// pub client publish message with payload length is larger than 256(the configured max message payload)
	pktpub.ID = 1
	pktpub.Message.Payload = []byte(genRandomString(257)) // exceeds the max limit
	pub.sendC2S(pktpub)
	pub.assertS2CPacketTimeout()
	pub.assertClosed(true)

	// pub client connect with Will message
	pktwill := mqtt.NewPublish()
	pktwill.Message.Topic = "test"
	pktwill.Message.Retain = false
	pktcon.ClientID = "pub-with-will"

	// will message payload is 256(the configured max message payload)
	pktwill.Message.Payload = []byte(genRandomString(256))
	pktcon.Will = &pktwill.Message
	pubWill := newMockConn(t)
	b.ses.Handle(pubWill)
	pubWill.sendC2S(pktcon)
	pubWill.assertS2CPacket("<Connack SessionPresent=false ReturnCode=0>")

	// will message payload is larger than 256(the configured max message payload)
	pktwill.Message.Payload = []byte(genRandomString(257)) // exceeds the max limit
	pktcon.ClientID = "pub-with-will-overflow"
	pktcon.Will = &pktwill.Message
	pubWillOverFlow := newMockConn(t)
	b.ses.Handle(pubWillOverFlow)
	pubWillOverFlow.sendC2S(pktcon)
	pubWillOverFlow.assertS2CPacketTimeout()
	pubWillOverFlow.assertClosed(true)
}

func TestSessionMqttCheckClientID(t *testing.T) {
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
