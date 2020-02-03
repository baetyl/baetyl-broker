package session

import (
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/link"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestSessionLinkException(t *testing.T) {
	b, err := newMockBroker(t, testConfDefault)
	assert.NoError(t, err)
	assert.NotNil(t, b)
	defer b.close()

	errs := make(chan error, 10)
	c := newMockStream(t, "t")

	// empty topic
	go func() {
		errs <- b.manager.Talk(c)
	}()
	m := &link.Message{}
	c.sendC2S(m)
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")

	// invalid topic
	go func() {
		errs <- b.manager.Talk(c)
	}()
	m.Context.Topic = "$non-exist"
	c.sendC2S(m)
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")

	// invalid message type
	go func() {
		errs <- b.manager.Talk(c)
	}()
	m.Context.Type = 111
	c.sendC2S(m)
	err = <-errs
	assert.EqualError(t, err, "message type is invalid")

	// invalid message qos
	go func() {
		errs <- b.manager.Talk(c)
	}()
	m.Context.Type = 0
	m.Context.QOS = 111
	m.Context.Topic = "a"
	c.sendC2S(m)
	err = <-errs
	assert.EqualError(t, err, "message QOS is not supported")
}

func TestSessionLinkSendRecvBL(t *testing.T) {
	b, err := newMockBroker(t, testConfDefault)
	assert.NoError(t, err)
	assert.NotNil(t, b)
	b.manager.cfg.ResendInterval = time.Millisecond * 1000
	defer b.close()

	errs := make(chan error, 10)
	publid, sublid := "$link/pubc", "$link/subc"

	// pubc connect
	pubc := newMockStream(t, "pubc")
	go func() {
		errs <- b.manager.Talk(pubc)
		pubc.Close()
	}()
	// subc1 connect with the same link id
	subc1 := newMockStream(t, "subc")
	go func() {
		errs <- b.manager.Talk(subc1)
		subc1.Close()
	}()
	// subc2 connect with the same link id
	subc2 := newMockStream(t, "subc")
	go func() {
		errs <- b.manager.Talk(subc2)
		subc2.Close()
	}()

	b.waitClientReady(3)
	b.assertClientCount(3)
	b.assertSessionCount(2)
	b.waitBindingReady(publid, 1)
	b.assertBindingCount(publid, 1)
	b.waitBindingReady(sublid, 2)
	b.assertBindingCount(sublid, 2)
	b.assertExchangeCount(2)

	// pubc send a message with qos 0 to subc
	m := &link.Message{}
	m.Context.Topic = sublid
	m.Content = []byte("111")
	pubc.sendC2S(m)
	assertS2CMessageLB(subc1, subc2, "Context:<Topic:\"$link/subc\" > Content:\"111\" ")
	b.assertSession(publid, "{\"ID\":\"$link/pubc\",\"CleanSession\":false,\"Subscriptions\":{\"$link/pubc\":1}}")
	b.assertSession(sublid, "{\"ID\":\"$link/subc\",\"CleanSession\":false,\"Subscriptions\":{\"$link/subc\":1}}")

	// pubc send more messages with qos 0 to subc
	m = &link.Message{}
	m.Context.Topic = sublid
	m.Content = []byte("222")
	count := 100
	go func() {
		for index := 0; index < count; index++ {
			pubc.sendC2S(m)
		}
	}()
	go func() {
		for index := 0; index < count; index++ {
			pubc.sendC2S(m)
		}
	}()
	go func() {
		for index := 0; index < count; index++ {
			pubc.sendC2S(m)
		}
	}()
	for index := 0; index < count*3; index++ {
		assertS2CMessageLB(subc1, subc2, "Context:<Topic:\"$link/subc\" > Content:\"222\" ")
	}
	subc1.assertS2CMessageTimeout()
	subc2.assertS2CMessageTimeout()

	// pubc send a message with qos 1 to subc
	m = &link.Message{}
	m.Context.ID = 1
	m.Context.QOS = 1
	m.Context.Topic = sublid
	m.Content = []byte("333")
	pubc.sendC2S(m)
	pubc.assertS2CMessage("Context:<ID:1 Type:Ack > ")
	subc := assertS2CMessageLB(subc1, subc2, "Context:<ID:1 QOS:1 Topic:\"$link/subc\" > Content:\"333\" ")
	ack := &link.Message{}
	ack.Context.ID = 1
	ack.Context.Type = link.Ack
	subc.sendC2S(ack)
	subc1.assertS2CMessageTimeout()
	subc2.assertS2CMessageTimeout()

	// close subc1 by sending a invalid message
	subc1.sendC2S(&link.Message{})
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")
	b.waitClientReady(2)
	b.assertClientCount(2)
	b.assertSessionCount(2)
	b.waitBindingReady(publid, 1)
	b.assertBindingCount(publid, 1)
	b.waitBindingReady(sublid, 1)
	b.assertBindingCount(sublid, 1)
	b.assertExchangeCount(2)

	m = &link.Message{}
	m.Context.Topic = sublid
	m.Content = []byte(">444<")
	pubc.sendC2S(m)
	subc2.assertS2CMessage("Context:<Topic:\"$link/subc\" > Content:\">444<\" ")
	subc1.assertS2CMessageTimeout()

	m = &link.Message{}
	m.Context.ID = 2
	m.Context.QOS = 1
	m.Context.Topic = sublid
	m.Content = []byte(">555<")
	pubc.sendC2S(m)
	pubc.assertS2CMessage("Context:<ID:2 Type:Ack > ")
	subc2.assertS2CMessage("Context:<ID:2 QOS:1 Topic:\"$link/subc\" > Content:\">555<\" ")
	ack = &link.Message{}
	ack.Context.ID = 2
	ack.Context.Type = link.Ack
	subc2.sendC2S(ack)
	subc1.assertS2CMessageTimeout()

	//close subc2 by sending a invalid message
	subc2.sendC2S(&link.Message{})
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")
	b.waitClientReady(1)
	b.assertClientCount(1)
	b.assertSessionCount(2)
	b.waitBindingReady(publid, 1)
	b.assertBindingCount(publid, 1)
	b.waitBindingReady(sublid, 0)
	b.assertBindingCount(sublid, 0)
	b.assertExchangeCount(2)

	m = &link.Message{}
	m.Context.ID = 3
	m.Context.QOS = 1
	m.Context.Topic = sublid
	m.Content = []byte(">666<")
	pubc.sendC2S(m)
	pubc.assertS2CMessage("Context:<ID:3 Type:Ack > ")
	subc1.assertS2CMessageTimeout()
	subc2.assertS2CMessageTimeout()

	subc3 := newMockStream(t, "subc")
	go func() {
		errs <- b.manager.Talk(subc3)
		subc3.Close()
	}()
	subc3.assertS2CMessage("Context:<ID:3 QOS:1 Topic:\"$link/subc\" > Content:\">666<\" ")
	ack = &link.Message{}
	ack.Context.ID = 3
	ack.Context.Type = link.Ack
	subc3.sendC2S(ack)

	subc3.sendC2S(&link.Message{})
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")
	b.waitClientReady(1)
	b.assertClientCount(1)
	b.assertSessionCount(2)
	b.waitBindingReady(publid, 1)
	b.assertBindingCount(publid, 1)
	b.waitBindingReady(sublid, 0)
	b.assertBindingCount(sublid, 0)
	b.assertExchangeCount(2)

	pubc.sendC2S(&link.Message{})
	err = <-errs
	assert.EqualError(t, err, "message topic is invalid")
	b.waitClientReady(0)
	b.assertClientCount(0)
	b.assertSessionCount(2)
	b.waitBindingReady(publid, 0)
	b.assertBindingCount(publid, 0)
	b.waitBindingReady(sublid, 0)
	b.assertBindingCount(sublid, 0)
	b.assertExchangeCount(2)
}
