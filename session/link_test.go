package session

//
//import (
//	"path"
//	"testing"
//	"time"
//
//	"github.com/baetyl/baetyl-go/link"
//	"github.com/baetyl/baetyl-go/utils"
//	_ "github.com/mattn/go-sqlite3"
//	"github.com/stretchr/testify/assert"
//)
//
//func TestSessionLinkException(t *testing.T) {
//	t.Skip(t.Name())
//	b := newMockBroker(t, testConfDefault)
//	defer b.closeAndClean()
//
//	errs := make(chan error, 10)
//	c := newMockStream(t, "t")
//
//	// empty topic
//	go func() {
//		errs <- b.ses.Talk(c)
//	}()
//	m := &link.Message{}
//	c.sendC2S(m)
//	err := <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//
//	// invalid topic
//	go func() {
//		errs <- b.ses.Talk(c)
//	}()
//	m.Context.Topic = "$non-exist"
//	c.sendC2S(m)
//	err = <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//
//	// invalid message type
//	go func() {
//		errs <- b.ses.Talk(c)
//	}()
//	m.Context.Type = 111
//	c.sendC2S(m)
//	err = <-errs
//	assert.EqualError(t, err, "message type is invalid")
//
//	// invalid message qos
//	go func() {
//		errs <- b.ses.Talk(c)
//	}()
//	m.Context.Type = 0
//	m.Context.QOS = 111
//	m.Context.Topic = "a"
//	c.sendC2S(m)
//	err = <-errs
//	assert.EqualError(t, err, "message QOS is not supported")
//}
//
//func TestSessionLinkAllStates(t *testing.T) {
//	t.Skip(t.Name())
//	b := newMockBroker(t, testConfDefault)
//
//	lid := "$link/c"
//	errs := make(chan error, 10)
//	queuePath := path.Join(b.ses.cfg.DB.Source, "queue", utils.CalculateBase64(lid))
//
//	// c1 connects
//	c1 := newMockStream(t, "c")
//	go func() {
//		errs <- b.ses.Talk(c1)
//		c1.Close()
//	}()
//
//	b.waitClientReady(lid, 1)
//	b.assertSessionState(lid, STATE1)
//	b.assertSessionStore(lid, "{\"id\":\""+lid+"\",\"kind\":\"link\",\"subs\":{\""+lid+"\":1}}")
//	b.assertExchangeCount(1)
//	assert.True(t, utils.FileExists(queuePath))
//
//	// send invalid topic to disconnect
//	c1.sendC2S(&link.Message{})
//	err := <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//
//	b.waitClientReady(lid, 0)
//	b.assertSessionCount(1)
//	b.assertSessionState(lid, STATE2)
//	b.assertSessionStore(lid, "{\"id\":\""+lid+"\",\"kind\":\"link\",\"subs\":{\""+lid+"\":1}}")
//	b.assertExchangeCount(1)
//	assert.True(t, utils.FileExists(queuePath))
//
//	// broker closes unexpected, session queue data is already stored
//	b.close()
//	assert.True(t, utils.FileExists(queuePath))
//
//	// broker restarts, persisted session will be started in state2
//	b = newMockBroker(t, testConfDefault)
//	defer b.closeAndClean()
//	b.assertSessionState(lid, STATE2)
//	b.assertSessionStore(lid, "{\"id\":\""+lid+"\",\"kind\":\"link\",\"subs\":{\""+lid+"\":1}}")
//	b.assertExchangeCount(1)
//	assert.True(t, utils.FileExists(queuePath))
//
//	// c2 connects again
//	c2 := newMockStream(t, "c")
//	go func() {
//		errs <- b.ses.Talk(c2)
//		c2.Close()
//	}()
//
//	b.waitClientReady(lid, 1)
//	b.assertSessionState(lid, STATE1)
//	b.assertSessionStore(lid, "{\"id\":\""+lid+"\",\"kind\":\"link\",\"subs\":{\""+lid+"\":1}}")
//	b.assertExchangeCount(1)
//	assert.True(t, utils.FileExists(queuePath))
//}
//
//func TestSessionLinkMaxSessionsAndClients(t *testing.T) {
//	t.Skip(t.Name())
//	b := newMockBroker(t, testConfSession)
//	defer b.closeAndClean()
//
//	errs := make(chan error, 10)
//	// c1 connect
//	c1 := newMockStream(t, "c1")
//	go func() {
//		errs <- b.ses.Talk(c1)
//		c1.Close()
//	}()
//	// c2 connect
//	c2 := newMockStream(t, "c2")
//	go func() {
//		errs <- b.ses.Talk(c2)
//		c2.Close()
//	}()
//	// c3 connect
//	c3 := newMockStream(t, "c3")
//	go func() {
//		errs <- b.ses.Talk(c3)
//		c3.Close()
//	}()
//
//	// c4 connect
//	c4 := newMockStream(t, "c4")
//	go func() {
//		errs <- b.ses.Talk(c4)
//		c4.Close()
//	}()
//
//	err := <-errs
//	assert.EqualError(t, err, "exceeds limit")
//	lid := assertClientClosed(1, c1, c2, c3, c4)
//	b.assertSessionCount(3)
//
//	// c5 connect
//	c5 := newMockStream(t, lid)
//	go func() {
//		errs <- b.ses.Talk(c5)
//		c5.Close()
//	}()
//
//	b.waitClientReady("$link/"+lid, 2)
//	c5.assertClosed(false)
//	b.assertSessionCount(3)
//
//	// c6 connect
//	c6 := newMockStream(t, lid)
//	go func() {
//		errs <- b.ses.Talk(c6)
//		c6.Close()
//	}()
//
//	err = <-errs
//	assert.EqualError(t, err, "exceeds limit")
//	c6.assertClosed(true)
//}
//
//func TestSessionLinkSendRecvBL(t *testing.T) {
//	t.Skip(t.Name())
//	b := newMockBroker(t, testConfDefault)
//	b.ses.cfg.ResendInterval = time.Millisecond * 1000
//	defer b.closeAndClean()
//
//	errs := make(chan error, 10)
//	publid, sublid := "$link/pubc", "$link/subc"
//
//	// pubc connect
//	pubc := newMockStream(t, "pubc")
//	go func() {
//		errs <- b.ses.Talk(pubc)
//		pubc.Close()
//	}()
//	// subc1 connect with the same link id
//	subc1 := newMockStream(t, "subc")
//	go func() {
//		errs <- b.ses.Talk(subc1)
//		subc1.Close()
//	}()
//	// subc2 connect with the same link id
//	subc2 := newMockStream(t, "subc")
//	go func() {
//		errs <- b.ses.Talk(subc2)
//		subc2.Close()
//	}()
//
//	b.waitClientReady(publid, 1)
//	b.waitClientReady(sublid, 2)
//	b.assertSessionCount(2)
//	b.assertSessionState(publid, STATE1)
//	b.assertSessionState(sublid, STATE1)
//	b.assertSessionStore(publid, "{\"id\":\"$link/pubc\",\"kind\":\"link\",\"subs\":{\"$link/pubc\":1}}")
//	b.assertSessionStore(sublid, "{\"id\":\"$link/subc\",\"kind\":\"link\",\"subs\":{\"$link/subc\":1}}")
//	b.assertExchangeCount(2)
//
//	// pubc send a message with qos 0 to subc
//	m := &link.Message{}
//	m.Context.Topic = sublid
//	m.Content = []byte("111")
//	pubc.sendC2S(m)
//	assertS2CMessageLB(subc1, subc2, "Context:<Topic:\"$link/subc\" > Content:\"111\" ")
//
//	// pubc send more messages with qos 0 to subc
//	m = &link.Message{}
//	m.Context.Topic = sublid
//	m.Content = []byte("222")
//	count := 100
//	go func() {
//		for index := 0; index < count; index++ {
//			pubc.sendC2S(m)
//		}
//	}()
//	go func() {
//		for index := 0; index < count; index++ {
//			pubc.sendC2S(m)
//		}
//	}()
//	go func() {
//		for index := 0; index < count; index++ {
//			pubc.sendC2S(m)
//		}
//	}()
//	for index := 0; index < count*3; index++ {
//		assertS2CMessageLB(subc1, subc2, "Context:<Topic:\"$link/subc\" > Content:\"222\" ")
//	}
//	subc1.assertS2CMessageTimeout()
//	subc2.assertS2CMessageTimeout()
//
//	// pubc send a message with qos 1 to subc
//	m = &link.Message{}
//	m.Context.ID = 1
//	m.Context.QOS = 1
//	m.Context.Topic = sublid
//	m.Content = []byte("333")
//	pubc.sendC2S(m)
//	pubc.assertS2CMessage("Context:<ID:1 Type:Ack > ")
//	subc := assertS2CMessageLB(subc1, subc2, "Context:<ID:1 QOS:1 Topic:\"$link/subc\" > Content:\"333\" ")
//	ack := &link.Message{}
//	ack.Context.ID = 1
//	ack.Context.Type = link.Ack
//	subc.sendC2S(ack)
//	subc1.assertS2CMessageTimeout()
//	subc2.assertS2CMessageTimeout()
//
//	// close subc1 by sending a invalid message
//	subc1.sendC2S(&link.Message{})
//	err := <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//	b.waitClientReady(publid, 1)
//	b.waitClientReady(sublid, 1)
//	b.assertSessionCount(2)
//	b.assertSessionState(publid, STATE1)
//	b.assertSessionState(sublid, STATE1)
//	b.assertSessionStore(publid, "{\"id\":\"$link/pubc\",\"kind\":\"link\",\"subs\":{\"$link/pubc\":1}}")
//	b.assertSessionStore(sublid, "{\"id\":\"$link/subc\",\"kind\":\"link\",\"subs\":{\"$link/subc\":1}}")
//	b.assertExchangeCount(2)
//
//	m = &link.Message{}
//	m.Context.Topic = sublid
//	m.Content = []byte(">444<")
//	pubc.sendC2S(m)
//	subc2.assertS2CMessage("Context:<Topic:\"$link/subc\" > Content:\">444<\" ")
//	subc1.assertS2CMessageTimeout()
//
//	m = &link.Message{}
//	m.Context.ID = 2
//	m.Context.QOS = 1
//	m.Context.Topic = sublid
//	m.Content = []byte(">555<")
//	pubc.sendC2S(m)
//	pubc.assertS2CMessage("Context:<ID:2 Type:Ack > ")
//	subc2.assertS2CMessage("Context:<ID:2 QOS:1 Topic:\"$link/subc\" > Content:\">555<\" ")
//	ack = &link.Message{}
//	ack.Context.ID = 2
//	ack.Context.Type = link.Ack
//	subc2.sendC2S(ack)
//	subc1.assertS2CMessageTimeout()
//
//	//close subc2 by sending a invalid message
//	subc2.sendC2S(&link.Message{})
//	err = <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//	b.waitClientReady(publid, 1)
//	b.waitClientReady(sublid, 0)
//	b.assertSessionCount(2)
//	b.assertSessionState(publid, STATE1)
//	b.assertSessionState(sublid, STATE2)
//	b.assertSessionStore(publid, "{\"id\":\"$link/pubc\",\"kind\":\"link\",\"subs\":{\"$link/pubc\":1}}")
//	b.assertSessionStore(sublid, "{\"id\":\"$link/subc\",\"kind\":\"link\",\"subs\":{\"$link/subc\":1}}")
//	b.assertExchangeCount(2)
//
//	m = &link.Message{}
//	m.Context.ID = 3
//	m.Context.QOS = 1
//	m.Context.Topic = sublid
//	m.Content = []byte(">666<")
//	pubc.sendC2S(m)
//	pubc.assertS2CMessage("Context:<ID:3 Type:Ack > ")
//	subc1.assertS2CMessageTimeout()
//	subc2.assertS2CMessageTimeout()
//
//	subc3 := newMockStream(t, "subc")
//	go func() {
//		errs <- b.ses.Talk(subc3)
//		subc3.Close()
//	}()
//	subc3.assertS2CMessage("Context:<ID:3 QOS:1 Topic:\"$link/subc\" > Content:\">666<\" ")
//	ack = &link.Message{}
//	ack.Context.ID = 3
//	ack.Context.Type = link.Ack
//	subc3.sendC2S(ack)
//
//	subc3.sendC2S(&link.Message{})
//	err = <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//	b.waitClientReady(publid, 1)
//	b.waitClientReady(sublid, 0)
//	b.assertSessionCount(2)
//	b.assertSessionState(publid, STATE1)
//	b.assertSessionState(sublid, STATE2)
//	b.assertSessionStore(publid, "{\"id\":\"$link/pubc\",\"kind\":\"link\",\"subs\":{\"$link/pubc\":1}}")
//	b.assertSessionStore(sublid, "{\"id\":\"$link/subc\",\"kind\":\"link\",\"subs\":{\"$link/subc\":1}}")
//	b.assertExchangeCount(2)
//
//	pubc.sendC2S(&link.Message{})
//	err = <-errs
//	assert.EqualError(t, err, "message topic is invalid")
//	b.waitClientReady(publid, 0)
//	b.waitClientReady(sublid, 0)
//	b.assertSessionCount(2)
//	b.assertSessionState(publid, STATE2)
//	b.assertSessionState(sublid, STATE2)
//	b.assertSessionStore(publid, "{\"id\":\"$link/pubc\",\"kind\":\"link\",\"subs\":{\"$link/pubc\":1}}")
//	b.assertSessionStore(sublid, "{\"id\":\"$link/subc\",\"kind\":\"link\",\"subs\":{\"$link/subc\":1}}")
//	b.assertExchangeCount(2)
//}
