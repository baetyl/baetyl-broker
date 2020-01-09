package session

import (
	"fmt"
	"strings"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/docker/distribution/uuid"
	"google.golang.org/grpc/metadata"
)

// TODO: link session should be removed when it is expired because link client has no method to clean it's session

// ClientLink the client of Link
type ClientLink struct {
	id         string
	manager    *Manager
	session    *Session
	resender   *resender
	authorizer *Authorizer
	stream     link.Link_TalkServer
	log        *log.Logger
	tomb       utils.Tomb
	sync.Mutex
}

func (m *Manager) initClientLink(stream link.Link_TalkServer) (*ClientLink, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	m.log.Debug("to create link client with metatdata", log.Any("metadata", md))
	if !ok || len(md.Get("linkid")) == 0 {
		return nil, ErrSessionLinkIDNotSet
	}
	lid := md.Get("linkid")[0]
	if lid == "" {
		return nil, ErrSessionLinkIDNotSet
	}
	id := strings.ReplaceAll(uuid.Generate().String(), "-", "")
	c := &ClientLink{
		id:      id,
		manager: m,
		stream:  stream,
		log:     log.With(log.Any("type", "link"), log.Any("id", id)),
	}
	c.resender = newResender(m.cfg.MaxInflightQOS1Messages, m.cfg.RepublishInterval, &c.tomb)
	m.addClient(c)
	si := &Info{
		ID:            lid,
		Subscriptions: map[string]mqtt.QOS{lid: 1}, // always subscribe link's id, e.g. $link/<service_name>
	}
	_, err := m.initSession(si, c, false)
	if err != nil {
		return nil, err
	}
	c.tomb.Go(c.resending, c.receiving)
	return c, nil
}

func (c *ClientLink) getID() string {
	return c.id
}

func (c *ClientLink) setSession(s *Session) {
	c.session = s
	c.log = c.log.With(log.Any("sid", s.ID))
}

func (c *ClientLink) getSession() *Session {
	return c.session
}

// Close closes client by session
func (c *ClientLink) Close() error {
	c.log.Info("client is closing by session")
	defer c.log.Info("client has closed by session")
	return c.close(nil)
}

// closes client by itself
func (c *ClientLink) die(msg string, err error) {
	if !c.tomb.Alive() {
		return
	}
	if err != nil {
		c.log.Error(msg, log.Error(err))
	}
	go func() {
		c.log.Info("client is closing by itself")
		if err != nil {
			c.log.Error(msg, log.Error(err))
		}
		c.manager.delClient(c)
		c.close(err)
		c.log.Info("client has closed by itself")
	}()
}

func (c *ClientLink) close(err error) error {
	c.tomb.Kill(err)
	return c.tomb.Wait()
}

func (c *ClientLink) authorize(action, topic string) bool {
	return c.authorizer == nil || c.authorizer.Authorize(action, topic)
}

func (c *ClientLink) send(msg *link.Message) error {
	c.Lock()
	err := c.stream.Send(msg)
	c.Unlock()
	if err != nil {
		c.die("failed to send message", err)
		return err
	}

	if ent := c.log.Check(log.DebugLevel, "client sent a message"); ent != nil {
		ent.Write(log.Any("msg", fmt.Sprintf("%v", msg)))
	}

	return nil
}

func (c *ClientLink) sending() error {
	c.log.Info("client starts to send messages")
	defer c.log.Info("client has stopped sending messages")

	var err error
	var evt *common.Event
	qos0 := c.session.qos0.Chan()
	qos1 := c.session.qos1.Chan()
	for {
		select {
		case evt = <-qos0:
			if err = c.send(evt.Message); err != nil {
				return err
			}
		case evt = <-qos1:
			_iqel := newIQEL(evt.Context.ID, 1, evt)
			if err = c.resender.store(_iqel); err != nil {
				c.log.Error(err.Error())
			}
			if err = c.send(_iqel.message()); err != nil {
				return err
			}
			select {
			case c.resender.c <- _iqel:
				return nil
			case <-c.tomb.Dying():
				return ErrSessionClientAlreadyClosed
			}
		case <-c.tomb.Dying():
			return nil
		}
	}
}

func (c *ClientLink) resending() error {
	c.log.Info("client starts to resend messages", log.Any("interval", c.resender.d))
	defer c.log.Info("client has stopped resending messages")
	return c.resender.resending(func(i *iqel) error {
		return c.send(i.message())
	})
}

func (c *ClientLink) receiving() error {
	c.log.Info("client starts to receive messages")
	defer c.log.Info("client has stopped receiving messages")

	var err error
	var msg *link.Message
	for {
		msg, err = c.stream.Recv()
		if err != nil {
			c.die("failed to receive message", err)
			return err
		}

		if ent := c.log.Check(log.DebugLevel, "client received a message"); ent != nil {
			ent.Write(log.Any("msg", fmt.Sprintf("%v", msg)))
		}

		switch msg.Context.Type {
		case link.Msg, link.MsgRtn:
			err = c.onMsg(msg, c.callback)
		case link.Ack:
			err = c.onAck(msg)
		default:
			err = ErrSessionMessageTypeInvalid
		}
		if err != nil {
			c.die("failed to handle message", err)
			return err
		}
	}
}

func (c *ClientLink) onAck(msg *link.Message) error {
	c.resender.delete(msg.Context.ID)
	return nil
}

func (c *ClientLink) onMsg(msg *link.Message, cb func(uint64)) error {
	// TODO: improvement, cache auth result
	if msg.Context.QOS > 1 {
		return ErrSessionMessageQosNotSupported
	}
	if !c.manager.checker.CheckTopic(msg.Context.Topic, false) {
		return ErrSessionMessageTopicInvalid
	}
	if !c.authorize(Publish, msg.Context.Topic) {
		return ErrSessionMessageTopicNotPermitted
	}
	if msg.Context.QOS == 0 {
		cb = nil
	}
	c.manager.exchange.Route(msg, cb)
	return nil
}

func (c *ClientLink) callback(id uint64) {
	msg := new(link.Message)
	msg.Context.ID = id
	msg.Context.Type = link.Ack
	c.send(msg)
}
