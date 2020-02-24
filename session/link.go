package session

import (
	"context"
	"fmt"
	"strings"
	"sync"

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
	id      string
	mgr     *Manager
	session *Session
	stream  link.Link_TalkServer
	auth    *Authorizer
	log     *log.Logger
	mut     sync.Mutex
	once    sync.Once
	tomb    utils.Tomb
}

// Call handler of link
func (m *Manager) Call(ctx context.Context, msg *link.Message) (*link.Message, error) {
	// TODO: improvement, cache auth result
	if msg.Context.QOS > 1 {
		return nil, ErrSessionMessageQosNotSupported
	}
	if !m.checker.CheckTopic(msg.Context.Topic, false) {
		return nil, ErrSessionMessageTopicInvalid
	}
	if msg.Context.QOS == 0 {
		m.exch.Route(msg, nil)
		return nil, nil
	}
	done := make(chan struct{})
	m.exch.Route(msg, func(_ uint64) {
		close(done)
	})
	select {
	case <-done:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Talk handler of link
func (m *Manager) Talk(stream link.Link_TalkServer) error {
	defer m.log.Info("link client is closed")
	err := m.checkSessions()
	if err != nil {
		return err
	}
	c, err := m.newClientLink(stream)
	if err != nil {
		m.log.Error("failed to create link client", log.Error(err))
		return err
	}
	m.log.Debug("link client is created")
	return c.tomb.Wait()
}

func (m *Manager) newClientLink(stream link.Link_TalkServer) (*ClientLink, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok || len(md.Get("linkid")) == 0 {
		return nil, ErrSessionLinkIDNotSet
	}
	lid := md.Get("linkid")[0]
	if lid == "" {
		return nil, ErrSessionLinkIDNotSet
	}
	lid = "$link/" + lid
	id := strings.ReplaceAll(uuid.Generate().String(), "-", "")
	c := &ClientLink{
		id:     id,
		mgr:    m,
		stream: stream,
		log:    log.With(log.Any("type", "link"), log.Any("id", id)),
	}
	si := &Info{
		ID:            lid,
		Kind:          LINK,
		CleanSession:  false,                       // always false for link client
		Subscriptions: map[string]mqtt.QOS{lid: 1}, // always subscribe link's id, e.g. $link/<service_name>
	}
	_, err := m.initSession(si, c, false)
	if err != nil {
		return nil, err
	}
	c.tomb.Go(c.receiving)
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
func (c *ClientLink) close() error {
	c.once.Do(func() {
		c.log.Info("client is closing")
		defer c.log.Info("client has closed")
		c.tomb.Kill(nil)
		c.session.delClient(c)
	})
	return c.tomb.Wait()
}

// closes client by itself
func (c *ClientLink) die(msg string, err error) {
	if !c.tomb.Alive() {
		return
	}
	if err != nil {
		c.log.Error(msg, log.Error(err))
	}
	c.tomb.Kill(err)
	go c.close()
}

func (c *ClientLink) authorize(action, topic string) bool {
	return c.auth == nil || c.auth.Authorize(action, topic)
}

func (c *ClientLink) send(msg *link.Message) error {
	c.mut.Lock()
	err := c.stream.Send(msg)
	c.mut.Unlock()
	if err != nil {
		c.die("failed to send message", err)
		return err
	}

	if ent := c.log.Check(log.DebugLevel, "client sent a message"); ent != nil {
		ent.Write(log.Any("msg", fmt.Sprintf("%v", msg)))
	}

	return nil
}
func (c *ClientLink) sendEvent(e *eventWrapper, _ bool) (err error) {
	return c.send(e.Message)
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
			c.session.acknowledge(msg.Context.ID)
		default:
			err = ErrSessionMessageTypeInvalid
		}
		if err != nil {
			c.die("failed to handle message", err)
			return err
		}
	}
}

func (c *ClientLink) onMsg(msg *link.Message, cb func(uint64)) error {
	// TODO: improvement, cache auth result
	if msg.Context.QOS > 1 {
		return ErrSessionMessageQosNotSupported
	}
	if !c.mgr.checker.CheckTopic(msg.Context.Topic, false) {
		return ErrSessionMessageTopicInvalid
	}
	if !c.authorize(Publish, msg.Context.Topic) {
		return ErrSessionMessageTopicNotPermitted
	}
	if msg.Context.QOS == 0 {
		cb = nil
	}
	c.mgr.exch.Route(msg, cb)
	return nil
}

func (c *ClientLink) callback(id uint64) {
	msg := new(link.Message)
	msg.Context.ID = id
	msg.Context.Type = link.Ack
	c.send(msg)
}
