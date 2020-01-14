package session

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"sync"

	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/retain"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors
var (
	ErrConnectionExceeds                   = errors.New("number of connections exceeds the max limit")
	ErrSessionClientAlreadyClosed          = errors.New("session client is already closed")
	ErrSessionClientAlreadyConnecting      = errors.New("session client is already connecting")
	ErrSessionClientPacketUnexpected       = errors.New("session client received unexpected packet")
	ErrSessionClientPacketIDConflict       = errors.New("packet id conflict, to acknowledge old packet")
	ErrSessionClientPacketNotFound         = errors.New("packet id is not found")
	ErrSessionProtocolVersionInvalid       = errors.New("protocol version is invalid")
	ErrSessionClientIDInvalid              = errors.New("client ID is invalid")
	ErrSessionUsernameNotSet               = errors.New("username is not set")
	ErrSessionPasswordNotSet               = errors.New("password is not set")
	ErrSessionUsernameNotPermitted         = errors.New("username or password is not permitted")
	ErrSessionCommonNameNotFound           = errors.New("commonName not found")
	ErrSessionCommonNameNotPermitted       = errors.New("commonName not permitted")
	ErrSessionMessageQosNotSupported       = errors.New("message QOS is not supported")
	ErrSessionMessageTopicInvalid          = errors.New("message topic is invalid")
	ErrSessionMessageTopicNotPermitted     = errors.New("message topic is not permitted")
	ErrSessionMessageTypeInvalid           = errors.New("message type is invalid")
	ErrSessionWillMessageQosNotSupported   = errors.New("will QoS is not supported")
	ErrSessionWillMessageTopicInvalid      = errors.New("will topic is invalid")
	ErrSessionWillMessageTopicNotPermitted = errors.New("will topic is not permitted")
	ErrSessionLinkIDNotSet                 = errors.New("link id is not set")
)

type client interface {
	getID() string
	getSession() *Session
	setSession(*Session)
	io.Closer
}

// Manager the manager of sessions
type Manager struct {
	cfg           Config
	sessiondb     *Backend
	retaindb      *retain.Backend
	exchange      *exchange.Exchange
	checker       *mqtt.TopicChecker
	authenticator *Authenticator
	sessions      map[string]*Session
	clients       map[string]client            // TODO: limit the number of clients
	bindings      map[string]map[string]client // map[sid]map[cid]client
	log           *log.Logger
	sync.Mutex
}

// NewManager create a new session manager
func NewManager(cfg Config) (*Manager, error) {
	sessiondb, err := NewBackend(cfg)
	if err != nil {
		return nil, err
	}
	// load stored sessions from backend database
	items, err := sessiondb.List()
	if err != nil {
		sessiondb.Close()
		return nil, err
	}
	retaindb, err := sessiondb.NewRetainBackend()
	if err != nil {
		sessiondb.Close()
		return nil, err
	}
	manager := &Manager{
		cfg:           cfg,
		sessiondb:     sessiondb,
		retaindb:      retaindb,
		authenticator: NewAuthenticator(cfg.Principals),
		checker:       mqtt.NewTopicChecker(cfg.SysTopics),
		exchange:      exchange.NewExchange(cfg.SysTopics),
		sessions:      map[string]*Session{},
		clients:       map[string]client{},
		bindings:      map[string]map[string]client{},
		log:           log.With(log.Any("session", "manager")),
	}
	for _, i := range items {
		si := i.(*Info)
		s, err := manager.newSession(si)
		if err != nil {
			manager.Close()
			return nil, err
		}
		manager.sessions[si.ID] = s
		for topic, qos := range si.Subscriptions {
			s.subs.Set(topic, qos)
			manager.exchange.Bind(topic, s)
			// TODO: it is unnecessary to subscribe messages with qos 0 for the session without any client
		}
	}
	manager.log.Info("session manager has initialized")
	return manager, nil
}

// Close close
func (m *Manager) Close() error {
	m.log.Info("session manager is closing")
	defer m.log.Info("session manager has closed")

	m.Lock()
	defer m.Unlock()

	for _, c := range m.clients {
		c.Close()
	}

	for _, s := range m.sessions {
		s.Close()
	}

	m.retaindb.Close()
	m.sessiondb.Close()
	return nil
}

// * connection handlers

// ClientMQTTHandler the connection handler to create a new MQTT client
func (m *Manager) ClientMQTTHandler(conn mqtt.Connection) {
	err := m.checkConnection()
	if err != nil {
		conn.Close()
		return
	}
	m.initClientMQTT(conn)
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
		m.exchange.Route(msg, nil)
		return nil, nil
	}
	done := make(chan struct{})
	m.exchange.Route(msg, func(_ uint64) {
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
	err := m.checkConnection()
	if err != nil {
		return err
	}
	c, err := m.initClientLink(stream)
	if err != nil {
		m.log.Error("failed to create link client", log.Error(err))
		return err
	}
	err = c.sending()
	if err != nil {
		m.log.Debug("failed to send message", log.Error(err))
	}
	return c.Close()
}

// * init session for client

// initSession init session for client, creates new session if not exists
func (m *Manager) initSession(si *Info, c client, unique bool) (exists bool, err error) {
	m.Lock()
	defer m.Unlock()

	var s *Session
	sid, cid := si.ID, c.getID()
	if s, exists = m.sessions[sid]; exists {
		if unique {
			// close previous clients with the same session id
			for _, prev := range m.bindings[sid] {
				prev.Close()
				delete(m.bindings[sid], prev.getID())
				delete(m.clients, prev.getID())
			}
			if len(m.bindings[sid]) != 0 {
				panic("all previous clients should be deleted")
			}
		} else {
			m.log.Info("add new client to existing session", log.Any("session", sid), log.Any("cid", cid))
		}
	} else {
		s, err = m.newSession(si)
		if err != nil {
			return
		}
		m.sessions[sid] = s
	}

	// binding client and session
	if _, ok := m.bindings[sid]; !ok {
		m.bindings[sid] = map[string]client{}
	}
	m.bindings[sid][cid] = c
	// set session for client
	c.setSession(s)
	// update session
	s.Info.CleanSession = si.CleanSession
	if s.Info.CleanSession {
		m.sessiondb.Del(sid)
		s.log.Info("session is removed from backend")
	} else {
		m.sessiondb.Set(&s.Info)
		s.log.Info("session is stored to backend")
	}

	// TODO: if no topic subscribed, the session does not need to create queues
	for topic, qos := range s.Subscriptions {
		s.subs.Set(topic, qos)
		m.exchange.Bind(topic, s)
	}
	return
}

func (m *Manager) newSession(si *Info) (*Session, error) {
	sid := si.ID
	cfg := m.cfg.Persistence
	cfg.Name = base64.StdEncoding.EncodeToString([]byte(sid))
	cfg.BatchSize = m.cfg.MaxInflightQOS1Messages
	queuedb, err := m.sessiondb.NewQueueBackend(cfg)
	if err != nil {
		m.log.Error("failed to create session", log.Any("session", sid), log.Error(err))
		return nil, err
	}
	defer m.log.Info("session is created", log.Any("session", sid))
	return &Session{
		Info: *si,
		subs: mqtt.NewTrie(),
		qos0: queue.NewTemporary(sid, m.cfg.MaxInflightQOS0Messages, true),
		qos1: queue.NewPersistence(cfg, queuedb),
		log:  m.log.With(log.Any("id", sid)),
	}, nil
}

// * subscription

// Subscribe subscribes topics
func (m *Manager) subscribe(c client, subs []mqtt.Subscription) error {
	m.Lock()
	defer m.Unlock()

	s := c.getSession()
	if s == nil {
		panic("session should be prepared before client subscribes")
	}
	if s.Subscriptions == nil {
		s.Subscriptions = map[string]mqtt.QOS{}
	}
	for _, sub := range subs {
		s.Subscriptions[sub.Topic] = sub.QOS
		s.subs.Set(sub.Topic, sub.QOS)
		m.exchange.Bind(sub.Topic, s)
	}
	if !s.CleanSession {
		return m.sessiondb.Set(&s.Info)
	}
	return nil
}

// Unsubscribe unsubscribes topics
func (m *Manager) unsubscribe(c client, topics []string) error {
	m.Lock()
	defer m.Unlock()

	s := c.getSession()
	if s == nil {
		panic("session should be prepared before client unsubscribes")
	}
	for _, t := range topics {
		delete(s.Subscriptions, t)
		s.subs.Empty(t)
		m.exchange.Unbind(t, s)
	}
	if !s.CleanSession {
		return m.sessiondb.Set(&s.Info)
	}
	return nil
}

func (m *Manager) checkConnection() error {
	m.Lock()
	num := len(m.clients)
	m.Unlock()
	if m.cfg.MaxConnections > 0 && num >= m.cfg.MaxConnections {
		m.log.Error(ErrConnectionExceeds.Error(), log.Any("max", m.cfg.MaxConnections))
		return ErrConnectionExceeds
	}
	return nil
}

//  * client operations

func (m *Manager) addClient(c client) {
	m.Lock()
	m.clients[c.getID()] = c
	m.Unlock()
}

// delClient deletes client and clean session if needs
func (m *Manager) delClient(c client) {
	m.Lock()
	defer m.Unlock()

	// remove client
	cid := c.getID()
	delete(m.clients, cid)

	// remove session if needs
	s := c.getSession()
	if s == nil {
		return
	}
	sid := s.ID
	delete(m.bindings[sid], cid)
	// close session if not bound and CleanSession=true
	if s.CleanSession && len(m.bindings[sid]) == 0 {
		m.sessiondb.Del(sid)
		m.exchange.UnbindAll(s)
		s.Close()
		delete(m.sessions, sid)
		delete(m.bindings, sid)
		// TODO: to delete persistent queue data if CleanSession=true
	}
}

// * session operations

// SetRetain sets retain message
func (m *Manager) setRetain(topic string, msg *link.Message) error {
	retain := &retain.Retain{Topic: msg.Context.Topic, Message: msg}
	err := m.retaindb.Set(retain)
	if err != nil {
		return err
	}
	return nil
}

// GetRetain gets retain messages
func (m *Manager) getRetain() ([]*link.Message, error) {
	retains, err := m.retaindb.List()
	if err != nil {
		return nil, err
	}
	msgs := make([]*link.Message, 0)
	for _, v := range retains {
		msg := v.(*retain.Retain).Message
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// RemoveRetain removes retain message
func (m *Manager) removeRetain(topic string) error {
	err := m.retaindb.Del(topic)
	if err != nil {
		return err
	}
	return nil
}
