package session

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/retain"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors
var (
	ErrMessageInvalid    = errors.New("Only message with topic can be routed")
	ErrConnectionExceeds = errors.New("The number of connections exceeds the max limit")
)

type client interface {
	getID() string
	getSession() *Session
	setSession(*Session)
	io.Closer
}

// Config session cfg
type Config struct {
	PersistenceDriver       string        `yaml:"persistenceDriver" json:"persistenceDriver" default:"sqlite3"`
	PersistenceLocation     string        `yaml:"persistenceLocation" json:"persistenceLocation" default:"var/lib/baetyl"`
	MaxInflightQOS0Messages int           `yaml:"maxInflightQOS0Messages" json:"maxInflightQOS0Messages" default:"100" validate:"min=1"`
	MaxInflightQOS1Messages int           `yaml:"maxInflightQOS1Messages" json:"maxInflightQOS1Messages" default:"20" validate:"min=1"`
	RepublishInterval       time.Duration `yaml:"republishInterval" json:"republishInterval" default:"20s"`
	CleanInterval           time.Duration `yaml:"cleanInterval" json:"cleanInterval" default:"1h"`
	ExpireDay               int           `yaml:"expireDay" json:"expireDay" default:"7" validate:"min=1"`
	MaxConnections          int           `yaml:"maxConnections" json:"maxConnections"`
	SysTopics               []string      `yaml:"sysTopics" json:"sysTopics" default:"[\"$link\"]"`
}

// Manager the manager of sessions
type Manager struct {
	cfg       Config
	auth      *auth.Auth
	sessiondb *Backend
	retaindb  *retain.Backend
	exchange  *exchange.Exchange
	checker   *mqtt.TopicChecker
	sessions  map[string]*Session
	clients   map[string]client            // TODO: limit the number of clients
	bindings  map[string]map[string]client // map[sid]map[cid]client
	log       *log.Logger
	sync.Mutex
}

// NewManager create a new session manager
func NewManager(cfg Config, auth *auth.Auth) (*Manager, error) {
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
		cfg:       cfg,
		auth:      auth,
		sessiondb: sessiondb,
		retaindb:  retaindb,
		checker:   mqtt.NewTopicChecker(cfg.SysTopics),
		exchange:  exchange.NewExchange(cfg.SysTopics),
		sessions:  map[string]*Session{},
		clients:   map[string]client{},
		bindings:  map[string]map[string]client{},
		log:       log.With(log.Any("manager", "session")),
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
func (m *Manager) ClientMQTTHandler(connection mqtt.Connection, anonymous bool) {
	m.Lock()
	defer m.Unlock()

	if m.cfg.MaxConnections > 0 && len(m.clients) >= m.cfg.MaxConnections {
		m.log.Warn(ErrConnectionExceeds.Error(), log.Any("max", m.cfg.MaxConnections))
		connection.Close()
		return
	}

	c := newClientMQTT(m, connection, anonymous)
	m.clients[c.getID()] = c
}

// Call handler of link
func (m *Manager) Call(ctx context.Context, msg *link.Message) (*link.Message, error) {
	if msg.Context.Topic == "" {
		return nil, ErrMessageInvalid
	}
	m.exchange.Route(msg, nil)
	return nil, nil
}

// Talk handler of link
func (m *Manager) Talk(stream link.Link_TalkServer) error {
	// m.Lock()
	// defer m.Unlock()

	// if m.cfg.MaxConnections > 0 && len(m.clients) >= m.cfg.MaxConnections {
	// 	m.log.Warn(ErrConnectionExceeds.Error(), log.Any("max", m.cfg.MaxConnections))
	// 	return ErrConnectionExceeds
	// }

	// c := newClientGRPC(m, stream)
	// m.clients[c.getID()] = c
	return errors.New("not implemented")
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
		}
	} else {
		s, err = m.newSession(si)
		if err != nil {
			return
		}
		m.sessions[sid] = s
	}

	// TODO: if no topic subscribed, the session does not need to create queues
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
	return
}

func (m *Manager) newSession(si *Info) (*Session, error) {
	sid := si.ID
	cfg := queue.Config{
		Name:          sid,
		Driver:        m.cfg.PersistenceDriver,
		Location:      m.cfg.PersistenceLocation,
		ExpireTime:    24 * time.Duration(m.cfg.ExpireDay) * time.Hour,
		CleanInterval: m.cfg.CleanInterval,
	}
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
		qos1: queue.NewPersistence(cfg, queuedb, m.cfg.MaxInflightQOS1Messages),
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

//  * remove client and session

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
