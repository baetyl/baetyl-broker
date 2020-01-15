package session

import (
	"encoding/base64"
	"errors"
	"sync"

	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/retain"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	cmap "github.com/orcaman/concurrent-map"
)

// all errors
var (
	ErrConnectionRefuse                    	    = errors.New("connection refuse, sessions are closing")
	ErrConnectionExceeds                   	    = errors.New("number of connections exceeds the max limit")
	ErrSessionClientAlreadyConnecting      	    = errors.New("session client is already connecting")
	ErrSessionClientPacketUnexpected       	    = errors.New("session client received unexpected packet")
	ErrSessionClientPacketIDConflict       	    = errors.New("packet id conflict, to acknowledge old packet")
	ErrSessionClientPacketNotFound         	    = errors.New("packet id is not found")
	ErrSessionProtocolVersionInvalid       	    = errors.New("protocol version is invalid")
	ErrSessionClientIDInvalid              	    = errors.New("client ID is invalid")
	ErrSessionUsernameNotSet               	    = errors.New("username is not set")
	ErrSessionPasswordNotSet               	    = errors.New("password is not set")
	ErrSessionUsernameNotPermitted         	    = errors.New("username or password is not permitted")
	ErrSessionCertificateCommonNameNotFound     = errors.New("certificate common name is not found")
	ErrSessionCertificateCommonNameNotPermitted = errors.New("certificate common name is not permitted")
	ErrSessionMessageQosNotSupported       	    = errors.New("message QOS is not supported")
	ErrSessionMessageTopicInvalid          	    = errors.New("message topic is invalid")
	ErrSessionMessageTopicNotPermitted     	    = errors.New("message topic is not permitted")
	ErrSessionMessageTypeInvalid           	    = errors.New("message type is invalid")
	ErrSessionWillMessageQosNotSupported   	    = errors.New("will QoS is not supported")
	ErrSessionWillMessageTopicInvalid      	    = errors.New("will topic is invalid")
	ErrSessionWillMessageTopicNotPermitted 	    = errors.New("will topic is not permitted")
	ErrSessionLinkIDNotSet                 	    = errors.New("link id is not set")
	ErrSessionAbnormal                     	    = errors.New("session is abnormal")
)

type client interface {
	getID() string
	getSession() *Session
	setSession(*Session)
	close() error
}

// Manager the manager of sessions
type Manager struct {
	cfg           Config
	sessiondb     *Backend
	retaindb      *retain.Backend
	exchange      *exchange.Exchange
	checker       *mqtt.TopicChecker
	authenticator *Authenticator
	sessions      cmap.ConcurrentMap // map[sid]session
	clients       cmap.ConcurrentMap // map[cid]client
	tomb          utils.Tomb
	log           *log.Logger
	mu            sync.Mutex
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
		sessions:      cmap.New(),
		clients:       cmap.New(),
		log:           log.With(log.Any("session", "manager")),
	}
	for _, i := range items {
		si := i.(*Info)
		s, err := manager.newSession(si)
		if err != nil {
			manager.Close()
			return nil, err
		}
		manager.sessions.Set(si.ID, s)
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
	m.tomb.Kill(nil)

	for item := range m.clients.IterBuffered() {
		item.Val.(client).close()
	}
	for item := range m.sessions.IterBuffered() {
		item.Val.(*Session).close()
	}

	m.retaindb.Close()
	m.sessiondb.Close()
	return nil
}

// * init session for client

// initSession init session for client, creates new session if not exists
func (m *Manager) initSession(si *Info, c client, exclusive bool) (exists bool, err error) {
	// prepare session
	sid := si.ID
	var sv interface{}
	if sv, exists = m.sessions.Get(sid); !exists {
		sv, err = m.newSession(si)
		if err != nil {
			return
		}
		if exists = !m.sessions.SetIfAbsent(sid, sv); exists {
			sv.(*Session).close()
			var ok bool
			if sv, ok = m.sessions.Get(sid); !ok {
				return false, ErrSessionAbnormal
			}
		}
	}
	s := sv.(*Session)
	ocs := s.addClient(c, exclusive)
	for _, oc := range ocs {
		oc.close() // close old clients
	}
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
	// only used by link client
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
		clis: map[string]client{},
		qos0: queue.NewTemporary(sid, m.cfg.MaxInflightQOS0Messages, true),
		qos1: queue.NewPersistence(cfg, queuedb),
		log:  m.log.With(log.Any("id", sid)),
	}, nil
}

// * subscription

// Subscribe subscribes topics
// only used by mqtt client
func (m *Manager) subscribe(c client, subs []mqtt.Subscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.mu.Lock()
	defer m.mu.Unlock()

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
	if !m.tomb.Alive() {
		m.log.Error(ErrConnectionRefuse.Error())
		return ErrConnectionRefuse
	}
	if m.cfg.MaxConnections > 0 && m.clients.Count() >= m.cfg.MaxConnections {
		m.log.Error(ErrConnectionExceeds.Error(), log.Any("max", m.cfg.MaxConnections))
		return ErrConnectionExceeds
	}
	return nil
}

//  * client operations

func (m *Manager) addClient(c client) {
	m.clients.Set(c.getID(), c)
}

// delClient deletes client and clean session if needs
func (m *Manager) delClient(c client) {
	// remove client
	cid := c.getID()
	m.clients.Remove(cid)

	// remove session if needs
	s := c.getSession()
	if s == nil {
		return
	}

	if s.delClient(c) {
		// close session if not bound and CleanSession=true
		sid := s.ID
		m.sessiondb.Del(sid)
		m.exchange.UnbindAll(s)
		s.close()
		m.sessions.Remove(sid)
		m.log.Info("session is removed", log.Any("sid", sid))
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
