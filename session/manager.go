package session

import (
	"errors"
	"sync/atomic"

	"github.com/baetyl/baetyl-broker/database"
	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors
var (
	ErrConnectionRefuse                          = errors.New("connection refuse on server side")
	ErrSessionClientAlreadyClosed                = errors.New("session client is already closed")
	ErrSessionClientAlreadyConnecting            = errors.New("session client is already connected")
	ErrSessionClientPacketUnexpected             = errors.New("session client received unexpected packet")
	ErrSessionClientPacketIDConflict             = errors.New("packet id conflict, to acknowledge old packet")
	ErrSessionClientPacketNotFound               = errors.New("packet id is not found")
	ErrSessionClientIDInvalid                    = errors.New("client ID is invalid")
	ErrSessionProtocolVersionInvalid             = errors.New("protocol version is invalid")
	ErrSessionUsernameNotSet                     = errors.New("username is not set")
	ErrSessionPasswordNotSet                     = errors.New("password is not set")
	ErrSessionUsernameNotPermitted               = errors.New("username or password is not permitted")
	ErrSessionCertificateCommonNameNotFound      = errors.New("certificate common name is not found")
	ErrSessionCertificateCommonNameNotPermitted  = errors.New("certificate common name is not permitted")
	ErrSessionMessageQosNotSupported             = errors.New("message QOS is not supported")
	ErrSessionMessageTopicInvalid                = errors.New("message topic is invalid")
	ErrSessionMessageTopicNotPermitted           = errors.New("message topic is not permitted")
	ErrSessionMessageTypeInvalid                 = errors.New("message type is invalid")
	ErrSessionMessagePayloadSizeExceedsLimit     = errors.New("message payload exceeds the max limit")
	ErrSessionWillMessageQosNotSupported         = errors.New("will QoS is not supported")
	ErrSessionWillMessageTopicInvalid            = errors.New("will topic is invalid")
	ErrSessionWillMessageTopicNotPermitted       = errors.New("will topic is not permitted")
	ErrSessionWillMessagePayloadSizeExceedsLimit = errors.New("will message payload exceeds the max limit")
	ErrSessionLinkIDNotSet                       = errors.New("link id is not set")
	ErrSessionAbnormal                           = errors.New("session is abnormal")
)

type client interface {
	getID() string
	setSession(sid string, s *Session)
	authorize(string, string) bool
	sendEvent(e *eventWrapper, dup bool) error
	close() error
}

// Manager the manager of sessions
type Manager struct {
	cfg           Config
	db            database.DB
	sessions      *syncmap
	checker       *mqtt.TopicChecker
	exch          *exchange.Exchange
	auth          *Authenticator
	sessionBucket database.Bucket
	retainBucket  database.Bucket
	log           *log.Logger
	quit          int32 // if quit != 0, it means manager is closed
}

// NewManager create a new session manager
func NewManager(cfg Config) (m *Manager, err error) {
	m = &Manager{
		cfg:      cfg,
		sessions: newSyncMap(cfg.MaxSessions),
		checker:  mqtt.NewTopicChecker(cfg.SysTopics),
		exch:     exchange.NewExchange(cfg.SysTopics),
		auth:     NewAuthenticator(cfg.Principals),
		log:      log.With(log.Any("session", "manager")),
	}
	m.db, err = database.New(cfg.DB)
	if err != nil {
		return nil, err
	}
	m.sessionBucket, err = m.db.NewBucket("session", nil)
	if err != nil {
		m.Close()
		return
	}
	m.retainBucket, err = m.db.NewBucket("retain", nil)
	if err != nil {
		m.Close()
		return
	}
	var ss []Info
	// load stored sessions from backend database
	err = m.sessionBucket.ListKV(&ss)
	if err != nil {
		m.Close()
		return
	}
	for _, i := range ss {
		m.checkSubscriptions(&i)
		_, _, err = m.initSession(i)
		if err != nil {
			m.Close()
			return
		}
	}
	m.log.Info("session manager has initialized")
	return m, nil
}

func (m *Manager) initSession(si Info) (s *Session, exists bool, err error) {
	if err = m.checkQuitState(); err != nil {
		return
	}

	var v interface{}
	if v, exists = m.sessions.load(si.ID); exists {
		s = v.(*Session)
	} else {
		s = newSession(si, m)
		err = m.sessions.store(si.ID, s)
		if err != nil {
			s.log.Error("number of sessions exceeds the limit", log.Any("max", s.mgr.cfg.MaxSessions))
			return
		}
	}

	s.updateInfo(&si, nil, nil, nil)
	err = s.updateState()
	return
}

func (m *Manager) checkQuitState() error {
	if atomic.LoadInt32(&m.quit) == 1 {
		m.log.Error(ErrConnectionRefuse.Error())
		return ErrConnectionRefuse
	}
	return nil
}

func (m *Manager) checkSubscriptions(si *Info) {
	for topic, qos := range si.Subscriptions {
		// Re-check subscriptions, if topic invalid, log error, delete and skip
		if qos != 0 && qos != 1 {
			m.log.Warn(ErrSessionMessageQosNotSupported.Error(), log.Any("qos", qos))
			delete(si.Subscriptions, topic)
			continue
		}
		if !m.checker.CheckTopic(topic, true) {
			m.log.Warn(ErrSessionMessageTopicInvalid.Error(), log.Any("topic", topic))
			delete(si.Subscriptions, topic)
			continue
		}
	}
}

// Close close
func (m *Manager) Close() error {
	m.log.Info("session manager is closing")
	defer m.log.Info("session manager has closed")

	atomic.AddInt32(&m.quit, -1)

	for _, v := range m.sessions.empty() {
		v.(*Session).Close()
	}
	if m.sessionBucket != nil {
		m.sessionBucket.Close(false)
	}
	if m.retainBucket != nil {
		m.retainBucket.Close(false)
	}
	if m.db != nil {
		m.db.Close()
	}
	return nil
}

// * retain message operations

func (m *Manager) listRetainedMessages() ([]*link.Message, error) {
	var retains []RetainMessage
	err := m.retainBucket.ListKV(&retains)
	if err != nil {
		return nil, err
	}
	msgs := make([]*link.Message, 0)
	for _, v := range retains {
		msgs = append(msgs, v.Message)
	}
	return msgs, nil
}

func (m *Manager) retainMessage(topic string, msg *link.Message) error {
	return m.retainBucket.SetKV(topic, &RetainMessage{Topic: msg.Context.Topic, Message: msg})
}

func (m *Manager) unretainMessage(topic string) error {
	return m.retainBucket.DelKV(topic)
}
