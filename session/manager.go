package session

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors
var (
	ErrConnectionRefuse                          = errors.New("connection refuse on server side")
	ErrSessionClosed                             = errors.New("session is closed")
	ErrSessionNumberExceeds                      = errors.New("number of sessions exceeds the limit")
	ErrSessionClientNumberExceeds                = errors.New("number of session clients exceeds the limit")
	ErrSessionClientAlreadyConnecting            = errors.New("session client is already connecting")
	ErrSessionClientPacketUnexpected             = errors.New("session client received unexpected packet")
	ErrSessionClientPacketIDConflict             = errors.New("packet id conflict, to acknowledge old packet")
	ErrSessionClientPacketNotFound               = errors.New("packet id is not found")
	ErrSessionProtocolVersionInvalid             = errors.New("protocol version is invalid")
	ErrSessionClientIDInvalid                    = errors.New("client ID is invalid")
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
	getSession() *Session
	setSession(sid string, s *Session)
	authorize(string, string) bool
	sendEvent(e *eventWrapper, dup bool) error
	close() error
}

// Manager the manager of sessions
type Manager struct {
	cfg      Config
	sessions sync.Map
	checker  *mqtt.TopicChecker
	sstore   *Backend       // session store to persist sessions
	mstore   *RetainBackend // message store to retain messages
	exch     *exchange.Exchange
	auth     *Authenticator
	log      *log.Logger
	quit     int32 // if quit != 0, it means manager is closed
}

// NewManager create a new session manager
func NewManager(cfg Config) (m *Manager, err error) {
	m = &Manager{
		cfg:     cfg,
		checker: mqtt.NewTopicChecker(cfg.SysTopics),
		exch:    exchange.NewExchange(cfg.SysTopics),
		auth:    NewAuthenticator(cfg.Principals),
		log:     log.With(log.Any("session", "manager")),
	}
	m.sstore, err = NewBackend(cfg)
	if err != nil {
		m.Close()
		return
	}
	m.mstore, err = m.sstore.NewRetainBackend()
	if err != nil {
		m.Close()
		return
	}
	// load stored sessions from backend database
	ss, err := m.sstore.List()
	if err != nil {
		m.Close()
		return
	}
	for _, i := range ss {
		si := i.(*Info)
		m.checkSubscriptions(si)
		_, _, err = m.initSession(*si)
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
	if v, exists = m.sessions.Load(si.ID); exists {
		s = v.(*Session)
	} else {
		s = newSession(si, m)
		if v, exists = m.sessions.LoadOrStore(si.ID, s); exists {
			s = v.(*Session)
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

func (m *Manager) checkSessions() error {
	if err := m.checkQuitState(); err != nil {
		return err
	}
	if m.cfg.MaxSessions > 0 && m.countSessions() >= m.cfg.MaxSessions {
		m.log.Error(ErrSessionNumberExceeds.Error(), log.Any("max", m.cfg.MaxSessions))
		return ErrSessionNumberExceeds
	}
	return nil
}

func (m *Manager) countSessions() (count int) {
	m.sessions.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return
}

// Close close
func (m *Manager) Close() error {
	m.log.Info("session manager is closing")
	defer m.log.Info("session manager has closed")

	atomic.AddInt32(&m.quit, -1)

	m.sessions.Range(func(_, v interface{}) bool {
		v.(*Session).Close()
		return true
	})
	if m.mstore != nil {
		m.mstore.Close()
	}
	if m.sstore != nil {
		m.sstore.Close()
	}
	return nil
}

// * retain message operations

func (m *Manager) listRetainedMessages() ([]*link.Message, error) {
	retains, err := m.mstore.List()
	if err != nil {
		return nil, err
	}
	msgs := make([]*link.Message, 0)
	for _, v := range retains {
		msg := v.(*RetainMessage).Message
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (m *Manager) retainMessage(topic string, msg *link.Message) error {
	return m.mstore.Set(&RetainMessage{Topic: msg.Context.Topic, Message: msg})
}

func (m *Manager) unretainMessage(topic string) error {
	return m.mstore.Del(topic)
}
