package session

import (
	"errors"
	"sync"

	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/retain"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors
var (
	ErrConnectionRefuse                          = errors.New("connection refuse, sessions are closing")
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
	setSession(*Session)
	authorize(string, string) bool
	sendEvent(e *eventWrapper, dup bool) error
	close() error
}

// Manager the manager of sessions
type Manager struct {
	cfg      Config
	sstore   *Backend        // session store to persist sessions
	mstore   *retain.Backend // message store to retain messages
	checker  *mqtt.TopicChecker
	sessions map[string]*Session
	exch     *exchange.Exchange
	auth     *Authenticator
	mut      sync.RWMutex
	log      *log.Logger
	quit     bool
}

// NewManager create a new session manager
func NewManager(cfg Config) (m *Manager, err error) {
	m = &Manager{
		cfg:      cfg,
		sessions: map[string]*Session{},
		checker:  mqtt.NewTopicChecker(cfg.SysTopics),
		exch:     exchange.NewExchange(cfg.SysTopics),
		auth:     NewAuthenticator(cfg.Principals),
		log:      log.With(log.Any("session", "manager")),
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
	sessions, err := m.sstore.List()
	if err != nil {
		m.Close()
		return
	}
	for _, i := range sessions {
		_, err = m.initSession(i.(*Info), nil, false)
		if err != nil {
			m.Close()
			return
		}
	}
	m.log.Info("session manager has initialized")
	return m, nil
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
	m.mut.RLock()
	defer m.mut.RUnlock()

	if m.quit {
		m.log.Error(ErrConnectionRefuse.Error())
		return ErrConnectionRefuse
	}

	if m.cfg.MaxSessions > 0 && len(m.sessions) >= m.cfg.MaxSessions {
		m.log.Error(ErrSessionNumberExceeds.Error(), log.Any("max", m.cfg.MaxSessions))
		return ErrSessionNumberExceeds
	}
	return nil
}

func (m *Manager) initSession(si *Info, c client, exclusive bool) (exists bool, err error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.quit {
		err = ErrConnectionRefuse
		m.log.Error(err.Error())
		return
	}

	// prepare session
	var s *Session
	if s, exists = m.sessions[si.ID]; exists {
		s.update(si)
	} else {
		s = newSession(si, m)
		m.sessions[si.ID] = s
	}

	if s.CleanSession {
		err = m.sstore.Del(s.ID)
	} else {
		err = m.sstore.Set(&s.Info)
	}

	if c != nil {
		err = s.addClient(c, exclusive)
		if err != nil {
			return false, err
		}
	} else {
		if len(s.Subscriptions) == 0 {
			s.gotoState0()
		} else {
			err = s.gotoState2()
			if err != nil {
				return false, err
			}
		}
	}

	// bind session to exchage after resources are prepared
	for topic := range s.Subscriptions {
		m.exch.Bind(topic, s)
	}
	return
}

func (m *Manager) removeSession(s *Session) {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.exch.UnbindAll(s)
	delete(m.sessions, s.ID)
}

// Close close
func (m *Manager) Close() error {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.log.Info("session manager is closing")
	defer m.log.Info("session manager has closed")

	m.quit = true

	for _, s := range m.sessions {
		s.Close()
	}
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
		msg := v.(*retain.Retain).Message
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (m *Manager) retainMessage(topic string, msg *link.Message) error {
	return m.mstore.Set(&retain.Retain{Topic: msg.Context.Topic, Message: msg})
}

func (m *Manager) unretainMessage(topic string) error {
	return m.mstore.Del(topic)
}
