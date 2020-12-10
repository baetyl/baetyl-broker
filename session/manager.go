package session

import (
	"encoding/json"
	"sync/atomic"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-broker/v2/exchange"
	"github.com/baetyl/baetyl-broker/v2/store"

	"github.com/gogo/protobuf/proto"
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
	ErrSessionUsernameNotPermitted               = errors.New("username or password is not permitted")
	ErrSessionCertificateCommonNameNotFound      = errors.New("certificate common name is not found")
	ErrSessionCertificateCommonNameNotPermitted  = errors.New("certificate common name is not permitted")
	ErrSessionMessageQosNotSupported             = errors.New("message QOS is not supported")
	ErrSessionMessageTopicInvalid                = errors.New("message topic is invalid")
	ErrSessionMessageTopicNotPermitted           = errors.New("message topic is not permitted")
	ErrSessionMessagePayloadSizeExceedsLimit     = errors.New("message payload exceeds the max limit")
	ErrSessionWillMessageQosNotSupported         = errors.New("will QoS is not supported")
	ErrSessionWillMessageTopicInvalid            = errors.New("will topic is invalid")
	ErrSessionWillMessageTopicNotPermitted       = errors.New("will topic is not permitted")
	ErrSessionWillMessagePayloadSizeExceedsLimit = errors.New("will message payload exceeds the max limit")
	ErrSessionSubscribePayloadEmpty              = errors.New("subscribe payload can't be empty")
	ErrSessionManagerClosed                      = errors.New("manager has closed")
)

// Manager the manager of sessions
type Manager struct {
	cfg           Config
	store         store.DB
	sessions      *syncmap
	clients       *syncmap
	checker       *mqtt.TopicChecker
	exch          *exchange.Exchange
	auth          *Authenticator
	sessionBucket store.KVBucket
	retainBucket  store.KVBucket
	log           *log.Logger
	quit          int32 // if quit != 0, it means manager is closed
}

// NewManager create a new session manager
func NewManager(cfg Config) (m *Manager, err error) {
	m = &Manager{
		cfg:      cfg,
		sessions: newSyncMap(),
		clients:  newSyncMap(),
		checker:  mqtt.NewTopicChecker(cfg.SysTopics),
		exch:     exchange.NewExchange(cfg.SysTopics),
		auth:     NewAuthenticator(cfg.Principals),
		log:      log.With(log.Any("session", "manager")),
	}
	m.store, err = store.New(cfg.Persistence.Store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.sessionBucket, err = m.store.NewKVBucket("#session")
	if err != nil {
		_err := m.Close()
		if _err != nil {
			m.log.Error("failed to close manager", log.Error(_err))
		}
		return
	}
	m.retainBucket, err = m.store.NewKVBucket("#retain")
	if err != nil {
		_err := m.Close()
		if _err != nil {
			m.log.Error("failed to close manager", log.Error(_err))
		}
		return
	}
	var ss []Info
	// load stored sessions from backend database
	err = m.sessionBucket.ListKV(func(data []byte) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := Info{}
		if err := json.Unmarshal(data, &v); err != nil {
			return errors.Trace(err)
		}
		ss = append(ss, v)
		return nil
	})
	if err != nil {
		_err := m.Close()
		if _err != nil {
			m.log.Error("failed to close manager", log.Error(_err))
		}
		return
	}

	for _, si := range ss {
		m.checkSubscriptions(&si)

		var s *Session
		s, err = newSession(si, m)
		if err != nil {
			_err := m.Close()
			if _err != nil {
				m.log.Error("failed to close manager", log.Error(_err))
			}
			return
		}

		m.sessions.store(si.ID, s)
	}
	m.log.Info("session manager has initialized")
	return m, nil
}

func (m *Manager) addClient(si Info, c *Client) (s *Session, exists bool, err error) {
	if err = m.checkQuitState(); err != nil {
		return s, exists, errors.Trace(err)
	}

	defer func() {
		if err != nil {
			m.clients.delete(si.ID)
			return
		}
		c.setSession(si.ID, s)
	}()

	if v, loaded := m.clients.store(si.ID, c); loaded {
		err := v.(*Client).close()
		if err != nil {
			m.log.Error("failed to close old client", log.Any("id", v.(*Client).id), log.Error(err))
			return nil, false, errors.Trace(err)
		}
	}

	if v, loaded := m.sessions.load(si.ID); loaded {
		s = v.(*Session)
		if !s.info.CleanSession {
			exists = true
			s.update(si, c.authorize)
			return s, exists, errors.Trace(err)
		}

		// If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one. [MQTT-3.1.2-6]
		m.cleanSession(s)
	}

	s, err = newSession(si, m)
	if err != nil {
		return s, exists, errors.Trace(err)
	}

	m.sessions.store(si.ID, s)
	return
}

func (m *Manager) delClient(clientID string) error {
	if err := m.checkQuitState(); err != nil {
		return errors.Trace(err)
	}

	m.clients.delete(clientID)

	v, exists := m.sessions.load(clientID)
	if !exists {
		return nil
	}

	s := v.(*Session)
	// TODO: need lock here ?
	if s.info.CleanSession {
		m.cleanSession(s)
	}

	s.disableQos1()
	return nil
}

func (m *Manager) cleanSession(s *Session) {
	m.exch.UnbindAll(s)
	m.sessions.delete(s.info.ID)
	s.close()
}

func (m *Manager) checkQuitState() error {
	if atomic.LoadInt32(&m.quit) == 1 {
		m.log.Error(ErrSessionManagerClosed.Error())
		return ErrSessionManagerClosed
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
	if err := m.checkQuitState(); err != nil {
		return errors.Trace(err)
	}

	m.log.Info("session manager is closing")
	defer m.log.Info("session manager has closed")

	atomic.AddInt32(&m.quit, -1)

	for _, s := range m.sessions.empty() {
		s.(*Session).close()
	}

	for _, c := range m.clients.empty() {
		err := c.(*Client).close()
		if err != nil {
			m.log.Error("failed to close client", log.Any("id", c.(*Client).id), log.Error(err))
		}
	}

	if m.store != nil {
		err := m.store.Close()
		if err != nil {
			m.log.Error("failed to close store", log.Error(err))
		}
	}
	return nil
}

// * retain message operations

func (m *Manager) listRetainedMessages() ([]*mqtt.Message, error) {
	msgs := make([]*mqtt.Message, 0)
	err := m.retainBucket.ListKV(func(data []byte) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := new(mqtt.Message)
		if err := proto.Unmarshal(data, v); err != nil {
			return errors.Trace(err)
		}
		msgs = append(msgs, v)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return msgs, nil
}

func (m *Manager) retainMessage(msg *mqtt.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return m.retainBucket.SetKV([]byte(msg.Context.Topic), data)
}

func (m *Manager) unretainMessage(topic string) error {
	return m.retainBucket.DelKV([]byte(topic))
}
