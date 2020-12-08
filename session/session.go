package session

import (
	"encoding/json"
	"sync"

	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-broker/v2/common"
	"github.com/baetyl/baetyl-broker/v2/queue"
)

// Info session information
type Info struct {
	ID            string              `json:"id,omitempty"`
	WillMessage   *mqtt.Message       `json:"will,omitempty"`
	Subscriptions map[string]mqtt.QOS `json:"subs,omitempty"`
	CleanSession  bool                `json:"-"`
}

func (i *Info) String() string {
	d, _ := json.Marshal(i)
	return string(d)
}

// Session session of a client
type Session struct {
	info    Info
	manager *Manager
	subs    *mqtt.Trie
	cnt     *mqtt.Counter
	qos0msg queue.Queue // queue for qos0
	qos1msg queue.Queue // queue for qos1
	qos1pkt *cache
	qos1ack chan *eventWrapper
	log     *log.Logger
	mut     sync.RWMutex // mutex for session
}

func newSession(i Info, m *Manager) (*Session, error) {
	cnt := mqtt.NewCounter()
	s := &Session{
		info:    i,
		manager: m,
		subs:    mqtt.NewTrie(),
		cnt:     cnt,
		qos0msg: queue.NewTemporary(i.ID, m.cfg.MaxInflightQOS0Messages, true),
		qos1ack: make(chan *eventWrapper, m.cfg.MaxInflightQOS1Messages),
		qos1pkt: &cache{
			offset: cnt.GetNextID(),
		},
		log: m.log.With(log.Any("id", i.ID)),
	}

	// TODO: will data race if directly visit manager
	qc := m.cfg.Persistence.Queue
	qc.Name = i.ID
	qc.BatchSize = m.cfg.MaxInflightQOS1Messages
	qbk, err := m.store.NewBatchBucket(qc.Name)
	if err != nil {
		s.log.Error("failed to create qos1 bucket", log.Error(err))
		return nil, err
	}
	s.qos1msg, err = queue.NewPersistence(qc, qbk)
	if err != nil {
		s.log.Error("failed to create qos1 persistent", log.Error(err))
		return nil, err
	}

	for topic, qos := range i.Subscriptions {
		s.subs.Set(topic, qos)
		s.manager.exch.Bind(topic, s)
		s.info.Subscriptions[topic] = qos
	}

	s.persistent()

	return s, nil
}

func (s *Session) close() {
	s.log.Info("session is closing")
	defer s.log.Info("session has closed")

	if s.qos0msg != nil {
		s.qos0msg.Close(s.info.CleanSession)
	}

	if s.qos1msg != nil {
		s.qos1msg.Close(s.info.CleanSession)
	}
}

// * the following operations need lock

// TODO: return err or just log
func (s *Session) update(si Info, auth func(action, topic string) bool) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.info.WillMessage = si.WillMessage
	s.info.CleanSession = si.CleanSession

	for topic := range s.info.Subscriptions {
		if auth != nil && !auth(Subscribe, topic) {
			s.log.Warn(ErrSessionMessageTopicNotPermitted.Error(), log.Any("topic", topic))
			s.subs.Empty(topic)
			s.manager.exch.Unbind(topic, s)
			delete(s.info.Subscriptions, topic)
		}
	}

	// reset qos1 queue
	if s.qos1msg != nil {
		err := s.qos1msg.Close(si.CleanSession)
		if err != nil {
			s.log.Error("failed to close qos1 queue when update", log.Error(err))
			return
		}
	}

	// TODO: will data race if directly visit manager
	qc := s.manager.cfg.Persistence.Queue
	qc.Name = si.ID
	qc.BatchSize = s.manager.cfg.MaxInflightQOS1Messages
	qbk, err := s.manager.store.NewBatchBucket(qc.Name)
	if err != nil {
		s.log.Error("failed to create qos1 bucket", log.Error(err))
		return
	}
	s.qos1msg, err = queue.NewPersistence(qc, qbk)
	if err != nil {
		s.log.Error("failed to create qos1 persistent", log.Error(err))
		return
	}

	s.persistent()
}

func (s *Session) disableQos1() {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.qos1msg != nil {
		s.qos1msg.Disable()
	}
}

// Push pushes source message to session queue
func (s *Session) Push(e *common.Event) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// always flow message with qos 0 into qos0 queue
	if e.Context.QOS == 0 {
		return s.qos0msg.Push(e)
	}

	// TODO: improve
	qs := s.subs.Match(e.Context.Topic)
	if len(qs) == 0 {
		s.log.Warn("a message is ignored since there is no sub matched", log.Any("message", e.String()))
		e.Done()
		return nil
	}

	for _, q := range qs {
		if q.(mqtt.QOS) > 0 {
			// chose maximum QoS of all the matching subscriptions. [MQTT-3.3.5-1]
			return s.qos1msg.Push(e)
		}
	}

	return s.qos0msg.Push(e)
}

// * the following operations are only used by mqtt client

func (s *Session) subscribe(subs []mqtt.Subscription, auth func(action, topic string) bool) {
	if len(subs) == 0 {
		return
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	if s.info.Subscriptions == nil {
		s.info.Subscriptions = make(map[string]mqtt.QOS)
	}

	for _, v := range subs {
		if auth != nil && !auth(Subscribe, v.Topic) {
			s.log.Warn(ErrSessionMessageTopicNotPermitted.Error(), log.Any("topic", v.Topic))
			continue
		}
		s.subs.Set(v.Topic, v.QOS)
		s.manager.exch.Bind(v.Topic, s)
		s.info.Subscriptions[v.Topic] = v.QOS
	}

	s.persistent()
}

func (s *Session) unsubscribe(topics []string) {
	if len(topics) == 0 {
		return
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	for _, topic := range topics {
		s.subs.Empty(topic)
		s.manager.exch.Unbind(topic, s)
		delete(s.info.Subscriptions, topic)
	}

	s.persistent()
}

func (s *Session) will() *mqtt.Message {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.info.WillMessage
}

func (s *Session) cleanWill() {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.info.WillMessage = nil
	s.persistent()
}

func (s *Session) matchQOS(topic string) (bool, uint32) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return mqtt.MatchTopicQOS(s.subs, topic)
}

func (s *Session) acknowledge(id uint64) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	err := s.qos1pkt.delete(id)
	if err != nil {
		s.log.Warn("failed to acknowledge", log.Any("id", id), log.Error(err))
	}
}

func (s *Session) persistent() {
	if s.info.CleanSession {
		err := s.manager.sessionBucket.DelKV([]byte(s.info.ID))
		if err != nil {
			s.log.Error("failed to delete session", log.Error(err))
		}
		return
	}

	data, err := json.Marshal(s.info)
	if err != nil {
		s.log.Error("failed to marshal session info", log.Error(err))
		return
	}
	err = s.manager.sessionBucket.SetKV([]byte(s.info.ID), data)
	if err != nil {
		s.log.Error("failed to persist session", log.Error(err))
	}
}
