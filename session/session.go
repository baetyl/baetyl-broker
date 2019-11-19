package session

import (
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// Info session information
type Info struct {
	ID            string
	CleanSession  bool
	Subscriptions map[string]common.QOS
}

// Session session of a client
type Session struct {
	info    Info
	store   *Store
	backend *Backend
	subs    *common.Trie
	qos0    queue.Queue // queue for qos0
	qos1    queue.Queue // queue for qos1
	sync.RWMutex
}

func (s *Store) newSession(info Info) (*Session, error) {
	backend, err := s.backend.NewQueueBackend(info.ID)
	if err != nil {
		return nil, err
	}
	ses := &Session{
		store: s,
		info:  info,
		subs:  common.NewTrie(),
		qos0:  queue.NewTemporary(s.config.InflightQOS0Messages, true),
		qos1:  queue.NewPersistence(s.config.InflightQOS1Messages, backend),
	}
	for topic, qos := range info.Subscriptions {
		ses.subs.Add(topic, qos)
		s.exchange.Bind(topic, ses)
		// TODO: it is unnecessary to subscribe messages with qos 0 for the session without any client
	}
	return ses, nil
}

// ID returns the session ID
func (s *Session) ID() string {
	s.RLock()
	defer s.RUnlock()
	return s.info.ID
}

// Push pushes source message to session queue
func (s *Session) Push(e *common.Event) error {
	// always flow message with qos 0 into qos0 queue
	if e.Context.QOS == 0 {
		return s.qos0.Push(e)
	}
	// TODO: improve
	qs := s.subs.Match(e.Context.Topic)
	if len(qs) == 0 {
		panic("At least one object matched")
	}
	for _, q := range qs {
		if q.(common.QOS) > 0 {
			return s.qos1.Push(e)
		}
	}
	return s.qos0.Push(e)
}

// Subscribe subscribes topics
func (s *Session) Subscribe(subs []common.Subscription) error {
	s.Lock()
	defer s.Unlock()
	for _, sub := range subs {
		s.info.Subscriptions[sub.Topic] = sub.QOS
		s.subs.Add(sub.Topic, sub.QOS)
		s.store.exchange.Bind(sub.Topic, s)
	}
	if !s.info.CleanSession {
		return s.backend.Set(&s.info)
	}
	return nil
}

// Unsubscribe unsubscribes topics
func (s *Session) Unsubscribe(topics []string) error {
	s.Lock()
	defer s.Unlock()

	for _, t := range topics {
		delete(s.info.Subscriptions, t)
		s.subs.Remove(t, s)
		s.store.exchange.Unbind(t, s)
	}
	if !s.info.CleanSession {
		return s.backend.Set(&s.info)
	}
	return nil
}

// Close closes session
func (s *Session) Close() error {
	log.Debug("session is closing")
	defer log.Debug("session has closed")

	return s.backend.Close()
}
