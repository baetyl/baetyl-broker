package session

import (
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// Info session information
type Info struct {
	ID            string              `json:"id,omitempty"`
	WillMessage   *mqtt.Message       `json:"will,omitempty"`
	Subscriptions map[string]mqtt.QOS `json:"subs,omitempty"`
	CleanSession  bool                `json:"-"`
}

// Session session of a client
type Session struct {
	info   Info
	mgr    *Manager
	qos0   queue.Queue // queue for qos0
	qos1   queue.Queue // queue for qos1
	client *Client
	subs   *mqtt.Trie
	cnt    *mqtt.Counter
	log *log.Logger
	mut sync.RWMutex // mutex for session
}

func newSession(i Info, m *Manager) *Session {
	return &Session{
		info: i,
		mgr:  m,
		subs: mqtt.NewTrie(),
		cnt:  mqtt.NewCounter(),
		log:  m.log.With(log.Any("id", i.ID)),
	}
}

func (s *Session) init(si *Info, c *Client) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	var auth func(action, topic string) bool
	if c != nil {
		if prev := s.client; prev != nil {
			prev.close()
		}
		auth = c.authorize
		s.client = c
		c.setSession(s.info.ID, s)
	}

	return s.update(si, nil, nil, auth)
}

// * client operations

func (s *Session) delClient() {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.client = nil
	if s.info.CleanSession {
		s.close()
		s.log.Info("remove session whose cleansession flag is true when client is closed")
		return
	}
}

// * the following operations are only used by mqtt client

func (s *Session) subscribe(subs []mqtt.Subscription) error {
	if len(subs) == 0 {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	return s.update(nil, subs, nil, nil)
}

func (s *Session) unsubscribe(topics []string) (bool, error){
	if len(topics) == 0 {
		return false, nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	return len(s.info.Subscriptions) == 0, s.update(nil, nil, topics, nil)
}

// * the following operations need lock

// Push pushes source message to session queue
func (s *Session) Push(e *common.Event) error {
	s.mut.RLock()
	defer s.mut.RUnlock()

	// always flow message with qos 0 into qos0 queue
	if e.Context.QOS == 0 {
		if s.qos0 == nil {
			s.log.Warn("a message is ignored since qos0 queue is not prepared", log.Any("message", e.String()))
			e.Done()
			return nil
		}
		return s.qos0.Push(e)
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
			if s.qos1 == nil {
				s.log.Warn("a message is ignored since qos1 queue is not prepared", log.Any("message", e.String()))
				e.Done()
				return nil
			}
			return s.qos1.Push(e)
		}
	}
	if s.qos0 == nil {
		s.log.Warn("a message is ignored since qos0 queue is not prepared", log.Any("message", e.String()))
		e.Done()
		return nil
	}
	return s.qos0.Push(e)
}

func (s *Session) will() *mqtt.Message {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return s.info.WillMessage
}

func (s *Session) matchQOS(topic string) (bool, uint32) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	return mqtt.MatchTopicQOS(s.subs, topic)
}

func (s *Session) update(si *Info, add []mqtt.Subscription, del []string, auth func(action, topic string) bool) error {
	if s.info.Subscriptions == nil {
		s.info.Subscriptions = map[string]mqtt.QOS{}
	}

	if si != nil {
		s.info.ID = si.ID
		s.info.WillMessage = si.WillMessage
		s.info.CleanSession = si.CleanSession
		for topic, qos := range si.Subscriptions {
			s.subs.Set(topic, qos)
			s.mgr.exch.Bind(topic, s)
			s.info.Subscriptions[topic] = qos
		}
	}

	for _, v := range add {
		s.subs.Set(v.Topic, v.QOS)
		s.mgr.exch.Bind(v.Topic, s)
		s.info.Subscriptions[v.Topic] = v.QOS
	}

	for _, topic := range del {
		s.subs.Empty(topic)
		s.mgr.exch.Unbind(topic, s)
		delete(s.info.Subscriptions, topic)
	}

	for topic := range s.info.Subscriptions {
		if auth != nil && !auth(Subscribe, topic) {
			s.log.Warn(ErrSessionMessageTopicNotPermitted.Error(), log.Any("topic", topic))
			s.subs.Empty(topic)
			s.mgr.exch.Unbind(topic, s)
			delete(s.info.Subscriptions, topic)
		}
	}

	if len(s.info.Subscriptions) == 0 {
		s.info.Subscriptions = nil
	}

	if s.info.CleanSession {
		err := s.mgr.sessionBucket.DelKV(s.info.ID)
		if err != nil {
			s.log.Error("failed to delete session", log.Error(err))
		}
	} else {
		err := s.mgr.sessionBucket.SetKV(s.info.ID, &s.info)
		if err != nil {
			s.log.Error("failed to persist session", log.Error(err))
		}
	}

	if len(s.info.Subscriptions) != 0 {
		return s.gotoState1()
	}
	return s.gotoState0()
}

// * session states
// STATE0: no sub
// STATE1: has sub but no client, need to prepare qos0, qos1 and subs

func (s *Session) gotoState0() error{
	s.log.Info("go to state0")

	if s.qos0 != nil {
		s.qos0.Close(true)
		s.qos0 = nil
	}

	if s.qos1 != nil {
		s.qos1.Close(s.info.CleanSession)
		s.qos1 = nil
	}
	return nil
}

func (s *Session) gotoState1() error {
	s.log.Info("go to state1")

	if s.qos0 == nil {
		s.qos0 = queue.NewTemporary(s.info.ID, s.mgr.cfg.MaxInflightQOS0Messages, true)
	}

	if s.qos1 == nil {
		conf := s.mgr.cfg.Persistence.Queue
		conf.Name = s.info.ID
		conf.BatchSize = s.mgr.cfg.MaxInflightQOS1Messages
		bucket, err := s.mgr.store.NewBucket(conf.Name, new(queue.Encoder))
		if err != nil {
			s.log.Error("failed to create queue bucket", log.Error(err))
			return err
		}

		s.qos1 = queue.NewPersistence(conf, bucket)
	}
	return nil
}

func (s *Session) close() {
	s.log.Info("session is closing")
	defer s.log.Info("session has closed")

	s.mgr.exch.UnbindAll(s)
	s.mgr.sessions.delete(s.info.ID)
	s.gotoState0()

	if s.client != nil {
		s.client.close()
	}
}