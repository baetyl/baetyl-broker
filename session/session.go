package session

import (
	"encoding/json"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// state: session state type
type state int

// all session states
const (
	STATE0 state = iota // no sub
	STATE1              // has sub and client, need to prepare qos0, qos1, subs and dispatcher
	STATE2              // has sub but no client, need to prepare qos1 and subs
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
	info   Info
	stat   state
	mgr    *Manager
	qos0   queue.Queue // queue for qos0
	qos1   queue.Queue // queue for qos1
	client client
	subs   *mqtt.Trie
	cnt    *mqtt.Counter
	disp   *dispatcher
	log    *log.Logger
	mut    sync.RWMutex // mutex for session
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

func (s *Session) init(si *Info, c client) (err error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.updateInfo(si, nil, nil, nil)
	if err = s.updateState(); err != nil {
		return
	}
	if c != nil {
		err = s.addClient(c)
	}
	return
}

func (s *Session) gotoState0() {
	s.log.Info("go to state0")
	if s.disp != nil {
		s.disp.close()
		s.disp = nil
	}
	if s.qos0 != nil {
		s.qos0.Close(s.info.CleanSession)
		s.qos0 = nil
	}
	if s.qos1 != nil {
		s.qos1.Close(s.info.CleanSession)
		s.qos1 = nil
	}
	s.stat = STATE0
}

func (s *Session) gotoState1() error {
	s.log.Info("go to state1")
	if s.qos1 != nil {
		s.qos1.Close(s.info.CleanSession)
	}
	err := s.createQOS1()
	if err != nil {
		return err
	}
	if s.qos0 == nil {
		s.qos0 = queue.NewTemporary(s.info.ID, s.mgr.cfg.MaxInflightQOS0Messages, true)
	}
	if s.disp != nil {
		s.disp.close()
	}
	s.disp = newDispatcher(s)
	s.stat = STATE1
	return nil
}

func (s *Session) gotoState2() error {
	s.log.Info("go to state2")
	if s.disp != nil {
		s.disp.close()
		s.disp = nil
	}
	if s.qos0 != nil {
		s.qos0.Close(s.info.CleanSession)
		s.qos0 = nil
	}
	if s.qos1 == nil {
		err := s.createQOS1()
		if err != nil {
			return err
		}
	}
	s.stat = STATE2
	return nil
}

func (s *Session) createQOS1() error {
	qc := s.mgr.cfg.Persistence.Queue
	qc.Name = s.info.ID
	qc.BatchSize = s.mgr.cfg.MaxInflightQOS1Messages
	qbk, err := s.mgr.store.NewBucket(qc.Name, new(queue.Encoder))
	if err != nil {
		s.log.Error("failed to create queue bucket", log.Error(err))
		return err
	}
	s.qos1 = queue.NewPersistence(qc, qbk)
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

// * client operations

func (s *Session) addClient(c client) error {
	if prev := s.client; prev != nil {
		prev.close()
	}
	c.setSession(s.info.ID, s)
	s.client = c
	s.updateInfo(nil, nil, nil, c.authorize)
	err := s.updateState()
	return err
}

func (s *Session) delClient() {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.info.CleanSession {
		s.close()
		s.log.Info("remove session whose cleansession flag is true when all clients are closed")
		return
	}
	s.client = nil
	s.updateState()
}

// * the following operations are only used by mqtt client

func (s *Session) subscribe(subs []mqtt.Subscription) error {
	if len(subs) == 0 {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	s.updateInfo(nil, subs, nil, nil)
	return s.updateState()
}

func (s *Session) unsubscribe(topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	s.updateInfo(nil, nil, topics, nil)
	return s.updateState()
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

// Close closes session
func (s *Session) Close() {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.close()
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

func (s *Session) updateState() (err error) {
	if len(s.info.Subscriptions) == 0 {
		s.gotoState0()
	} else if s.client == nil {
		err = s.gotoState2()
	} else {
		err = s.gotoState1()
	}
	return
}

func (s *Session) updateInfo(si *Info, add []mqtt.Subscription, del []string, auth func(action, topic string) bool) {
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
}

func (s *Session) acknowledge(id uint64) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.disp == nil {
		s.log.Warn("no dispatcher")
		return
	}

	err := s.disp.delete(id)
	if err != nil {
		s.log.Warn("failed to acknowledge", log.Any("id", id), log.Error(err))
	}
}
