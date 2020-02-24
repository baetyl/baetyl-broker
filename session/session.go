package session

import (
	"encoding/json"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
)

// state: session state type
type state int

// all session states
const (
	STATE0 state = iota // no sub
	STATE1              // has sub and client, need to prepare qos0, qos1, subs and dispatcher
	STATE2              // has sub but no client, need to prepare qos1 and subs
)

// kind: session kind
type kind string

//  all session kinds
const (
	MQTT kind = "mqtt"
	LINK kind = "link"
)

// Info session information
type Info struct {
	ID            string              `json:"id,omitempty"`
	Kind          kind                `json:"kind,omitempty"`
	WillMessage   *link.Message       `json:"will,omitempty"`
	Subscriptions map[string]mqtt.QOS `json:"subs,omitempty"`
	CleanSession  bool                `json:"-"`
}

func (i *Info) String() string {
	d, _ := json.Marshal(i)
	return string(d)
}

// Session session of a client
type Session struct {
	Info
	stat state
	mgr  *Manager
	qos0 queue.Queue // queue for qos0
	qos1 queue.Queue // queue for qos1
	subs *mqtt.Trie
	clis map[string]client
	cnt  *mqtt.Counter
	disp *dispatcher
	log  *log.Logger
	muts sync.RWMutex // mutex for session
	mutc sync.RWMutex // mutex for clients
	once sync.Once
}

func newSession(i *Info, m *Manager) *Session {
	m.checkSubscriptions(i)
	return &Session{
		Info: *i,
		mgr:  m,
		cnt:  mqtt.NewCounter(),
		clis: map[string]client{},
		log:  m.log.With(log.Any("id", i.ID)),
	}
}

func (s *Session) gotoState0() {
	s.log.Info("go to state0")
	if s.disp != nil {
		s.disp.close()
		s.disp = nil
	}
	if s.qos0 != nil {
		s.qos0.Close(s.CleanSession)
		s.qos0 = nil
	}
	if s.qos1 != nil {
		s.qos1.Close(s.CleanSession)
		s.qos1 = nil
	}
	if s.subs != nil {
		s.subs = nil
	}
	s.stat = STATE0
}

func (s *Session) gotoState1() error {
	s.log.Info("go to state1")
	err := s.createQOS1()
	if err != nil {
		return err
	}
	if s.qos0 == nil {
		s.qos0 = queue.NewTemporary(s.ID, s.mgr.cfg.MaxInflightQOS0Messages, true)
	}
	if s.subs == nil {
		s.subs = mqtt.NewTrie()
	}
	for topic, qos := range s.Subscriptions {
		s.subs.Set(topic, qos)
	}
	if s.disp == nil {
		s.disp = newDispatcher(s)
	}
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
		s.qos0.Close(s.CleanSession)
		s.qos0 = nil
	}
	err := s.createQOS1()
	if err != nil {
		return err
	}
	if s.subs == nil {
		s.subs = mqtt.NewTrie()
	}
	for topic, qos := range s.Subscriptions {
		s.subs.Set(topic, qos)
	}
	s.stat = STATE2
	return nil
}

func (s *Session) createQOS1() error {
	if s.qos1 != nil {
		return nil
	}
	qc := s.mgr.cfg.Persistence
	qc.Name = utils.CalculateBase64(s.ID)
	qc.BatchSize = s.mgr.cfg.MaxInflightQOS1Messages
	qbk, err := queue.NewBackend(qc)
	if err != nil {
		s.log.Error("failed to create queue backend", log.Error(err))
		return err
	}
	s.qos1 = queue.NewPersistence(qc, qbk)
	return nil
}

func (s *Session) close() {
	s.log.Info("session is closing")
	defer s.log.Info("session has closed")
	s.gotoState0()
	for _, c := range s.copyClients() {
		c.close()
	}
}

// * the following operations need lock

// Push pushes source message to session queue
func (s *Session) Push(e *common.Event) error {
	s.muts.RLock()
	defer s.muts.RUnlock()

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
	if s.subs == nil {
		s.log.Warn("a message is ignored since subs tree is not prepared", log.Any("message", e.String()))
		e.Done()
		return nil
	}
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

func (s *Session) matchQOS(topic string) (bool, uint32) {
	s.muts.RLock()
	defer s.muts.RUnlock()

	return mqtt.MatchTopicQOS(s.subs, topic)
}

//  only used by mqtt client
func (s *Session) update(si *Info) {
	s.muts.Lock()
	defer s.muts.Unlock()

	s.ID = si.ID
	s.Kind = si.Kind
	s.WillMessage = si.WillMessage
	s.CleanSession = si.CleanSession
	for k, v := range si.Subscriptions {
		s.Subscriptions[k] = v
	}
}

func (s *Session) countClients() int {
	s.mutc.RLock()
	defer s.mutc.RUnlock()

	return len(s.clis)
}

func (s *Session) copyClients() map[string]client {
	s.mutc.RLock()
	defer s.mutc.RUnlock()

	res := map[string]client{}
	for k, v := range s.clis {
		res[k] = v
	}
	return res
}

func (s *Session) emptyClients() map[string]client {
	s.mutc.Lock()
	defer s.mutc.Unlock()

	res := map[string]client{}
	for k, v := range s.clis {
		res[k] = v
	}
	s.clis = map[string]client{}
	return res
}

func (s *Session) addClient(c client, exclusive bool) error {
	s.muts.Lock()
	defer s.muts.Unlock()

	if exclusive {
		for _, p := range s.emptyClients() {
			p.close()
		}
	} else if s.countClients() != 0 {
		// check limit
		if s.mgr.cfg.MaxClientsPerSession > 0 && s.countClients() >= s.mgr.cfg.MaxClientsPerSession {
			s.log.Error(ErrSessionClientNumberExceeds.Error(), log.Any("max", s.mgr.cfg.MaxClientsPerSession))
			return ErrSessionClientNumberExceeds
		}
		s.log.Info("add new client to existing session", log.Any("cid", c.getID()))
	}
	c.setSession(s)
	s.mutc.Lock()
	s.clis[c.getID()] = c
	s.mutc.Unlock()

	for topic := range s.Subscriptions {
		// Re-check subscriptions, if topic not permit, log error, delete and skip
		if !c.authorize(Subscribe, topic) {
			s.log.Warn(ErrSessionMessageTopicNotPermitted.Error(), log.Any("topic", topic))
			delete(s.Subscriptions, topic)
			s.subs.Empty(topic)
			s.mgr.exch.Unbind(topic, s)
		}
	}

	if !s.CleanSession {
		err := s.mgr.sstore.Set(&s.Info)
		if err != nil {
			return err
		}
	}

	if len(s.Subscriptions) == 0 {
		s.gotoState0()
	} else {
		err := s.gotoState1()
		if err != nil {
			return err
		}
	}
	return nil
}

// returns true if session should be cleaned
func (s *Session) delClient(c client) {
	s.muts.Lock()
	defer s.muts.Unlock()

	s.mutc.Lock()
	delete(s.clis, c.getID())
	s.mutc.Unlock()

	if s.CleanSession && s.countClients() == 0 {
		s.log.Info("remove session whose cleansession flag is true when all clients are closed")
		s.mgr.removeSession(s)
		s.close()
		return
	}

	var err error
	if len(s.Subscriptions) == 0 {
		s.gotoState0()
	} else if s.countClients() == 0 {
		err = s.gotoState2()
	} else {
		err = s.gotoState1()
	}
	if err != nil {
		s.log.Error("failed to switch state during disconnect, to close session", log.Error(err))
		s.mgr.removeSession(s)
		s.close()
	}
	return
}

func (s *Session) acknowledge(id uint64) {
	s.muts.RLock()
	defer s.muts.RUnlock()

	if s.disp == nil {
		s.log.Warn("no dispatcher")
		return
	}

	err := s.disp.delete(id)
	if err != nil {
		s.log.Warn("failed to acknowledge", log.Any("id", id), log.Error(err))
	}
}

// Close closes session
func (s *Session) Close() {
	s.muts.Lock()
	defer s.muts.Unlock()

	s.close()
}

// * the following operations are only used by mqtt client

func (s *Session) subscribe(subs []mqtt.Subscription) error {
	if len(subs) == 0 {
		return nil
	}

	s.muts.Lock()
	defer s.muts.Unlock()

	if s.Subscriptions == nil {
		s.Subscriptions = map[string]mqtt.QOS{}
	}
	for _, sub := range subs {
		s.Subscriptions[sub.Topic] = sub.QOS
	}
	if !s.CleanSession {
		err := s.mgr.sstore.Set(&s.Info)
		if err != nil {
			s.log.Error("failed to switch state during subscribe, to close session", log.Error(err))
			s.mgr.removeSession(s)
			s.close()
			return err
		}
	}

	err := s.gotoState1()
	if err != nil {
		s.log.Error("failed to switch state during subscribe, to close session", log.Error(err))
		s.mgr.removeSession(s)
		s.close()
		return err
	}

	// bind session to exchage after resources are prepared
	for topic := range s.Subscriptions {
		s.mgr.exch.Bind(topic, s)
	}
	return nil
}

func (s *Session) unsubscribe(topics []string) error {
	if len(topics) == 0 {
		return nil
	}

	s.muts.Lock()
	defer s.muts.Unlock()

	for _, t := range topics {
		delete(s.Subscriptions, t)
		s.subs.Empty(t)
		// unbind session from exchange before resources are closed
		s.mgr.exch.Unbind(t, s)
	}
	if !s.CleanSession {
		err := s.mgr.sstore.Set(&s.Info)
		if err != nil {
			s.log.Error("failed to switch state during unsubscribe, to close session", log.Error(err))
			s.mgr.removeSession(s)
			s.close()
			return err
		}
	}

	if len(s.Subscriptions) == 0 {
		s.gotoState0()
	} else {
		err := s.gotoState1()
		if err != nil {
			s.log.Error("failed to switch state during unsubscribe, to close session", log.Error(err))
			s.mgr.removeSession(s)
			s.close()
			return err
		}
	}
	return nil
}
