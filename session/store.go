package session

import (
	"io"
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// the client has not bound the session will be stored under this key in map
const sessionless = "~!@#$%^&*()_sessionless"

// Exchange exchange interface
type Exchange interface {
	Bind(topic string, sub common.Queue)
	Unbind(topic string, sub common.Queue)
	UnbindAll(sub common.Queue)
	Route(*common.Message, func(uint64))
}

type client interface {
	setSession(*Session)
	io.Closer
}

// Config session config
type Config struct {
	PersistenceDriver       string        `yaml:"persistenceDriver" json:"persistenceDriver" default:"sqlite3"`
	PersistenceLocation     string        `yaml:"persistenceLocation" json:"persistenceLocation" default:"var/lib/baetyl"`
	MaxInflightQOS0Messages int           `yaml:"maxInflightQOS0Messages" json:"maxInflightQOS0Messages" default:"100" validate:"min=1"`
	MaxInflightQOS1Messages int           `yaml:"maxInflightQOS1Messages" json:"maxInflightQOS1Messages" default:"20" validate:"min=1"`
	MaxConnections          int           `yaml:"maxConnections" json:"maxConnections" default:"100" validate:"min=1"`
	RepublishInterval       time.Duration `yaml:"republishInterval" json:"republishInterval" default:"20s"`
}

// Store the store of sessions
type Store struct {
	auth     *auth.Auth
	config   Config
	backend  *Backend
	exchange Exchange
	sessions map[string]*Session
	clients  map[string]map[client]client
	sync.Mutex
}

// NewStore create a new session store
func NewStore(config Config, exchange Exchange, auth *auth.Auth) (*Store, error) {
	backend, err := NewSessionBackend(config)
	if err != nil {
		return nil, err
	}
	// load stored sessions from backend database
	items, err := backend.List()
	if err != nil {
		backend.Close()
		return nil, err
	}
	store := &Store{
		auth:     auth,
		config:   config,
		backend:  backend,
		exchange: exchange,
		sessions: map[string]*Session{},
		clients:  map[string]map[client]client{},
	}
	// store all clients without session under sessionless key in map
	store.clients[sessionless] = map[client]client{}
	for _, i := range items {
		info := i.(*Info)
		ses, err := store.newSession(info)
		if err != nil {
			store.Close()
			return nil, err
		}
		store.sessions[ses.ID] = ses
		for topic, qos := range info.Subscriptions {
			ses.subs.Set(topic, qos)
			exchange.Bind(topic, ses)
			// TODO: it is unnecessary to subscribe messages with qos 0 for the session without any client
		}
	}
	log.Info("session store has initialized")
	return store, nil
}

// Close close
func (s *Store) Close() error {
	s.Lock()
	defer s.Unlock()

	log.Info("session store is closing")
	defer log.Info("session store has closed")

	for _, some := range s.clients {
		for _, cli := range some {
			cli.Close()
		}
	}

	for _, ses := range s.sessions {
		ses.Close()
	}

	return s.backend.Close()
}

// NewClientMQTT creates a new MQTT client
func (s *Store) NewClientMQTT(c transport.Connection, anonymous bool) {
	cli := &ClientMQTT{
		store:      s,
		connection: c,
		anonymous:  anonymous,
		publisher:  newPublisher(s.config.RepublishInterval, s.config.MaxInflightQOS1Messages),
	}
	s.Lock()
	// session will be bound after connect packet is handled
	s.clients[sessionless][cli] = cli
	s.Unlock()
	cli.Go(cli.receiving)
}

// delClient deletes client and clean session if needs
func (s *Store) delClient(ses *Session, cli client) {
	cli.Close()

	s.Lock()
	defer s.Unlock()

	key := sessionless
	if ses != nil {
		key = ses.ID
	}
	some, ok := s.clients[key]
	if ok {
		delete(some, cli)
	}

	// close session if not bound and CleanSession=true
	if ses != nil && ses.CleanSession && len(some) == 0 {
		s.backend.Del(ses.ID)
		s.exchange.UnbindAll(ses)
		ses.Close()
		delete(s.sessions, ses.ID)
	}
}

// initSession init session for client, creates new session if not exists
func (s *Store) initSession(info *Info, cli client, unique bool) (bool, error) {
	s.Lock()
	defer s.Unlock()

	if ses, ok := s.sessions[info.ID]; ok {
		if unique {
			// close previous clients with the same client id
			for _, prev := range s.clients[ses.ID] {
				prev.Close()
				delete(s.clients[ses.ID], prev)
			}
		}
		cli.setSession(ses)
		// move client from sessionless to ses.ID
		delete(s.clients[sessionless], cli)
		if _, ok := s.clients[ses.ID]; !ok {
			s.clients[ses.ID] = map[client]client{}
		}
		s.clients[ses.ID][cli] = cli
		ses.Info.CleanSession = info.CleanSession // update CleanSession
		if info.CleanSession {
			// delete session from backend if clean session is true
			s.backend.Del(ses.ID)
			log.Infof("session (%s) is stored", ses.ID)
		}
		return true, nil
	}
	ses, err := s.newSession(info)
	if err != nil {
		return false, err
	}
	cli.setSession(ses)
	s.sessions[ses.ID] = ses
	if _, ok := s.clients[ses.ID]; !ok {
		s.clients[ses.ID] = map[client]client{}
	}
	s.clients[ses.ID][cli] = cli
	if !info.CleanSession {
		// set new session into backend if clean session is false
		s.backend.Set(info)
		log.Infof("session (%s) is stored", ses.ID)
	}
	return false, nil
}

func (s *Store) newSession(info *Info) (*Session, error) {
	backend, err := s.backend.NewQueueBackend(info.ID)
	if err != nil {
		log.Errorf("failed to create session (%s): %s", info.ID, err)
		return nil, err
	}
	defer log.Infof("session (%s) is created", info.ID)
	return &Session{
		Info: *info,
		subs: common.NewTrie(),
		qos0: queue.NewTemporary(s.config.MaxInflightQOS0Messages, true),
		qos1: queue.NewPersistence(s.config.MaxInflightQOS1Messages, backend),
	}, nil
}

// Subscribe subscribes topics
func (s *Store) subscribe(ses *Session, subs []common.Subscription) error {
	s.Lock()
	defer s.Unlock()

	if ses.Subscriptions == nil {
		ses.Subscriptions = map[string]common.QOS{}
	}
	for _, sub := range subs {
		ses.Subscriptions[sub.Topic] = sub.QOS
		ses.subs.Set(sub.Topic, sub.QOS)
		s.exchange.Bind(sub.Topic, ses)
	}
	if !ses.CleanSession {
		return s.backend.Set(&ses.Info)
	}
	return nil
}

// Unsubscribe unsubscribes topics
func (s *Store) unsubscribe(ses *Session, topics []string) error {
	s.Lock()
	defer s.Unlock()

	for _, t := range topics {
		delete(ses.Subscriptions, t)
		ses.subs.Empty(t)
		s.exchange.Unbind(t, ses)
	}
	if !ses.CleanSession {
		return s.backend.Set(&ses.Info)
	}
	return nil
}
