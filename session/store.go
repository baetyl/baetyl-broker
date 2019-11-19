package session

import (
	"io"
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// Exchange exchange interface
type Exchange interface {
	Bind(topic string, sub common.Queue)
	Unbind(topic string, sub common.Queue)
	Route(*common.Message, func(uint64))
}

// Config session config
type Config struct {
	PersistenceDriver    string        `yaml:"persistenceDriver" json:"persistenceDriver" default:"sqlite3"`
	PersistenceLocation  string        `yaml:"persistenceLocation" json:"persistenceLocation" default:"var/lib/baetyl"`
	InflightQOS0Messages int           `yaml:"inflightQOS0Messages" json:"inflightQOS0Messages" default:"100" validate:"min=1"`
	InflightQOS1Messages int           `yaml:"inflightQOS1Messages" json:"inflightQOS1Messages" default:"10" validate:"min=1"`
	RepublishInterval    time.Duration `yaml:"republishInterval" json:"republishInterval" default:"20s"`
}

// Store the store of sessions
type Store struct {
	config   Config
	backend  *Backend
	exchange Exchange
	sessions sync.Map
	clients  sync.Map
}

// NewStore create a new session store
func NewStore(config Config, exchange Exchange) (*Store, error) {
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
		config:   config,
		backend:  backend,
		exchange: exchange,
	}
	for _, i := range items {
		ses, err := store.newSession(*i.(*Info))
		if err != nil {
			store.Close()
			return nil, err
		}
		store.sessions.Store(ses.ID(), ses)
	}
	log.Info("session store has initialized")
	return store, nil
}

// Register registers client and return its session, creates new session if not exists
func (s *Store) Register(info *Info) (*Session, bool, error) {
	if ses, ok := s.sessions.Load(info.ID); ok {
		return ses.(*Session), true, nil
	}
	ses, err := s.newSession(*info)
	if err != nil {
		return nil, false, err
	}
	s.sessions.Store(info.ID, ses)

	if info.CleanSession {
		// delete old session if clean session is true
		s.backend.Del(info)
		log.Infof("[%s] session is deleted", info.ID)
	} else {
		// store new session if clean session is false
		s.backend.Set(info)
		log.Infof("[%s] session is stored", info.ID)
	}
	return ses, false, nil
}

// Close close
func (s *Store) Close() error {
	log.Info("session store is closing")
	defer log.Info("session store has closed")

	s.clients.Range(func(_, v interface{}) bool {
		err := v.(io.Closer).Close()
		if err != nil {
			log.Warnf("failed to close client: %s", err.Error())
		}
		return true
	})

	s.sessions.Range(func(_, v interface{}) bool {
		ses := v.(*Session)
		err := ses.Close()
		if err != nil {
			log.Warnf("failed to close session (%s): %s", ses.ID(), err.Error())
		}
		return true
	})

	return s.backend.Close()
}

// NewClientMQTT creates a new MQTT client
func (s *Store) NewClientMQTT(c transport.Connection) {
	cli := &ClientMQTT{store: s, connection: c}
	cli.publisher = newPublisher(s.config.RepublishInterval, s.config.InflightQOS1Messages)
	s.clients.Store(cli, cli)
	cli.Go(cli.handling)
}
