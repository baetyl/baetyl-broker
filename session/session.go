package session

import (
	"encoding/json"
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

func (i *Info) String() string {
	d, _ := json.Marshal(i)
	return string(d)
}

// Session session of a client
type Session struct {
	Info
	subs *common.Trie
	qos0 queue.Queue // queue for qos0
	qos1 queue.Queue // queue for qos1
	sync.Once
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

// Close closes session
func (s *Session) Close() error {
	s.Do(func() {
		log.Infof("session (%s) is closing", s.ID)
		defer log.Infof("session (%s) has closed", s.ID)
		err := s.qos0.Close()
		if err != nil {
			log.Warn("failed to close qos0 queue", err)
		}
		err = s.qos1.Close()
		if err != nil {
			log.Warn("failed to close qos1 queue", err)
		}
	})
	return nil
}
