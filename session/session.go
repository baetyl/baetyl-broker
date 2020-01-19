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
	cmap "github.com/orcaman/concurrent-map"
)

// Info session information
type Info struct {
	ID            string
	Will          *link.Message `json:"Will,omitempty"` // will message
	CleanSession  bool
	Subscriptions map[string]mqtt.QOS
}

func (i *Info) String() string {
	d, _ := json.Marshal(i)
	return string(d)
}

// Session session of a client
type Session struct {
	Info
	qos0     queue.Queue // queue for qos0
	qos1     queue.Queue // queue for qos1
	subs     *mqtt.Trie
	clis     cmap.ConcurrentMap
	resender chan *iqel
	evt      *common.Event
	tomb     utils.Tomb
	log      *log.Logger
	mu       sync.Mutex
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
		panic("At least one subscription matched")
	}
	for _, q := range qs {
		if q.(mqtt.QOS) > 0 {
			return s.qos1.Push(e)
		}
	}
	return s.qos0.Push(e)
}

func (s *Session) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clis)
}

func (s *Session) addClient(c client, exclusive bool) cmap.ConcurrentMap {
	s.mu.Lock()
	defer s.mu.Unlock()
	var prev cmap.ConcurrentMap
	if exclusive {
		prev = s.clis
		s.clis = cmap.New()
	} else if len(s.clis) != 0 {
		s.log.Info("add new client to existing session", log.Any("cid", c.getID()))
	}
	c.setSession(s)
	c.setResender(s.resender)
	s.clis.Set(c.getID(), c)
	//if s.clis.Count() == 1 {
	//	s.tomb.Go(s.sending, s.resending)
	//}
	return prev
}

// returns true if session should be cleaned
func (s *Session) delClient(c client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clis.Remove(c.getID())
	return s.CleanSession && s.clis.Count() == 0
}

// Close closes session
func (s *Session) close() {
	s.Do(func() {
		s.log.Info("session is closing")
		defer s.log.Info("session has closed")
		err := s.qos0.Close()
		if err != nil {
			s.log.Warn("failed to close qos0 queue", log.Error(err))
		}
		err = s.qos1.Close()
		if err != nil {
			s.log.Warn("failed to close qos1 queue", log.Error(err))
		}
	})
}

func (s *Session) sending() error {
	s.log.Info("session starts to send messages")
	defer s.log.Info("session has stopped sending messages")

	qos0 := s.qos0.Chan()
	qos1 := s.qos1.Chan()
	for {
		if s.clis.Count() == 0 {
			return nil
		}
		for c := range s.clis.IterBuffered() {
			client := c.Val.(client)
			if s.evt != nil {
				if err := client.sending(s.evt); err != nil {
					continue
				}
			}
			select {
			case s.evt = <-qos0:
			case s.evt = <-qos1:
			case <-s.tomb.Dying():
				return nil
			}
		}
	}
}

func (s *Session) resending() error {
	s.log.Info("session starts to sending messages")
	defer s.log.Info("session has stopped resending messages")
	return nil
	//var _iqel *iqel
	//for {
	//	for c := range s.clis.IterBuffered() {
	//		client := c.Val.(client)
	//		fmt.Println("+++++++++++++ 1 resending: ", client, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//		if _iqel != nil {
	//			fmt.Println("+++++++++++++ 2 resending: ", client, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//			if err := client.resending(_iqel); err == nil {
	//				fmt.Println("+++++++++++++ 3 resending: ", client, err, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//				continue
	//			}
	//		}
	//		select {
	//		case _iqel = <-s.resender:
	//			fmt.Println("+++++++++++++ 4 resending: ", client, _iqel, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//			if err := client.resending(_iqel); err == nil {
	//				fmt.Println("+++++++++++++ 5 resending: ", client, _iqel, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//				_iqel = nil
	//			}
	//		case <-s.tomb.Dying():
	//			fmt.Println("+++++++++++++ 6 resending: ", client, _iqel, " ses:", client.getSession().ID, " count:", s.clis.Count())
	//			return nil
	//		}
	//	}
	//}
}
