package session

import (
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"

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
	qos0       queue.Queue // queue for qos0
	qos1       queue.Queue // queue for qos1
	subs       *mqtt.Trie
	clis       cmap.ConcurrentMap
	resender   *resender
	sendingC   chan struct{}
	resendingC chan struct{}
	counter    *mqtt.Counter
	tomb       utils.Tomb
	log        *log.Logger
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
	return s.clis.Count()
}

func (s *Session) addClient(c client, exclusive bool) <-chan cmap.Tuple {
	var prev <-chan cmap.Tuple
	if exclusive {
		prev = s.clis.IterBuffered()
		s.clis.Remove(c.getID())
	} else if s.clis.Count() != 0 {
		s.log.Info("add new client to existing session", log.Any("cid", c.getID()))
	}
	c.setSession(s)
	s.clis.Set(c.getID(), c)
	select {
	case s.sendingC <- struct{}{}:
	default:
	}
	select {
	case s.resendingC <- struct{}{}:
	default:
	}
	return prev
}

// returns true if session should be cleaned
func (s *Session) delClient(c client) bool {
	s.clis.Remove(c.getID())
	return s.CleanSession && s.clis.Count() == 0
}

// Close closes session
func (s *Session) close(location string) {
	s.Do(func() {
		s.log.Info("session is closing")
		defer s.log.Info("session has closed")
		s.tomb.Kill(nil)
		err := s.qos0.Close()
		if err != nil {
			s.log.Warn("failed to close qos0 queue", log.Error(err))
		}
		err = s.qos1.Close()
		if err != nil {
			s.log.Warn("failed to close qos1 queue", log.Error(err))
		}
		if s.CleanSession {
			// delete persistent queue data if CleanSession=true
			qname := utils.CalculateBase64(s.ID)
			p := path.Join(location, "queue", qname)
			if utils.FileExists(p) {
				os.RemoveAll(p)
				s.log.Info("queue data is deleted", log.Any("queue name", qname))
			}
		}
	})
	s.tomb.Wait()
}

func (s *Session) sending() error {
	s.log.Info("session starts to send messages")
	defer s.log.Info("session has stopped sending messages")

	qos0 := s.qos0.Chan()
	qos1 := s.qos1.Chan()
	resender := s.resender
	var iqel *iqel
	for {
		if s.clis.Count() == 0 {
			select {
			case <-s.sendingC:
			case <-s.tomb.Dying():
				return nil
			}
		}
		for c := range s.clis.IterBuffered() {
			client := c.Val.(client)
			if iqel != nil {
				if err := client.sending(iqel); err != nil {
					continue
				}
				if iqel.qos == 1 {
					select {
					case resender.c <- iqel:
					case <-s.tomb.Dying():
						return nil
					}
				}
			}
			select {
			case evt := <-qos0:
				if ent := s.log.Check(log.DebugLevel, "queue popped a message as qos 0"); ent != nil {
					ent.Write(log.Any("message", evt.String()))
				}
				iqel = newIQEL(0, 0, evt)
			case evt := <-qos1:
				if ent := s.log.Check(log.DebugLevel, "queue popped a message as qos 1"); ent != nil {
					ent.Write(log.Any("message", evt.String()))
				}
				iqel = newIQEL(uint16(s.counter.NextID()), 1, evt)
				if err := resender.store(iqel); err != nil {
					s.log.Error(err.Error())
				}
			case <-s.tomb.Dying():
				return nil
			}
		}
	}
}

func (s *Session) resending() error {
	s.log.Info("session starts to resend messages", log.Any("interval", s.resender.d))
	defer s.log.Info("session has stopped resending messages")

	var iqel *iqel
	timer := time.NewTimer(s.resender.d)
	r := s.resender
	defer timer.Stop()
	for {
		if s.clis.Count() == 0 {
			select {
			case <-s.resendingC:
			case <-s.tomb.Dying():
				return nil
			}
		}
	I:
		for c := range s.clis.IterBuffered() {
			client := c.Val.(client)
			if iqel != nil {
				select {
				case <-timer.C:
				default:
				}
				for timer.Reset(r.next(iqel)); iqel.wait(timer.C, r.t.Dying()) == common.ErrAcknowledgeTimedOut; timer.Reset(r.d) {
					if err := client.resending(iqel); err != nil {
						continue I
					}
				}
			}
			select {
			case iqel = <-r.c:
			case <-s.tomb.Dying():
				return nil
			}
		}
	}
}
