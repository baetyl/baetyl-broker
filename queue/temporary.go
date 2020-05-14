package queue

import (
	"sync"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
)

// Temporary is an temporary queue in memory
type Temporary struct {
	events   []*common.Event
	push     func(*common.Event) error
	quit     chan bool
	eget     chan bool // get events
	capacity int
	log      *log.Logger
	sync.Once
}

// NewTemporary creates a new temporary queue
func NewTemporary(id string, capacity int, dropIfFull bool) Queue {
	q := &Temporary{
		events:   make([]*common.Event, 0),
		quit:     make(chan bool),
		eget:     make(chan bool, 1),
		capacity: capacity,
		log:      log.With(log.Any("queue", "temp"), log.Any("id", id)),
	}
	if dropIfFull {
		q.push = q.putOrDrop
	} else {
		q.push = q.put
	}
	return q
}

// Chan returns message channel
func (q *Temporary) Chan() <-chan bool {
	return q.eget
}

// Pop pops messages from queue
func (q *Temporary) Pop() ([]*common.Event, error) {
	events := q.events[0:]
	q.events = q.events[len(q.events):]
	return events, nil
}

// Push pushes a message to queue
func (q *Temporary) Push(e *common.Event) error {
	defer e.Done()
	defer q.trigger()
	return q.push(e)
}

func (q *Temporary) put(e *common.Event) error {
	q.events = append(q.events, e)
	return nil
}

func (q *Temporary) putOrDrop(e *common.Event) error {
	if len(q.events) == q.capacity {
		if ent := q.log.Check(log.DebugLevel, "queue dropped a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
		return nil
	}
	q.events = append(q.events, e)
	if ent := q.log.Check(log.DebugLevel, "queue pushed a message"); ent != nil {
		ent.Write(log.Any("message", e.String()))
	}
	return nil
}

// Close closes this queue
func (q *Temporary) Close(_ bool) error {
	q.log.Info("queue is closing")
	defer q.log.Info("queue has closed")

	q.Do(func() {
		close(q.quit)
	})
	return nil
}

// triggers an event to get messages from queue in batch mode
func (q *Temporary) trigger() {
	select {
	case q.eget <- true:
	default:
	}
}
