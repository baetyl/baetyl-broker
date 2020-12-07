package queue

import (
	"sync"

	"github.com/baetyl/baetyl-go/v2/log"

	"github.com/baetyl/baetyl-broker/v2/common"
)

// Temporary is an temporary queue in memory
type Temporary struct {
	events chan *common.Event
	push   func(*common.Event) error
	quit   chan bool
	log    *log.Logger
	sync.Once
}

// NewTemporary creates a new temporary queue
func NewTemporary(id string, capacity int, dropIfFull bool) Queue {
	q := &Temporary{
		events: make(chan *common.Event, capacity),
		quit:   make(chan bool),
		log:    log.With(log.Any("queue", "temp"), log.Any("id", id)),
	}
	if dropIfFull {
		q.push = q.putOrDrop
	} else {
		q.push = q.put
	}
	return q
}

// Chan returns message channel
func (q *Temporary) Chan() <-chan *common.Event {
	return q.events
}

// Pop pops a message from queue
func (q *Temporary) Pop() (*common.Event, error) {
	select {
	case e := <-q.events:
		return e, nil
	case <-q.quit:
		return nil, ErrQueueClosed
	}
}

// Push pushes a message to queue
func (q *Temporary) Push(e *common.Event) error {
	defer e.Done()
	return q.push(e)
}

func (q *Temporary) put(e *common.Event) error {
	select {
	case q.events <- e:
		return nil
	case <-q.quit:
		return ErrQueueClosed
	}
}

func (q *Temporary) putOrDrop(e *common.Event) error {
	select {
	case q.events <- e:
		if ent := q.log.Check(log.DebugLevel, "queue pushed a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
		return nil
	case <-q.quit:
		return ErrQueueClosed
	default:
		if ent := q.log.Check(log.DebugLevel, "queue dropped a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
		return nil
	}
}

// Close closes this queue
func (q *Temporary) Close(_ bool) error {
	q.log.Info("queue is closing")
	defer q.log.Info("queue has closed")
	return nil
}
