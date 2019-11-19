package queue

import (
	"log"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
)

// Temporary is an temporary queue in memory
type Temporary struct {
	events chan *common.Event
	push   func(*common.Event) error
	quit   chan bool
	once   sync.Once
}

// NewTemporary creates a new temporary queue
func NewTemporary(capacity int, dropIfFull bool) Queue {
	q := &Temporary{
		events: make(chan *common.Event, capacity),
		quit:   make(chan bool),
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
		return nil
	case <-q.quit:
		return ErrQueueClosed
	default:
		log.Printf("dropped event: %s", e.String())
		return nil
	}
}

// Close closes this queue
func (q *Temporary) Close() error {
	q.once.Do(func() {
		close(q.quit)
	})
	return nil
}
