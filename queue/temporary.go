package queue

import (
	"log"
	"sync"

	"github.com/baetyl/baetyl-broker/common"
)

// Temporary is an temporary queue in memory
type Temporary struct {
	events chan common.Event
	_put   func(common.Event) error
	quit   chan bool
	once   sync.Once
}

// NewTemporary creates a new temporary queue
func NewTemporary(capacity int, dropIfFull bool) Queue {
	q := &Temporary{
		events: make(chan common.Event, capacity),
		quit:   make(chan bool),
	}
	if dropIfFull {
		q._put = q.putDropIfFull
	} else {
		q._put = q.put
	}
	return q
}

// Get gets a message
func (q *Temporary) Get() (common.Event, error) {
	select {
	case e := <-q.events:
		return e, nil
	case <-q.quit:
		return nil, ErrQueueClosed
	}
}

// Put puts a message
func (q *Temporary) Put(e common.Event) error {
	defer e.Done()
	return q._put(e)
}

func (q *Temporary) put(e common.Event) error {
	select {
	case q.events <- e:
		return nil
	case <-q.quit:
		return ErrQueueClosed
	}
}

func (q *Temporary) putDropIfFull(e common.Event) error {
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
