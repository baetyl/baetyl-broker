package queue

import (
	"errors"
	"log"
	"sync"
)

// // QueueMemoryConfig is the configuration of in-memory queue
// type QueueMemoryConfig struct {
// 	Capacity   int
// 	DropIfFull bool
// }

// ErrQueueClosed queue is closed
var ErrQueueClosed = errors.New("queue is closed")

// QueueMemory is an in-memory queue
type QueueMemory struct {
	events chan *Event
	_put   func(*Event) error
	quit   chan bool
	once   sync.Once
}

// NewQueueMemory creates a new in-memory queue
func NewQueueMemory(capacity int, dropIfFull bool) Queue {
	q := &QueueMemory{
		events: make(chan *Event, capacity),
		quit:   make(chan bool),
	}
	if dropIfFull {
		q._put = q.putIfNotFull
	} else {
		q._put = q.put
	}
	return q
}

// Get gets a message
func (q *QueueMemory) Get() (*Event, error) {
	select {
	case e := <-q.events:
		return e, nil
	case <-q.quit:
		return nil, ErrQueueClosed
	}
}

// Put puts a message
func (q *QueueMemory) Put(e *Event) error {
	defer e.Done()
	return q._put(e)
}

func (q *QueueMemory) put(e *Event) error {
	select {
	case q.events <- e:
		return nil
	case <-q.quit:
		return ErrQueueClosed
	}
}

func (q *QueueMemory) putIfNotFull(e *Event) error {
	select {
	case q.events <- e:
		return nil
	case <-q.quit:
		return ErrQueueClosed
	default:
		log.Printf("dropped event: %s", e.message)
		return nil
	}
}

// Close closes this queue
func (q *QueueMemory) Close() error {
	q.once.Do(q.close)
	return nil
}

func (q *QueueMemory) close() {
	close(q.quit)
}
