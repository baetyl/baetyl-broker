package queue

import (
	"log"
	"sync"
)

// Memory is an in-memory queue
type Memory struct {
	events chan *Event
	_put   func(*Event) error
	quit   chan bool
	once   sync.Once
}

// NewMemory creates a new in-memory queue
func NewMemory(capacity int, dropIfFull bool) Queue {
	q := &Memory{
		events: make(chan *Event, capacity),
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
func (q *Memory) Get() (*Event, error) {
	select {
	case e := <-q.events:
		return e, nil
	case <-q.quit:
		return nil, ErrQueueClosed
	}
}

// Put puts a message
func (q *Memory) Put(e *Event) error {
	defer e.Done()
	return q._put(e)
}

func (q *Memory) put(e *Event) error {
	select {
	case q.events <- e:
		return nil
	case <-q.quit:
		return ErrQueueClosed
	}
}

func (q *Memory) putDropIfFull(e *Event) error {
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
func (q *Memory) Close() error {
	q.once.Do(q.close)
	return nil
}

func (q *Memory) close() {
	close(q.quit)
}
