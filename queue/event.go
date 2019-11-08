package queue

import (
	"sync/atomic"

	"github.com/baetyl/baetyl-broker/msg"
)

// Event message event with acknowledgement
type Event struct {
	message *message.Message
	count   int32
	call    func(uint64)
	done    chan struct{}
}

// NewEvent creates a new event
func NewEvent(message *message.Message, count int32, call func(uint64)) *Event {
	e := &Event{
		message: message,
		count:   count,
		call:    call,
	}
	if count > 0 {
		e.done = make(chan struct{})
	}
	return e
}

// Done acknowledges once after event is handled
func (e *Event) Done() {
	if atomic.AddInt32(&e.count, -1) == 0 {
		if e.call != nil {
			e.call(e.message.GetContext().ID)
		}
		close(e.done)
	}
}

// Wait waits until acknowledged or cancelled
func (e *Event) Wait(cancel <-chan struct{}) bool {
	if e.done == nil {
		return true
	}
	select {
	case <-cancel:
		return false
	case <-e.done:
		return true
	}
}

func (e *Event) String() string {
	return e.message.String()
}
