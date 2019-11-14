package common

import (
	"sync/atomic"
)

// Event interface
type Event interface {
	Done()
	String() string
	Message() *Message
}

type acknowledge struct {
	count int32
	call  func(uint64)
	done  chan struct{}
}

// Done acknowledges once after event is handled
func (a *acknowledge) _done(id uint64) {
	if atomic.AddInt32(&a.count, -1) == 0 {
		if a.call != nil {
			a.call(id)
		}
		close(a.done)
	}
}

// Wait waits until acknowledged or cancelled
func (a *acknowledge) _wait(cancel <-chan struct{}) bool {
	if a.done == nil {
		return true
	}
	select {
	case <-cancel:
		return false
	case <-a.done:
		return true
	}
}

type event struct {
	msg *Message
	ack *acknowledge
}

func (e *event) Done() {
	if e.ack != nil {
		e.ack._done(e.msg.Context.ID)
	}
}

func (e *event) String() string {
	return e.msg.String()
}

func (e *event) Message() *Message {
	return e.msg
}

// NewEvent creates a new event
func NewEvent(msg *Message, count int32, call func(uint64)) Event {
	if count == 0 || call == nil {
		return &event{msg: msg}
	}
	return &event{
		msg: msg,
		ack: &acknowledge{
			count: count,
			call:  call,
			done:  make(chan struct{}),
		},
	}
}
