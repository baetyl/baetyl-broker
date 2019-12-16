package common

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/baetyl/baetyl-go/mqtt"
)

// all errors of acknowledgement
var (
	ErrAcknowledgeCanceled = errors.New("acknowledge canceled")
	ErrAcknowledgeTimedOut = errors.New("acknowledge timed out")
)

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
func (a *acknowledge) _wait(timeout <-chan time.Time, cancel <-chan struct{}) error {
	if a.done == nil {
		return nil
	}
	select {
	case <-a.done:
		return nil
	case <-timeout:
		return ErrAcknowledgeTimedOut
	case <-cancel:
		return ErrAcknowledgeCanceled
	}
}

// Event event with message and acknowledge
type Event struct {
	*Message
	ack *acknowledge
}

// Done the event is acknowledged
func (e *Event) Done() {
	if e.ack != nil {
		e.ack._done(e.Context.ID)
	}
}

// Wait waits until acknowledged (returns true), cancelled or timed out
func (e *Event) Wait(timeout <-chan time.Time, cancel <-chan struct{}) error {
	return e.ack._wait(timeout, cancel)
}

// NewEvent creates a new event
func NewEvent(msg *Message, count int32, call func(uint64)) *Event {
	if count == 0 || call == nil {
		return &Event{Message: msg}
	}
	return &Event{
		Message: msg,
		ack: &acknowledge{
			count: count,
			call:  call,
			done:  make(chan struct{}),
		},
	}
}

// NewMessage creates a new message by packet
func NewMessage(pkt *mqtt.Publish) *Message {
	var flags uint32
	if pkt.Message.Retain {
		flags = 1
	}
	return &Message{
		Context: &Context{
			ID:    uint64(pkt.ID),
			QOS:   uint32(pkt.Message.QOS),
			Topic: pkt.Message.Topic,
			Flags: flags,
		},
		Content: pkt.Message.Payload,
	}
}

// Retain checks whether the message is need to retain
func (m *Message) Retain() bool {
	return m.Context.Flags&0x1 == 0x1
}
