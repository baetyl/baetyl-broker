package common

import (
	"sync/atomic"
	"time"

	"github.com/256dpi/gomqtt/packet"
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
func (a *acknowledge) _wait(timeout <-chan time.Time, cancel <-chan struct{}) bool {
	if a.done == nil {
		return true
	}
	select {
	case <-a.done:
		return true
	case <-timeout:
		return false
	case <-cancel:
		return false
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
func (e *Event) Wait(timeout <-chan time.Time, cancel <-chan struct{}) bool {
	return e.ack._wait(timeout, cancel)
}

// Packet converts event to publish packet
func (e *Event) Packet(qos QOS) *Publish {
	pkt := packet.NewPublish()
	pkt.Message.QOS = qos
	pkt.Message.Topic = e.Context.Topic
	pkt.Message.Payload = e.Content
	return pkt
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
func NewMessage(pkt *Publish) *Message {
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
