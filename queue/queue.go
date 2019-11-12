package queue

import (
	"errors"
	"io"
)

// ErrQueueClosed queue is closed
var ErrQueueClosed = errors.New("queue is closed")

// Queue interfaces
type Queue interface {
	Put(*Event) error
	Get() (*Event, error)
	io.Closer
}
