package queue

import (
	"errors"
	"io"

	"github.com/baetyl/baetyl-broker/common"
)

// ErrQueueClosed queue is closed
var ErrQueueClosed = errors.New("queue is closed")

// Queue interfaces
type Queue interface {
	Put(common.Event) error
	Get() (common.Event, error)
	io.Closer
}
