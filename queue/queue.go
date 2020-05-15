package queue

import (
	"errors"

	"github.com/baetyl/baetyl-broker/common"
)

// ErrQueueClosed queue is closed
var ErrQueueClosed = errors.New("queue is closed")

// Queue interfaces
type Queue interface {
	Push(*common.Event) error
	Fetch() ([]*common.Event, error)
	Chan() <-chan *common.Event
	Close(bool) error
}
