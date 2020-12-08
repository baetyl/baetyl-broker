package queue

import (
	"errors"

	"github.com/baetyl/baetyl-broker/v2/common"
)

// ErrQueueClosed queue is closed
var ErrQueueClosed = errors.New("queue is closed")

// Queue interfaces
type Queue interface {
	Push(*common.Event) error
	Chan() <-chan *common.Event
	Disable()
	Close(bool) error
}
