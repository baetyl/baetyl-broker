package queue

import (
	"io"
)

// Queue interfaces
type Queue interface {
	Put(*Event) error
	Get() (*Event, error)
	io.Closer
}
