package db

import (
	"io"

	"github.com/baetyl/baetyl-broker/msg"
)

// DB the backend database
type DB interface {
	Put([]*message.Message) error
	Get(uint64, int) ([]*message.Message, error)
	Del([]uint64) error
	io.Closer
}
