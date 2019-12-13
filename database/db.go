package database

import (
	"errors"
	"io"
	"time"
)

// Factories of database
var Factories = map[string]func(conf Conf, encoder Encoder) (DB, error){}

// Encoder value encode/decode interface
type Encoder interface {
	Encode(interface{}) []byte
	Decode([]byte, ...interface{}) interface{}
}

// DB the backend database
type DB interface {
	Conf() Conf

	// for queue
	Put([]interface{}) error
	Get(uint64, int) ([]interface{}, error)
	Del([]uint64) error
	DelBefore(time.Time) error

	// for kv
	SetKV(k, v interface{}) error
	GetKV(k interface{}) (v interface{}, err error)
	DelKV(k interface{}) error
	ListKV() (vs []interface{}, err error)

	io.Closer
}

// Conf the configuration of database
type Conf struct {
	Driver string
	Source string
}

// New engine by given name
func New(conf Conf, encoder Encoder) (DB, error) {
	if f, ok := Factories[conf.Driver]; ok {
		return f(conf, encoder)
	}
	return nil, errors.New("database driver not found")
}
