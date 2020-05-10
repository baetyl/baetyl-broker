package database

import (
	"errors"
	"io"
	"time"
)

// Factories of database
var Factories = map[string]func(conf Conf) (DB, error){}

// Conf the configuration of database
type Conf struct {
	Driver string `yaml:"driver" json:"driver" default:"boltdb"`
	Source string `yaml:"source" json:"source" default:"var/lib/baetyl"`
}

// Encoder value encode/decode interface
type Encoder interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte, value interface{}, args ...interface{}) error
}

type DB interface {
	NewBucket(name string, encoder Encoder) (Bucket, error)
	io.Closer
}

// Bucket the backend database
type Bucket interface {
	Put([]interface{}) error
	Get(offset uint64, length int, results interface{}) error
	Del([]uint64) error
	DelBefore(time.Time) error

	SetKV(k string, v interface{}) error
	GetKV(k string, result interface{}) error
	DelKV(k string) error
	ListKV(results interface{}) error

	Close(clean bool) (err error)
}

// New DB by given name
func New(conf Conf) (DB, error) {
	if f, ok := Factories[conf.Driver]; ok {
		return f(conf)
	}
	return nil, errors.New("database driver not found")
}
