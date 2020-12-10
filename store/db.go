package store

import (
	"errors"
	"io"
)

var (
	ErrDataNotFound = errors.New("no data found for this key")
)

// Factories of database
var Factories = map[string]func(conf Conf) (DB, error){}

// Conf the configuration of database
type Conf struct {
	Driver string `yaml:"driver" json:"driver" default:"pebble"`
	Path   string `yaml:"path" json:"path" default:"var/lib/baetyl/db"`
}

type DB interface {
	NewBatchBucket(name string) (BatchBucket, error)
	NewKVBucket(name string) (KVBucket, error)

	io.Closer
}

// BatchBucket the backend database
type BatchBucket interface {
	Set(offset uint64, value []byte) error
	Get(offset uint64, length int, op func([]byte, uint64) error) error
	MaxOffset() (uint64, error)
	DelBeforeID(uint64) error
	DelBeforeTS(ts uint64) error
	Close(clean bool) (err error)
}

// KVBucket the backend database
type KVBucket interface {
	SetKV(key []byte, value []byte) error
	GetKV(key []byte, op func([]byte) error) error
	DelKV(key []byte) error
	ListKV(op func([]byte) error) error
}

// New DB by given name
func New(conf Conf) (DB, error) {
	if f, ok := Factories[conf.Driver]; ok {
		return f(conf)
	}
	return nil, errors.New("database driver not found")
}
