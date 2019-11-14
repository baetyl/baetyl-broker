package database

import (
	"errors"
	"io"
)

// Factories of database
var Factories = map[string]func(conf Conf, encode Encode) (DB, error){}

// Encode value encode/decode interface
type Encode interface {
	Encode(interface{}) []byte
	Decode(uint64, []byte) interface{}
}

// DB the backend database
type DB interface {
	Conf() Conf

	Put([]interface{}) error
	Get(uint64, int) ([]interface{}, error)
	Del([]uint64) error

	PutKV(k, v []byte) error
	GetKV(k []byte) (v []byte, err error)
	DelKV(k []byte) error
	ListKV() (vs [][]byte, err error)

	io.Closer
}

// Conf the configuration of database
type Conf struct {
	Driver string
	Source string
	Path   string
}

// New engine by given name
func New(conf Conf, encode Encode) (DB, error) {
	if f, ok := Factories[conf.Driver]; ok {
		return f(conf, encode)
	}
	return nil, errors.New("no such kind database")
}
