package queue

import (
	"os"
	"path"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/database"
	"github.com/gogo/protobuf/proto"
)

// Config queue config
type Config struct {
	Name     string
	Driver   string
	Location string
}

// Backend the backend database of queue
type Backend struct {
	cfg Config
	database.DB
}

// NewBackend create a new backend database
func NewBackend(cfg Config) (*Backend, error) {
	p := path.Join(cfg.Location, "queue")
	err := os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return nil, err
	}
	backend := &Backend{cfg: cfg}
	db, err := database.New(database.Conf{
		Driver: cfg.Driver,
		Source: path.Join(p, cfg.Name),
	}, backend)
	if err != nil {
		return nil, err
	}
	backend.DB = db
	return backend, nil
}

// Encode encodes the message to byte array
func (b *Backend) Encode(v interface{}) []byte {
	d, _ := proto.Marshal(v.(*common.Message))
	return d
}

// Decode decode the message from byte array
func (b *Backend) Decode(value []byte, others ...interface{}) interface{} {
	if len(others) != 1 {
		panic("the length of others must be 1")
	}
	v := new(common.Message)
	proto.Unmarshal(value, v)
	v.Context.ID = others[0].(uint64)
	return v
}

// Name returns name
func (b *Backend) Name() string {
	return b.cfg.Name
}
