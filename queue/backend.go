package queue

import (
	"os"
	"path"
	"time"

	"github.com/baetyl/baetyl-broker/database"
	"github.com/baetyl/baetyl-go/link"
	"github.com/gogo/protobuf/proto"
)

// Config queue config
type Config struct {
	Name          string        `yaml:"name" json:"name"`
	Driver        string        `yaml:"driver" json:"driver" default:"sqlite3"`
	Location      string        `yaml:"location" json:"location" default:"var/lib/baetyl"`
	BatchSize     int           `yaml:"batchSize" json:"batchSize" default:"10"`
	ExpireTime    time.Duration `yaml:"expireTime" json:"expireTime" default:"168h"`
	CleanInterval time.Duration `yaml:"cleanInterval" json:"cleanInterval" default:"1h"`
	WriteTimeout  time.Duration `yaml:"writeTimeout" json:"writeTimeout" default:"100ms"`
	DeleteTimeout time.Duration `yaml:"deleteTimeout" json:"deleteTimeout" default:"500ms"`
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
	d, _ := proto.Marshal(v.(*link.Message))
	return d
}

// Decode decode the message from byte array
func (b *Backend) Decode(value []byte, others ...interface{}) interface{} {
	if len(others) != 1 {
		panic("the length of others must be 1")
	}
	v := new(link.Message)
	proto.Unmarshal(value, v)
	v.Context.ID = others[0].(uint64)
	return v
}

// Name returns name
func (b *Backend) Name() string {
	return b.cfg.Name
}
