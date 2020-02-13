package retain

import (
	"encoding/json"
	"os"
	"path"

	"github.com/baetyl/baetyl-broker/database"
	"github.com/baetyl/baetyl-go/link"
)

// Config retain config
type Config struct {
	Driver   string
	Location string
}

// Backend the backend database of retain
type Backend struct {
	cfg Config
	db  database.DB
}

// Retain retain struct
type Retain struct {
	Topic   string
	Message *link.Message
}

// NewBackend create a new backend database
func NewBackend(cfg Config) (*Backend, error) {
	p := path.Join(cfg.Location, "retain")
	err := os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return nil, err
	}
	backend := &Backend{cfg: cfg}
	db, err := database.New(database.Conf{
		Driver: cfg.Driver,
		Source: path.Join(p, "retain.db"),
	}, backend)
	if err != nil {
		return nil, err
	}
	backend.db = db
	return backend, nil
}

// Encode encodes the message to byte array
func (b *Backend) Encode(v interface{}) []byte {
	d, _ := json.Marshal(v.(*Retain))
	return d
}

// Decode decode the message from byte array
func (b *Backend) Decode(value []byte, _ ...interface{}) interface{} {
	v := new(Retain)
	json.Unmarshal(value, v)
	return v
}

// Set sets retain information
func (b *Backend) Set(retain *Retain) error {
	return b.db.SetKV(retain.Topic, retain)
}

// Get gets retain information
func (b *Backend) Get(topic string) (*Retain, error) {
	r, err := b.db.GetKV(topic)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	return r.(*Retain), nil
}

// Del deletes retain information
func (b *Backend) Del(topic string) error {
	return b.db.DelKV(topic)
}

// List lists retain information
func (b *Backend) List() ([]interface{}, error) {
	return b.db.ListKV()
}

// Close closes backend database, don't remove file
func (b *Backend) Close() error {
	return b.db.Close(false)
}
