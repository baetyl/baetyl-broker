package session

import (
	"encoding/json"
	"os"
	"path"

	"github.com/baetyl/baetyl-broker/database"
	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-broker/retain"
)

// Backend the backend database of session
type Backend struct {
	cfg Config
	db  database.DB
}

// NewBackend create a new backend database for session
func NewBackend(cfg Config) (*Backend, error) {
	p := path.Join(cfg.PersistenceLocation, "session")
	err := os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return nil, err
	}
	backend := &Backend{cfg: cfg}
	db, err := database.New(database.Conf{
		Driver: cfg.PersistenceDriver,
		Source: path.Join(p, "session.db"),
	}, backend)
	if err != nil {
		return nil, err
	}
	backend.db = db
	return backend, nil
}

// NewQueueBackend create a new backend database for queue
func (b *Backend) NewQueueBackend(name string) (*queue.Backend, error) {
	return queue.NewBackend(queue.Config{
		Name:     name,
		Driver:   b.cfg.PersistenceDriver,
		Location: b.cfg.PersistenceLocation,
	})
}

// NewRetainBackend create a new backend database for retain
func NewRetainBackend(driver, location string) (*retain.Backend, error) {
	return retain.NewBackend(retain.Config{
		Driver:   driver,
		Location: location,
	})
}

// Encode encodes the message to byte array
func (b *Backend) Encode(v interface{}) []byte {
	d, _ := json.Marshal(v.(*Info))
	return d
}

// Decode decode the message from byte array
func (b *Backend) Decode(value []byte, _ ...interface{}) interface{} {
	v := new(Info)
	json.Unmarshal(value, v)
	return v
}

// Set sets session information
func (b *Backend) Set(info *Info) error {
	return b.db.SetKV(info.ID, info)
}

// Get gets session information
func (b *Backend) Get(id string) (*Info, error) {
	i, err := b.db.GetKV(id)
	if err != nil {
		return nil, err
	}
	if i == nil {
		return nil, nil
	}
	return i.(*Info), nil
}

// Del deletes session information
func (b *Backend) Del(id string) error {
	return b.db.DelKV(id)
}

// List lists session information
func (b *Backend) List() ([]interface{}, error) {
	return b.db.ListKV()
}

// Close closes backend database
func (b *Backend) Close() error {
	return b.db.Close()
}
