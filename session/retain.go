package session

import (
	"encoding/json"
	"os"
	"path"

	"github.com/baetyl/baetyl-broker/database"
	"github.com/baetyl/baetyl-go/link"
)

// RetainConfig retain config
type RetainConfig struct {
	Driver   string
	Location string
}

// RetainBackend the backend database of retain
type RetainBackend struct {
	cfg RetainConfig
	db  database.DB
}

// RetainMessage retain message
type RetainMessage struct {
	Topic   string
	Message *link.Message
}

// NewRetainBackend create a new backend database
func NewRetainBackend(cfg RetainConfig) (*RetainBackend, error) {
	p := path.Join(cfg.Location, "retain")
	err := os.MkdirAll(p, os.ModePerm)
	if err != nil {
		return nil, err
	}
	backend := &RetainBackend{cfg: cfg}
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
func (b *RetainBackend) Encode(v interface{}) []byte {
	d, _ := json.Marshal(v.(*RetainMessage))
	return d
}

// Decode decode the message from byte array
func (b *RetainBackend) Decode(value []byte, _ ...interface{}) interface{} {
	v := new(RetainMessage)
	json.Unmarshal(value, v)
	return v
}

// Set sets retain information
func (b *RetainBackend) Set(msg *RetainMessage) error {
	return b.db.SetKV(msg.Topic, msg)
}

// // Get gets retain information
// func (b *RetainBackend) Get(topic string) (*RetainMessage, error) {
// 	r, err := b.db.GetKV(topic)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if r == nil {
// 		return nil, nil
// 	}
// 	return r.(*RetainMessage), nil
// }

// Del deletes retain information
func (b *RetainBackend) Del(topic string) error {
	return b.db.DelKV(topic)
}

// List lists retain information
func (b *RetainBackend) List() ([]interface{}, error) {
	return b.db.ListKV()
}

// Close closes backend database, don't remove file
func (b *RetainBackend) Close() error {
	return b.db.Close(false)
}
