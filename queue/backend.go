package queue

import (
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/database"
	"github.com/gogo/protobuf/proto"
)

// Backend the backend database of queue
type Backend struct {
	database.DB
}

// NewBackend create a new backend database
func NewBackend(conf database.Conf) (*Backend, error) {
	b := new(Backend)
	db, err := database.New(conf, b)
	if err != nil {
		return nil, err
	}
	b.DB = db
	return b, nil
}

// Encode encodes the message to byte array
func (b *Backend) Encode(v interface{}) []byte {
	d, _ := proto.Marshal(v.(*common.Message))
	return d
}

// Decode decode the message from byte array
func (b *Backend) Decode(id uint64, d []byte) interface{} {
	v := new(common.Message)
	proto.Unmarshal(d, v)
	v.Context.ID = id
	return v
}
