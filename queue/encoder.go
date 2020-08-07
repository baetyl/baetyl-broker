package queue

import (
	"errors"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/gogo/protobuf/proto"
)

type Encoder struct{}

// Encode encodes the message to byte array
func (b *Encoder) Encode(value interface{}) (data []byte, err error) {
	return proto.Marshal(value.(*mqtt.Message))
}

// Decode decode the message from byte array
func (b *Encoder) Decode(data []byte, value interface{}, args ...interface{}) error {
	if len(args) != 1 {
		return errors.New("the length of others must be 1")
	}
	v := value.(*mqtt.Message)
	err := proto.Unmarshal(data, v)
	if err != nil {
		return err
	}
	v.Context.ID = args[0].(uint64)
	return nil
}
