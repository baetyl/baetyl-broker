package store

import (
	"encoding/json"
)

type encoder struct{}

func NewDefaultEncoder() Encoder {
	return new(encoder)
}

// Encode encoding
func (e *encoder) Encode(value interface{}) (data []byte, err error) {
	return json.Marshal(value)
}

// Decode decoding
func (e *encoder) Decode(data []byte, value interface{}, _ ...interface{}) error {
	return json.Unmarshal(data, value)
}
