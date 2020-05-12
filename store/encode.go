package store

import (
	"bytes"
	"encoding/gob"
)

type encoder struct{}

func NewDefaultEncoder() Encoder {
	return new(encoder)
}

// Encode encoding (Gob)
func (e *encoder) Encode(value interface{}) (data []byte, err error) {
	var buff bytes.Buffer
	en := gob.NewEncoder(&buff)
	err = en.Encode(value)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// Decode decoding (Gob)
func (e *encoder) Decode(data []byte, value interface{}, _ ...interface{}) error {
	var buff bytes.Buffer
	de := gob.NewDecoder(&buff)

	_, err := buff.Write(data)
	if err != nil {
		return err
	}
	return de.Decode(value)
}
