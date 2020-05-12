package store

import (
	"encoding/binary"
)

// U64ToByte converts uint64 to bytes
func U64ToByte(v uint64) []byte {
	r := make([]byte, 8)
	binary.BigEndian.PutUint64(r, v)
	return r
}

// ByteToU64 converts bytes to uint64
func ByteToU64(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}
