package store

import (
	"encoding/binary"
	"os"

	"github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/utils"
)

// U64U64ToByte converts two uint64 to bytes
func U64U64ToByte(sid, ts uint64) []byte {
	r := make([]byte, 16)
	binary.BigEndian.PutUint64(r, sid)
	binary.BigEndian.PutUint64(r[8:], ts)
	return r
}

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

func LoadConfig(cfg interface{}) error {
	f := os.Getenv(context.KeyConfFile)
	if utils.FileExists(f) {
		return errors.Trace(utils.LoadYAML(f, cfg))
	}
	return errors.Trace(utils.UnmarshalYAML(nil, cfg))
}
