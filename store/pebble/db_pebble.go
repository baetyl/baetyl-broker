package pebble

import (
	"bytes"
	"os"
	"time"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/cockroachdb/pebble"

	"github.com/baetyl/baetyl-broker/v2/store"
)

func init() {
	store.Factories["pebble"] = newPebbleDB
}

// pebbleDB the backend PebbleDB to persist values
type pebbleDB struct {
	*pebble.DB
	conf store.Conf
}

// pebbleBucket the bucket to save data
type pebbleBucket struct {
	db             *pebble.DB
	name           []byte
	prefixIterOpts *pebble.IterOptions
	writeOpts      *pebble.WriteOptions
}

// New creates a new pebble database
func newPebbleDB(conf store.Conf) (store.DB, error) {
	err := os.MkdirAll(conf.Path, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pebble.Open(conf.Path, &pebble.Options{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pebbleDB{
		DB:   db,
		conf: conf,
	}, nil
}

// NewBucket creates a bucket
func (d *pebbleDB) NewBatchBucket(name string) (store.BatchBucket, error) {
	b := []byte(name)
	return &pebbleBucket{
		db:             d.DB,
		name:           b,
		prefixIterOpts: getPrefixIterOptions(b),
		// need using Sync opt, Sync is slow on darwin
		// see: https://github.com/cockroachdb/pebble/issues/1028
		writeOpts: pebble.Sync,
	}, nil
}

// NewBucket creates a bucket
func (d *pebbleDB) NewKVBucket(name string) (store.KVBucket, error) {
	b := []byte(name)
	return &pebbleBucket{
		db:             d.DB,
		name:           b,
		prefixIterOpts: getPrefixIterOptions(b),
		writeOpts:      pebble.Sync,
	}, nil
}

func (b *pebbleBucket) Put(offset uint64, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}

	batch := b.db.NewBatch()
	for _, v := range values {
		key := encodeBatchKey(b.name, offset)
		err := batch.Set(key, v, nil)
		if err != nil {
			return errors.Trace(err)
		}
		offset++
	}
	return errors.Trace(b.db.Apply(batch, b.writeOpts))
}

// Get [begin, end)
func (b *pebbleBucket) Get(begin, end uint64, op func([]byte, uint64) error) error {
	beginKey := concatBucketName(b.name, store.U64ToByte(begin))
	endKey := concatBucketName(b.name, store.U64ToByte(end))
	iter := b.db.NewIter(b.prefixIterOpts)
	for iter.SeekGE(beginKey); iter.Valid() && bytes.Compare(iter.Key(), endKey) < 0; iter.Next() {
		offset, _ := decodeBatchKey(b.name, iter.Key())
		err := op(iter.Value(), offset)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(iter.Close())
}

func (b *pebbleBucket) MaxOffset() (uint64, error) {
	var offset uint64
	iter := b.db.NewIter(b.prefixIterOpts)
	if iter.Last() {
		offset, _ = decodeBatchKey(b.name, iter.Key())
	}
	if err := iter.Close(); err != nil {
		return offset, errors.Trace(err)
	}
	return offset, nil
}

func (b *pebbleBucket) MinOffset() (uint64, error) {
	var offset uint64
	iter := b.db.NewIter(b.prefixIterOpts)
	if iter.First() {
		offset, _ = decodeBatchKey(b.name, iter.Key())
	}
	if err := iter.Close(); err != nil {
		return offset, errors.Trace(err)
	}
	return offset, nil
}

// DelBeforeID deletes values whose keys are not greater than the given id from DB
func (b *pebbleBucket) DelBeforeID(id uint64) error {
	end := keyUpperBound(concatBucketName(b.name, store.U64ToByte(id)))
	return errors.Trace(b.db.DeleteRange(b.name, end, b.writeOpts))
}

// DelBeforeTS deletes expired messages from DB
func (b *pebbleBucket) DelBeforeTS(ts uint64) error {
	end := keyUpperBound(b.name)
	iter := b.db.NewIter(b.prefixIterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		_, kts := decodeBatchKey(b.name, iter.Key())
		if kts > ts {
			end = iter.Key()
			break
		}
	}

	if err := iter.Close(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(b.db.DeleteRange(b.name, end, b.writeOpts))
}

// Close close
func (b *pebbleBucket) Close(clean bool) (err error) {
	if !clean {
		return nil
	}
	end := keyUpperBound(b.name)
	return b.db.DeleteRange(b.name, end, b.writeOpts)
}

// SetKV deletes expired messages from DB
func (b *pebbleBucket) SetKV(key []byte, value []byte) error {
	key = encodeKVKey(b.name, key)
	return errors.Trace(b.db.Set(key, value, b.writeOpts))
}

// SetKV deletes expired messages from DB
func (b *pebbleBucket) GetKV(key []byte, op func([]byte) error) error {
	key = encodeKVKey(b.name, key)
	value, closer, err := b.db.Get(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = op(value)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(closer.Close())
}

func (b *pebbleBucket) DelKV(key []byte) error {
	key = encodeKVKey(b.name, key)
	return errors.Trace(b.db.Delete(key, b.writeOpts))
}

func (b *pebbleBucket) ListKV(op func([]byte) error) error {
	iter := b.db.NewIter(b.prefixIterOpts)
	for iter.First(); iter.Valid(); iter.Next() {
		err := op(iter.Value())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(iter.Close())
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func getPrefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	}
}

func encodeBatchKey(name []byte, offset uint64) []byte {
	// key = name + sid + ts (16 bytes)
	ts := uint64(time.Now().Unix())
	return concatBucketName(name, store.U64U64ToByte(offset, ts))
}

func decodeBatchKey(name, key []byte) (uint64, uint64) {
	length := len(name)
	return store.ByteToU64(key[length : length+8]), store.ByteToU64(key[length+8:])
}

func encodeKVKey(name, key []byte) []byte {
	// key = name + kvkey (8 bytes)
	return concatBucketName(name, key)
}

func decodeKVKey(key, name []byte) []byte {
	length := len(name)
	return key[length : length+8]
}

func concatBucketName(name, key []byte) []byte {
	prefix := make([]byte, len(name))
	copy(prefix, name)
	return append(prefix, key...)
}
