package rocksdb

import (
	"bytes"
	"os"
	"time"

	"github.com/baetyl/baetyl-go/v2/errors"
	rocksdb "github.com/tecbot/gorocksdb"

	"github.com/baetyl/baetyl-broker/v2/store"
)

func init() {
	store.Factories["rocksdb"] = newRocksDB
}

// rocksdbDB the backend rocksdbDB to persist values
type rocksdbDB struct {
	*rocksdb.DB
	conf store.Conf
}

// rocksdbBucket the bucket to save data
type rocksdbBucket struct {
	db        *rocksdb.DB
	name      string
	readOpts  *rocksdb.ReadOptions
	writeOpts *rocksdb.WriteOptions
}

// New creates a new rocksdb database
func newRocksDB(conf store.Conf) (store.DB, error) {
	err := os.MkdirAll(conf.Path, 0755)
	if err != nil {
		return nil, errors.Trace(err)
	}

	opts := rocksdb.NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)

	db, err := rocksdb.OpenDb(opts, conf.Path)
	if err != nil {
		return nil, err
	}
	rocksdb.NewFixedPrefixTransform()

	return &rocksdbDB{
		DB:   db,
		conf: conf,
	}, nil
}

// NewBucket creates a bucket
func (d *rocksdbDB) NewBatchBucket(name string) (store.BatchBucket, error) {
	indexId, err := d.allocateBatchBucketID(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rocksdbBucket{
		db:   d.DB,
		name: name,
		// TODO: update
		prefixIterOpts: getPrefixIterOptions([]byte(name)),
		writeOpts:      rocksdb.NewDefaultWriteOptions(),
	}, nil
}

type indexBucket struct {
	offset uint64
	name   string
}

func (d *rocksdbDB) getBucketID(name string) ([]byte, error) {

}

// NewBucket creates a bucket
func (d *rocksdbDB) allocateBatchBucketID(name string) (int, error) {

	return 0, nil
}

// NewBucket creates a bucket
func (d *rocksdbDB) NewKVBucket(name string) (store.KVBucket, error) {
	return &rocksdbBucket{
		db:             d.DB,
		name:           name,
		prefixIterOpts: getPrefixIterOptions([]byte(name)),
		writeOpts:      rocksdb.NewDefaultWriteOptions(),
	}, nil
}

func (b *rocksdbBucket) Put(offset uint64, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}

	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	for _, value := range values {
		key := encodeBatchKey([]byte(b.name), offset)
		batch.Put(key, value)
		offset++
	}
	return errors.Trace(b.db.Write(b.writeOpts, batch))
}

// Get [begin, end)
func (b *rocksdbBucket) Get(begin, end uint64, op func([]byte, uint64) error) error {
	beginKey := append([]byte(b.name), store.U64ToByte(begin)...)
	endKey := append([]byte(b.name), store.U64ToByte(end)...)
	iter := b.db.NewIterator(b.prefixIterOpts)
	for iter.Seek(beginKey); iter.Valid() && bytes.Compare(iter.Key().Data(), endKey) < 0; iter.Next() {
		offset, _ := decodeBatchKey(iter.Key().Data(), []byte(b.name))
		err := op(iter.Value().Data(), offset)
		if err != nil {
			return errors.Trace(err)
		}
	}
	iter.Close()
	return nil
}

func (b *rocksdbBucket) MaxOffset() (uint64, error) {
	var offset uint64
	iter := b.db.NewIterator(b.prefixIterOpts)
	if iter.SeekToLast(); iter.Valid() {
		offset, _ = decodeBatchKey(iter.Key().Data(), []byte(b.name))
	}
	iter.Close()
	return offset, nil
}

func (b *rocksdbBucket) MinOffset() (uint64, error) {
	var offset uint64
	iter := b.db.NewIterator(b.prefixIterOpts)
	if iter.SeekToFirst(); iter.Valid() {
		offset, _ = decodeBatchKey(iter.Key().Data(), []byte(b.name))
	}
	iter.Close()
	return offset, nil
}

// DelBeforeID deletes values whose keys are not greater than the given id from DB
func (b *rocksdbBucket) DelBeforeID(id uint64) error {
	end := keyUpperBound(append([]byte(b.name), store.U64ToByte(id)...))
	// TODO: to fix
	return errors.Trace(b.db.Delete([]byte(b.name), end, b.writeOpts))
}

// DelBeforeTS deletes expired messages from DB
func (b *rocksdbBucket) DelBeforeTS(ts uint64) error {
	end := keyUpperBound([]byte(b.name))
	iter := b.db.NewIterator(b.prefixIterOpts)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		_, kts := decodeBatchKey(iter.Key().Data(), []byte(b.name))
		if kts > ts {
			end = iter.Key().Data()
			break
		}
	}

	iter.Close()
	// TODO: fix
	return errors.Trace(b.db.DeleteRange([]byte(b.name), end, b.writeOpts))
}

// Close close
func (b *rocksdbBucket) Close(clean bool) (err error) {
	if !clean {
		return nil
	}
	end := keyUpperBound([]byte(b.name))
	// TOOD: fix
	return b.db.DeleteRange([]byte(b.name), end, b.writeOpts)
}

// SetKV deletes expired messages from DB
func (b *rocksdbBucket) SetKV(key []byte, value []byte) error {
	key = encodeKVKey([]byte(b.name), key)
	return errors.Trace(b.db.Put(b.writeOpts, key, value))
}

// SetKV deletes expired messages from DB
func (b *rocksdbBucket) GetKV(key []byte, op func([]byte) error) error {
	key = encodeKVKey([]byte(b.name), key)
	slice, err := b.db.Get(b.readOpts, key)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(op(slice.Data()))
}

func (b *rocksdbBucket) DelKV(key []byte) error {
	key = encodeKVKey([]byte(b.name), key)
	return errors.Trace(b.db.Delete(b.writeOpts, key))
}

func (b *rocksdbBucket) ListKV(op func([]byte) error) error {
	iter := b.db.NewIterator(b.prefixIterOpts)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		err := op(iter.Value().Data())
		if err != nil {
			return errors.Trace(err)
		}
	}
	iter.Close()
	return nil
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

func getPrefixIterOptions(prefix []byte) *rocksdb.IterOptions {
	return &rocksdb.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	}
}

func encodeBatchKey(name []byte, offset uint64) []byte {
	// key = name + sid + ts (16 bytes)
	ts := uint64(time.Now().Unix())
	return append(name, store.U64U64ToByte(offset, ts)...)
}

func decodeBatchKey(key, name []byte) (uint64, uint64) {
	length := len(name)
	return store.ByteToU64(key[length : length+8]), store.ByteToU64(key[length+8:])
}

func encodeKVKey(name, key []byte) []byte {
	// key = name + kvkey (8 bytes)
	return append(name, key...)
}

func decodeKVKey(key, name []byte) []byte {
	length := len(name)
	return key[length : length+8]
}
