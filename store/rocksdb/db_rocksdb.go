package rocksdb

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/baetyl/baetyl-go/v2/errors"
	rocksdb "github.com/tecbot/gorocksdb"

	"github.com/baetyl/baetyl-broker/v2/store"
)

const (
	IndexPrefix = "_indexer"
	// uint64 is 8 bytes
	FixedPrefixLength = 8
)

func init() {
	store.Factories["rocksdb"] = newRocksDB
}

// rocksdbDB the backend rocksdbDB to persist values
type rocksdbDB struct {
	*rocksdb.DB
	indexer *indexer
	conf    store.Conf
}

// rocksdbBucket the bucket to save data
type rocksdbBucket struct {
	db        *rocksdb.DB
	id        []byte
	readOpts  *rocksdb.ReadOptions
	writeOpts *rocksdb.WriteOptions
}

// indexer ...
type indexer struct {
	offset    uint64
	name      []byte
	db        *rocksdb.DB
	readOpts  *rocksdb.ReadOptions
	writeOpts *rocksdb.WriteOptions
	sync.Mutex
}

func (i *indexer) getBucketID(bucketName string) ([]byte, error) {
	key := encodeKVKey(i.name, []byte(bucketName))
	slice, err := i.db.Get(i.readOpts, key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	id := slice.Data()
	if len(id) == 0 {
		i.Lock()
		i.offset++
		offset := i.offset
		i.Unlock()
		key = encodeKVKey([]byte(i.name), []byte(bucketName))
		id = store.U64ToByte(offset)

		err := i.db.Put(i.writeOpts, key, id)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return id, nil
}

func newIndexer(d *rocksdb.DB) (*indexer, error) {
	bn := []byte(IndexPrefix)
	readOpt := rocksdb.NewDefaultReadOptions()
	slice, err := d.Get(readOpt, bn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var offset uint64
	if len(slice.Data()) != 0 {
		offset = store.ByteToU64(slice.Data())
	}

	return &indexer{
		offset:    offset,
		name:      bn,
		db:        d,
		readOpts:  readOpt,
		writeOpts: rocksdb.NewDefaultWriteOptions(),
	}, nil
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

	// Set up bloom filter
	// https://github.com/facebook/rocksdb/wiki/Prefix-Seek
	// will prefix bloom filter affect Get() in whole order

	opts.SetPrefixExtractor(rocksdb.NewFixedPrefixTransform(FixedPrefixLength))

	db, err := rocksdb.OpenDb(opts, conf.Path)
	if err != nil {
		return nil, err
	}

	indexer, err := newIndexer(db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rocksdbDB{
		DB:      db,
		indexer: indexer,
		conf:    conf,
	}, nil
}

// NewBucket creates a bucket
func (d *rocksdbDB) NewBatchBucket(name string) (store.BatchBucket, error) {
	id, err := d.indexer.getBucketID(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rocksdbBucket{
		db:        d.DB,
		id:        id,
		readOpts:  rocksdb.NewDefaultReadOptions(),
		writeOpts: rocksdb.NewDefaultWriteOptions(),
	}, nil
}

// NewBucket creates a bucket
func (d *rocksdbDB) NewKVBucket(name string) (store.KVBucket, error) {
	id, err := d.indexer.getBucketID(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rocksdbBucket{
		db:        d.DB,
		id:        id,
		readOpts:  rocksdb.NewDefaultReadOptions(),
		writeOpts: rocksdb.NewDefaultWriteOptions(),
	}, nil
}

// NewBucket creates a bucket
func (d *rocksdbDB) Close() error {
	d.DB.Close()
	return nil
}

func (b *rocksdbBucket) Put(offset uint64, values [][]byte) error {
	if len(values) == 0 {
		return nil
	}

	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	for _, value := range values {
		key := encodeBatchKey(b.id, offset)
		batch.Put(key, value)
		offset++
	}
	return errors.Trace(b.db.Write(b.writeOpts, batch))
}

// Get [begin, end)
func (b *rocksdbBucket) Get(begin, end uint64, op func([]byte, uint64) error) error {
	beginKey := concatBucketName(b.id, store.U64ToByte(begin))
	endKey := concatBucketName(b.id, store.U64ToByte(end))
	iter := b.db.NewIterator(b.readOpts)
	for iter.Seek(beginKey); iter.Valid() && bytes.Compare(iter.Key().Data(), endKey) < 0; iter.Next() {
		offset, _ := decodeBatchKey(b.id, iter.Key().Data())
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
	iter := b.db.NewIterator(b.readOpts)
	if iter.SeekToLast(); iter.Valid() {
		offset, _ = decodeBatchKey(b.id, iter.Key().Data())
	}
	iter.Close()
	return offset, nil
}

func (b *rocksdbBucket) MinOffset() (uint64, error) {
	var offset uint64
	iter := b.db.NewIterator(b.readOpts)
	if iter.SeekToFirst(); iter.Valid() {
		offset, _ = decodeBatchKey(b.id, iter.Key().Data())
	}
	iter.Close()
	return offset, nil
}

// DelBeforeID deletes values whose keys are not greater than the given id from DB
func (b *rocksdbBucket) DelBeforeID(id uint64) error {
	end := keyUpperBound(append(b.id, store.U64ToByte(id)...))
	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.DeleteRange(b.id, end)
	return errors.Trace(b.db.Write(b.writeOpts, batch))
}

// DelBeforeTS deletes expired messages from DB
func (b *rocksdbBucket) DelBeforeTS(ts uint64) error {
	end := keyUpperBound(b.id)
	iter := b.db.NewIterator(b.readOpts)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		_, kts := decodeBatchKey(b.id, iter.Key().Data())
		if kts > ts {
			end = iter.Key().Data()
			break
		}
	}

	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.DeleteRange(b.id, end)
	return errors.Trace(b.db.Write(b.writeOpts, batch))
}

// Close close
func (b *rocksdbBucket) Close(clean bool) (err error) {
	if !clean {
		return nil
	}
	end := keyUpperBound(b.id)

	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.DeleteRange(b.id, end)
	return errors.Trace(b.db.Write(b.writeOpts, batch))
}

// SetKV deletes expired messages from DB
func (b *rocksdbBucket) SetKV(key []byte, value []byte) error {
	key = encodeKVKey(b.id, key)
	return errors.Trace(b.db.Put(b.writeOpts, key, value))
}

// SetKV deletes expired messages from DB
func (b *rocksdbBucket) GetKV(key []byte, op func([]byte) error) error {
	key = encodeKVKey(b.id, key)
	slice, err := b.db.Get(b.readOpts, key)
	if err != nil {
		return errors.Trace(err)
	}
	if slice.Size() == 0 {
		return errors.New("rocksdb: not found")
	}
	return errors.Trace(op(slice.Data()))
}

func (b *rocksdbBucket) DelKV(key []byte) error {
	key = encodeKVKey(b.id, key)
	return errors.Trace(b.db.Delete(b.writeOpts, key))
}

func (b *rocksdbBucket) ListKV(op func([]byte) error) error {
	iter := b.db.NewIterator(b.readOpts)
	for iter.SeekToFirst(); iter.Valid() && bytes.HasPrefix(iter.Key().Data(), b.id); iter.Next() {
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

func encodeBatchKey(name []byte, offset uint64) []byte {
	// key = id + sid + ts (16 bytes)
	ts := uint64(time.Now().Unix())
	return concatBucketName(name, store.U64U64ToByte(offset, ts))
}

func decodeBatchKey(name, key []byte) (uint64, uint64) {
	length := len(name)
	return store.ByteToU64(key[length : length+8]), store.ByteToU64(key[length+8:])
}

func encodeKVKey(name, key []byte) []byte {
	// key = id + kvkey (8 bytes)
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
