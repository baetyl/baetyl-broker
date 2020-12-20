package rocksdb

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/baetyl/baetyl-broker/v2/store"
)

type mockStruct struct {
	ID    int
	Dummy string
	Obj   mockStruct2
}

type mockStruct2 struct {
	Name string
	Age  int
}

func TestDatabaseDriveNotFound(t *testing.T) {
	db, err := store.New(store.Conf{Driver: "does not exist", Path: "t.db"})
	assert.EqualError(t, err, "database driver not found")
	assert.Nil(t, db)
}

func TestDatabaseRocksdbDB(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	bucket, err := db.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	obj1 := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
	}

	obj2 := mockStruct{
		ID:    2,
		Dummy: "d2",
		Obj: mockStruct2{
			Name: "baetyl22",
			Age:  21,
		},
	}

	obj3 := mockStruct{
		ID:    3,
		Dummy: "d3",
		Obj: mockStruct2{
			Name: "baetyl33",
			Age:  31,
		},
	}

	var msgs [][]byte
	data1, err := json.Marshal(&obj1)
	assert.NoError(t, err)

	data2, err := json.Marshal(&obj2)
	assert.NoError(t, err)

	data3, err := json.Marshal(&obj3)
	assert.NoError(t, err)

	msgs = append(msgs, data1, data2, data3)

	err = bucket.Put(1, msgs)
	assert.NoError(t, err)

	values := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values = append(values, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, values[0], obj1)
	assert.Equal(t, values[1], obj2)
	assert.Equal(t, values[2], obj3)

	values2 := make([]mockStruct, 0)
	err = bucket.Get(1, 3, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values2 = append(values2, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values2, 2)

	err = bucket.DelBeforeID(uint64(2))
	assert.NoError(t, err)

	values3 := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values3 = append(values3, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values3, 1)

	err = bucket.Close(true)
	assert.NoError(t, err)

	values4 := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values4 = append(values4, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values4, 0)
}

func TestDatabaseRocksdbDelBeforeTs(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	bucket, err := db.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	obj1 := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
	}

	obj2 := mockStruct{
		ID:    2,
		Dummy: "d2",
		Obj: mockStruct2{
			Name: "baetyl22",
			Age:  21,
		},
	}

	obj3 := mockStruct{
		ID:    3,
		Dummy: "d3",
		Obj: mockStruct2{
			Name: "baetyl33",
			Age:  31,
		},
	}

	var msgs [][]byte
	data1, err := json.Marshal(&obj1)
	assert.NoError(t, err)

	data2, err := json.Marshal(&obj2)
	assert.NoError(t, err)

	data3, err := json.Marshal(&obj3)
	assert.NoError(t, err)

	msgs = append(msgs, data1, data2, data3)

	err = bucket.Put(1, msgs)
	assert.NoError(t, err)

	values := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values = append(values, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, values[0], obj1)
	assert.Equal(t, values[1], obj2)
	assert.Equal(t, values[2], obj3)

	time.Sleep(2 * time.Second)

	err = bucket.DelBeforeTS(uint64(time.Now().Add(-time.Second).Unix()))
	assert.NoError(t, err)

	values2 := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values2 = append(values2, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values2, 0)

	err = bucket.Put(1, msgs)
	assert.NoError(t, err)

	values3 := make([]mockStruct, 0)
	err = bucket.Get(1, 20, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values3 = append(values3, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values3, 3)

	time.Sleep(time.Second)

	err = bucket.DelBeforeTS(uint64(time.Now().Add(-3 * time.Second).Unix()))
	assert.NoError(t, err)

	values4 := make([]mockStruct, 0)
	err = bucket.Get(1, 20, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values4 = append(values4, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values4, 3)
}

func TestDatabaseRocksdbLarge(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	bucket, err := db.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	count := 10000
	var iis [][]byte
	for i := 1; i < count; i++ {
		v := mockStruct{
			ID:    i,
			Dummy: "aa",
		}
		data, err := json.Marshal(&v)
		assert.NoError(t, err)
		iis = append(iis, data)
	}

	err = bucket.Put(1, iis)
	assert.NoError(t, err)

	var values []mockStruct
	err = bucket.Get(1, uint64(count), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values = append(values, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values, count-1)
	for k, v := range values {
		m := mockStruct{}
		err := json.Unmarshal(iis[k], &m)
		assert.NoError(t, err)
		assert.Equal(t, m, v)
	}

	err = bucket.DelBeforeID(uint64(count - 1))
	assert.NoError(t, err)

	var values2 []mockStruct
	err = bucket.Get(1, uint64(count), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values2 = append(values2, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values2, 0)

	ncount := 20000
	var iis2 [][]byte
	for i := count; i < ncount; i++ {
		v := mockStruct{
			ID:    i,
			Dummy: "aa",
		}
		data, err := json.Marshal(&v)
		assert.NoError(t, err)
		iis2 = append(iis2, data)
	}

	err = bucket.Put(1, iis2)
	assert.NoError(t, err)

	var values3 []mockStruct
	err = bucket.Get(1, uint64(ncount), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values3 = append(values3, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values3, ncount-count)
	for k, v := range values3 {
		m := mockStruct{}
		err := json.Unmarshal(iis2[k], &m)
		assert.NoError(t, err)
		assert.Equal(t, m, v)
	}

	err = bucket.DelBeforeID(uint64(ncount - 1))
	assert.NoError(t, err)

	var values4 []mockStruct
	err = bucket.Get(1, uint64(ncount), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values4 = append(values4, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values4, 0)
}

func TestDatabaseRocksdbKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	bucket, err := db.NewKVBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	key1 := []byte("key1")
	obj1 := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
	}

	key2 := []byte("key2")
	obj2 := mockStruct{
		ID:    2,
		Dummy: "d2",
		Obj: mockStruct2{
			Name: "baetyl22",
			Age:  21,
		},
	}

	data1, err := json.Marshal(&obj1)
	assert.NoError(t, err)

	data2, err := json.Marshal(&obj2)
	assert.NoError(t, err)

	err = bucket.SetKV(key1, data1)
	assert.NoError(t, err)

	err = bucket.SetKV(key2, data2)
	assert.NoError(t, err)

	var value1 mockStruct
	err = bucket.GetKV(key1, func(data []byte) error {
		return json.Unmarshal(data, &value1)
	})
	assert.NoError(t, err)
	assert.Equal(t, value1, obj1)

	var value2 mockStruct
	err = bucket.GetKV(key2, func(data []byte) error {
		return json.Unmarshal(data, &value2)
	})
	assert.NoError(t, err)
	assert.Equal(t, value2, obj2)

	var values3 []mockStruct
	err = bucket.ListKV(func(data []byte) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values3 = append(values3, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values3, 2)
	assert.Equal(t, values3[0], obj1)
	assert.Equal(t, values3[1], obj2)

	err = bucket.DelKV(key1)
	assert.NoError(t, err)

	var value4 mockStruct
	err = bucket.GetKV(key1, func(data []byte) error {
		return json.Unmarshal(data, &value4)
	})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "rocksdb: not found")

	var value5 mockStruct
	err = bucket.GetKV(key2, func(data []byte) error {
		return json.Unmarshal(data, &value5)
	})
	assert.NoError(t, err)
	assert.Equal(t, value5, obj2)

	err = bucket.DelKV(key2)
	assert.NoError(t, err)

	var value6 mockStruct
	err = bucket.GetKV(key2, func(data []byte) error {
		return json.Unmarshal(data, &value6)
	})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "rocksdb: not found")

	var values7 []mockStruct
	err = bucket.ListKV(func(data []byte) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values7 = append(values7, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values7, 0)
}

func TestDatabaseRocksdbReopen(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	obj1 := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
	}

	obj2 := mockStruct{
		ID:    2,
		Dummy: "d2",
		Obj: mockStruct2{
			Name: "baetyl22",
			Age:  21,
		},
	}

	obj3 := mockStruct{
		ID:    3,
		Dummy: "d3",
		Obj: mockStruct2{
			Name: "baetyl33",
			Age:  31,
		},
	}

	var msgs [][]byte
	data1, err := json.Marshal(&obj1)
	assert.NoError(t, err)

	data2, err := json.Marshal(&obj2)
	assert.NoError(t, err)

	data3, err := json.Marshal(&obj3)
	assert.NoError(t, err)

	msgs = append(msgs, data1, data2, data3)

	err = bucket.Put(1, msgs)
	assert.NoError(t, err)

	values := make([]mockStruct, 0)
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values = append(values, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, values[0], obj1)
	assert.Equal(t, values[1], obj2)
	assert.Equal(t, values[2], obj3)

	err = db.Close()
	assert.NoError(t, err)

	db2, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db2)

	bucket2, err := db2.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket2)

	count := 10000
	var iis [][]byte
	for i := 0; i < count; i++ {
		v := mockStruct{
			ID:    i,
			Dummy: "aa",
		}
		data, err := json.Marshal(&v)
		assert.NoError(t, err)
		iis = append(iis, data)
	}

	err = bucket2.Put(4, iis)
	assert.NoError(t, err)

	values2 := make([]mockStruct, 0)
	err = bucket2.Get(1, uint64(3+count), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values2 = append(values2, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values2, 2+count)

	values3 := make([]mockStruct, 0)
	err = bucket2.Get(1, uint64(3+count+1), func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mockStruct{}
		if err := json.Unmarshal(data, &v); err != nil {
			return err
		}
		values3 = append(values3, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, values3, 3+count)

	err = db2.Close()
	assert.NoError(t, err)
}

func BenchmarkDatabaseRocksdb(b *testing.B) {
	dir, err := ioutil.TempDir("", b.Name())
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "rocksdb", Path: path.Join(dir, b.Name())})
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	bucket, err := db.NewBatchBucket(b.Name())
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	obj := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
	}
	data, err := json.Marshal(obj)
	assert.NoError(b, err)

	ds := [][]byte{data}
	b.ResetTimer()
	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bucket.Put(uint64(i), ds)
		}
	})
	// Get is slow because using json.Unmarshal here, should using protobuf instead
	b.Run("Get", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var values2 []mockStruct
			err = bucket.Get(1, 100, func(data []byte, offset uint64) error {
				if len(data) == 0 {
					return store.ErrDataNotFound
				}
				v := mockStruct{}
				if err := json.Unmarshal(data, &v); err != nil {
					return err
				}
				values2 = append(values2, v)
				return nil
			})
		}
	})
	b.Run("Del", func(b *testing.B) {
		for i := 0; i < 1000; i++ {
			bucket.DelBeforeID(uint64(i))
		}
	})
}
