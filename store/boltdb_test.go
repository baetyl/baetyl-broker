package store

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockStruct struct {
	ID    int
	Dummy string
	Obj1  mockStruct2
	Obj2  *mockStruct2
}

type mockStruct2 struct {
	Name string
	Age  int
}

func TestDatabaseDriveNotFound(t *testing.T) {
	db, err := New(Conf{Driver: "does not exist", Source: "t.db"})
	assert.EqualError(t, err, "database driver not found")
	assert.Nil(t, db)
}

func TestDatabaseBoltDB(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	encoders := []Encoder{new(mockEncoder), nil}
	for _, encoder := range encoders {
		bucket, err := db.NewBucket(t.Name(), encoder)
		assert.NoError(t, err)
		assert.NotNil(t, bucket)

		obj1 := mockStruct{
			ID:    1,
			Dummy: "d1",
			Obj1: mockStruct2{
				Name: "baetyl11",
				Age:  11,
			},
			Obj2: &mockStruct2{
				Name: "baetyl12",
				Age:  12,
			},
		}

		obj2 := mockStruct{
			ID:    2,
			Dummy: "d2",
			Obj1: mockStruct2{
				Name: "baetyl22",
				Age:  21,
			},
			Obj2: &mockStruct2{
				Name: "baetyl22",
				Age:  22,
			},
		}

		obj3 := mockStruct{
			ID:    3,
			Dummy: "d3",
			Obj1: mockStruct2{
				Name: "baetyl33",
				Age:  31,
			},
			Obj2: &mockStruct2{
				Name: "baetyl32",
				Age:  32,
			},
		}

		err = bucket.Put([]interface{}{obj1, obj2, obj3})
		assert.NoError(t, err)

		var values []mockStruct
		err = bucket.Get(1, 10, &values)
		assert.NoError(t, err)
		assert.Len(t, values, 3)
		assert.Equal(t, values[0], obj1)
		assert.Equal(t, values[1], obj2)
		assert.Equal(t, values[2], obj3)

		var valuesp []*mockStruct
		err = bucket.Get(1, 10, &valuesp)
		assert.NoError(t, err)
		assert.Len(t, valuesp, 3)
		assert.Equal(t, *valuesp[0], obj1)
		assert.Equal(t, *valuesp[1], obj2)
		assert.Equal(t, *valuesp[2], obj3)

		err = bucket.DelBeforeID(uint64(2))
		assert.NoError(t, err)

		var values2 []mockStruct
		err = bucket.Get(0, 10, &values2)
		assert.NoError(t, err)
		assert.Len(t, values2, 1)

		var values3 []mockStruct
		err = bucket.Get(0, 3, &values3)
		assert.NoError(t, err)
		assert.Len(t, values3, 1)

		var values4 []mockStruct
		err = bucket.Get(3, 1, &values4)
		assert.NoError(t, err)
		assert.Len(t, values4, 1)

		var values5 []mockStruct
		err = bucket.Get(3, 10, &values5)
		assert.NoError(t, err)
		assert.Len(t, values5, 1)

		var values6 []mockStruct
		err = bucket.Get(4, 10, &values6)
		assert.NoError(t, err)
		assert.Len(t, values6, 0)

		bucket.Close(true)
	}
}

func TestDatabaseBoltDBLarge(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	encoders := []Encoder{new(mockEncoder), nil}
	for _, encoder := range encoders {
		bucket, err := db.NewBucket(t.Name(), encoder)
		assert.NoError(t, err)
		assert.NotNil(t, bucket)

		count := 10000
		var iis []interface{}
		for i := 1; i < count; i++ {
			iis = append(iis, mockStruct{
				ID:    i,
				Dummy: "aa",
			})
		}
		err = bucket.Put(iis)
		assert.NoError(t, err)

		var values []mockStruct
		err = bucket.Get(1, count, &values)
		assert.NoError(t, err)
		assert.Len(t, values, count-1)
		for i := range values {
			assert.Equal(t, values[i], iis[i])
		}

		err = bucket.DelBeforeID(uint64(count-1))
		assert.NoError(t, err)

		var values2 []mockStruct
		err = bucket.Get(1, count, &values2)
		assert.NoError(t, err)
		assert.Len(t, values2, 0)

		count = 20000
		var iis2 []interface{}
		for i := 10000; i < count; i++ {
			iis2 = append(iis2, mockStruct{
				ID:    i,
				Dummy: "aa",
			})
		}
		err = bucket.Put(iis2)
		assert.NoError(t, err)

		var values3 []mockStruct
		err = bucket.Get(10000, count, &values3)
		assert.NoError(t, err)
		assert.Len(t, values, count-10001)
		for i := range values3 {
			assert.Equal(t, values3[i], iis2[i])
		}

		var values4 []mockStruct
		err = bucket.Get(1, count, &values4)
		assert.NoError(t, err)
		assert.Len(t, values, count-10001)
		for i := range values4 {
			assert.Equal(t, values4[i], iis2[i])
		}

		err = bucket.DelBeforeID(uint64(count-1))
		assert.NoError(t, err)

		var values5 []mockStruct
		err = bucket.Get(10000, count, &values5)
		assert.NoError(t, err)
		assert.Len(t, values5, 0)

		var values6 []mockStruct
		err = bucket.Get(0, count, &values6)
		assert.NoError(t, err)
		assert.Len(t, values6, 0)

		bucket.Close(true)
	}
}

func TestDatabaseBoltDBKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	encoders := []Encoder{new(mockEncoder), nil}
	for _, encoder := range encoders {
		bucket, err := db.NewBucket(t.Name(), encoder)
		assert.NoError(t, err)
		assert.NotNil(t, bucket)

		key1 := "key1"
		obj1 := mockStruct{
			ID:    1,
			Dummy: "d1",
			Obj1: mockStruct2{
				Name: "baetyl11",
				Age:  11,
			},
			Obj2: &mockStruct2{
				Name: "baetyl12",
				Age:  12,
			},
		}

		key2 := "key2"
		obj2 := mockStruct{
			ID:    2,
			Dummy: "d2",
			Obj1: mockStruct2{
				Name: "baetyl22",
				Age:  21,
			},
			Obj2: &mockStruct2{
				Name: "baetyl22",
				Age:  22,
			},
		}

		err = bucket.SetKV(key1, obj1)
		assert.NoError(t, err)

		err = bucket.SetKV(key2, obj2)
		assert.NoError(t, err)

		var value1 mockStruct
		err = bucket.GetKV(key1, &value1)
		assert.NoError(t, err)
		assert.Equal(t, value1, obj1)

		var value2 mockStruct
		err = bucket.GetKV(key2, &value2)
		assert.NoError(t, err)
		assert.Equal(t, value2, obj2)

		var values []mockStruct
		err = bucket.ListKV(&values)
		assert.NoError(t, err)
		assert.Len(t, values, 2)
		assert.Equal(t, values[0], obj1)
		assert.Equal(t, values[1], obj2)

		err = bucket.DelKV(key1)
		assert.NoError(t, err)

		var value3 mockStruct
		err = bucket.GetKV(key1, &value3)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "No data found for this key")

		err = bucket.DelKV(key2)
		assert.NoError(t, err)

		var value4 mockStruct
		err = bucket.GetKV(key2, &value4)
		assert.Error(t, err)
		assert.Equal(t, err.Error(), "No data found for this key")

		var values2 []mockStruct
		err = bucket.ListKV(&values2)
		assert.NoError(t, err)
		assert.Len(t, values2, 0)

		bucket.Close(true)
	}
}

func TestDatabaseBoltDBReopen(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBucket(t.Name(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	key1 := "key1"
	obj1 := mockStruct{
		ID:    1,
		Dummy: "d1",
		Obj1: mockStruct2{
			Name: "baetyl11",
			Age:  11,
		},
		Obj2: &mockStruct2{
			Name: "baetyl12",
			Age:  12,
		},
	}

	key2 := "key2"
	obj2 := mockStruct{
		ID:    2,
		Dummy: "d2",
		Obj1: mockStruct2{
			Name: "baetyl22",
			Age:  21,
		},
		Obj2: &mockStruct2{
			Name: "baetyl22",
			Age:  22,
		},
	}

	err = bucket.SetKV(key1, obj1)
	assert.NoError(t, err)

	err = bucket.SetKV(key2, obj2)
	assert.NoError(t, err)

	var value1 mockStruct
	err = bucket.GetKV(key1, &value1)
	assert.NoError(t, err)
	assert.Equal(t, value1, obj1)

	var value2 mockStruct
	err = bucket.GetKV(key2, &value2)
	assert.NoError(t, err)
	assert.Equal(t, value2, obj2)

	var values []mockStruct
	err = bucket.ListKV(&values)
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, values[0], obj1)
	assert.Equal(t, values[1], obj2)

	err = bucket.Close(false)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// Test Reopen
	db, err = New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err = db.NewBucket(t.Name(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var values2 []mockStruct
	err = bucket.ListKV(&values2)
	assert.NoError(t, err)
	assert.Len(t, values2, 2)
	assert.Equal(t, values2[0], obj1)
	assert.Equal(t, values2[1], obj2)

	err = bucket.Close(true)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	// Test Reopen
	db, err = New(Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err = db.NewBucket(t.Name(), nil)
	defer bucket.Close(true)
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var values3 []mockStruct
	err = bucket.ListKV(&values3)
	assert.NoError(t, err)
	assert.Len(t, values3, 0)
}

func BenchmarkDatabaseBoltDB(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, b.Name())})
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	bucket, err := db.NewBucket(b.Name(), nil)
	defer bucket.Close(true)
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	obj := 12345
	b.ResetTimer()
	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bucket.Put([]interface{}{obj})
		}
	})
	b.Run("Get", func(b *testing.B) {
		for i := 1; i <= b.N; i++ {
			var values []int
			bucket.Get(uint64(i), 100, &values)
		}
	})
	b.Run("Del", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bucket.DelBeforeID(uint64(i))
		}
	})
}

type mockEncoder struct{}

// Encode encodes the message to byte array
func (b *mockEncoder) Encode(value interface{}) (data []byte, err error) {
	return json.Marshal(value)
}

// Decode decode the message from byte array
func (b *mockEncoder) Decode(data []byte, value interface{}, _ ...interface{}) error {
	return json.Unmarshal(data, value)
}
