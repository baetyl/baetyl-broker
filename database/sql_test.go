package database

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

type dummy struct {
	ID   uint64
	Data string
}

type encode struct{}

func (e *encode) Encode(v interface{}) []byte {
	d, _ := json.Marshal(v)
	return d
}

func (e *encode) Decode(id uint64, d []byte) interface{} {
	var v dummy
	json.Unmarshal(d, &v)
	v.ID = id
	return v
}

func TestDatabaseSQLite(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "sqlite3", Source: path.Join(dir, "t.db")}, &encode{})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, "sqlite3", db.Conf().Driver)
	defer db.Close()

	values, err := db.Get(0, 1)
	assert.NoError(t, err)
	assert.Len(t, values, 0)

	value := &dummy{
		ID:   111,
		Data: "hi",
	}
	err = db.Put([]interface{}{value})
	assert.NoError(t, err)

	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 1)
	assert.Equal(t, uint64(1), values[0].(dummy).ID)
	assert.Equal(t, "hi", values[0].(dummy).Data)

	err = db.Del([]uint64{0, 1})
	assert.NoError(t, err)
	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 0)

	err = db.Put([]interface{}{value, value, value, value, value})
	assert.NoError(t, err)

	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 5)
	assert.Equal(t, uint64(2), values[0].(dummy).ID)
	assert.Equal(t, uint64(3), values[1].(dummy).ID)
	assert.Equal(t, uint64(4), values[2].(dummy).ID)
	assert.Equal(t, uint64(5), values[3].(dummy).ID)
	assert.Equal(t, uint64(6), values[4].(dummy).ID)

	values, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, uint64(5), values[0].(dummy).ID)
	assert.Equal(t, uint64(6), values[1].(dummy).ID)

	err = db.Del([]uint64{5, 6})
	assert.NoError(t, err)
	values, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 0)
}

func TestDatabaseSQLiteNoEncode(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "sqlite3", Source: path.Join(dir, "t.db")}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, "sqlite3", db.Conf().Driver)
	defer db.Close()

	values, err := db.Get(0, 1)
	assert.NoError(t, err)
	assert.Len(t, values, 0)

	value := []byte("hi")
	err = db.Put([]interface{}{value})
	assert.NoError(t, err)

	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 1)
	assert.Equal(t, value, values[0])

	err = db.Del([]uint64{0, 1})
	assert.NoError(t, err)
	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 0)

	err = db.Put([]interface{}{value, value, value, value, value})
	assert.NoError(t, err)

	values, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 5)
	assert.Equal(t, value, values[0])

	values, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, value, values[0])

	err = db.Del([]uint64{5, 6})
	assert.NoError(t, err)
	values, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, values, 0)
}

func TestDatabaseSQLiteKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "sqlite3", Source: path.Join(dir, "kv.db")}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, "sqlite3", db.Conf().Driver)
	defer db.Close()

	k1 := []byte("k1")
	k2 := []byte("k2")

	// list empty db
	vs, err := db.ListKV()
	assert.NoError(t, err)
	assert.Empty(t, vs)

	// k1 does not exist
	v, err := db.GetKV(k1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	// put k1
	err = db.PutKV(k1, k1)
	assert.NoError(t, err)

	// list db
	vs, err = db.ListKV()
	assert.NoError(t, err)
	assert.Len(t, vs, 1)
	assert.Equal(t, k1, vs[0])

	// k1 exists
	v, err = db.GetKV(k1)
	assert.NoError(t, err)
	assert.Equal(t, k1, v)

	// put k2
	err = db.PutKV(k2, k2)
	assert.NoError(t, err)

	// list db
	vs, err = db.ListKV()
	assert.NoError(t, err)
	assert.Len(t, vs, 2)

	// put k1 again
	err = db.PutKV(k1, k2)
	assert.NoError(t, err)

	// list db
	vs, err = db.ListKV()
	assert.NoError(t, err)
	assert.Len(t, vs, 2)

	// k1 exists
	v, err = db.GetKV(k1)
	assert.NoError(t, err)
	assert.Equal(t, k2, v)

	// delete k1
	err = db.DelKV(k1)
	assert.NoError(t, err)

	// k1 does not exist
	v, err = db.GetKV(k1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	// delete k1 again
	err = db.DelKV(k1)
	assert.NoError(t, err)

	// delete k2
	err = db.DelKV(k2)
	assert.NoError(t, err)

	// list empty db
	vs, err = db.ListKV()
	assert.NoError(t, err)
	assert.Empty(t, vs)
}

func BenchmarkDatabaseSQLite(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "sqlite3", Source: path.Join(dir, "t.db")}, &encode{})
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	values, err := db.Get(0, 1)
	assert.NoError(b, err)
	assert.Len(b, values, 0)

	value := &dummy{
		ID:   111,
		Data: "hi",
	}

	b.ResetTimer()
	b.Run("put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.Put([]interface{}{value})
		}
	})
	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.Get(1, 10)
		}
	})
	b.Run("del", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			db.Del([]uint64{uint64(i)})
		}
	})
}

func TestDatabaseSQLiteData(t *testing.T) {
	t.Skip("only for dev test")

	db, err := New(Conf{Driver: "sqlite3", Source: "queue4.db"}, &encode{})
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	value := &dummy{
		ID:   1,
		Data: "hi",
	}
	for i := 0; i < 10000; i++ {
		db.Put([]interface{}{value})
	}
}
