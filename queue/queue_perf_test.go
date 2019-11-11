package queue

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/db/sqldb"
	message "github.com/baetyl/baetyl-broker/msg"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func BenchmarkPersistentQueue(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)
	db, err := sqldb.NewSQLDB(path.Join(dir, "queue.db"))
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	q := NewQueuePersistent(100, db)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := NewEvent(m, 0, nil)

	b.ResetTimer()
	b.Run("put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Put(e)
		}
	})
	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Put(e)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Get()
		}
	})
	b.Run("put-get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Put(e)
		}
		for i := 0; i < b.N; i++ {
			q.Get()
		}
	})
}

func BenchmarkPersistentQueueParallel(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)
	db, err := sqldb.NewSQLDB(path.Join(dir, "queue.db"))
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	q := NewQueuePersistent(100, db)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := NewEvent(m, 0, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Put(e)
			q.Get()
		}
	})
}

func BenchmarkMemoryQueueParallel(b *testing.B) {
	q := NewQueueMemory(100, true)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := NewEvent(m, 0, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Put(e)
			q.Get()
		}
	})
}

func BenchmarkTimer(b *testing.B) {
	timer := time.NewTimer(time.Millisecond * 10)
	defer timer.Stop()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		timer.Reset(time.Millisecond * 10)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	d, err := proto.Marshal(m)
	assert.NoError(b, err)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proto.Unmarshal(d, m)
	}
}
