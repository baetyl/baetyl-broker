package queue

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/database"
	"github.com/gogo/protobuf/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestTemporaryQueue(t *testing.T) {
	b := NewTemporary(100, true)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := common.NewEvent(m, 0, nil)
	err := b.Put(e)
	assert.NoError(t, err)
	err = b.Put(e)
	assert.NoError(t, err)
	err = b.Put(e)
	assert.NoError(t, err)

	e, err = b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
	e, err = b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
	e, err = b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
}

func TestPersistentQueue(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	be, err := NewBackend(database.Conf{Driver: "sqlite3", Source: path.Join(dir, "queue.db")})
	assert.NoError(t, err)
	assert.NotNil(t, be)
	defer be.Close()

	b := NewPersistence(100, be)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := common.NewEvent(m, 0, nil)
	err = b.Put(e)
	assert.NoError(t, err)
	err = b.Put(e)
	assert.NoError(t, err)
	err = b.Put(e)
	assert.NoError(t, err)

	e1, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:1 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e1.String())
	e2, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:2 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e2.String())

	ms, err := be.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 3)

	e1.Done()
	e2.Done()

	ms, err = be.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 3)

	time.Sleep(time.Millisecond * 600)

	ms, err = be.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 1)

	e3, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:3 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e3.String())

	e3.Done()
	time.Sleep(time.Millisecond * 600)

	ms, err = be.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 0)
}

func BenchmarkPersistentQueue(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)
	be, err := NewBackend(database.Conf{Driver: "sqlite3", Source: path.Join(dir, "queue.db")})
	assert.NoError(b, err)
	assert.NotNil(b, be)
	defer be.Close()

	q := NewPersistence(100, be)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

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
	be, err := NewBackend(database.Conf{Driver: "sqlite3", Source: path.Join(dir, "queue.db")})
	assert.NoError(b, err)
	assert.NotNil(b, be)
	defer be.Close()

	q := NewPersistence(100, be)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Put(e)
			q.Get()
		}
	})
}

func BenchmarkTemporaryQueueParallel(b *testing.B) {
	q := NewTemporary(100, true)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

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
	m := new(common.Message)
	m.Content = []byte("hi")
	m.Context = new(common.Context)
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
