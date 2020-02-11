package queue

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/gogo/protobuf/proto"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestTemporaryQueue(t *testing.T) {
	b := NewTemporary(t.Name(), 100, true)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(link.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := common.NewEvent(m, 0, nil)
	err := b.Push(e)
	assert.NoError(t, err)
	err = b.Push(e)
	assert.NoError(t, err)
	err = b.Push(e)
	assert.NoError(t, err)

	e, err = b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
	e, err = b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
	e, err = b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e.String())
}

func TestPersistentQueue(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = t.Name()
	cfg.Location = dir
	be, err := NewBackend(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, be)
	defer be.Close()

	b := NewPersistence(cfg, be, true)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(link.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := common.NewEvent(m, 0, nil)
	err = b.Push(e)
	assert.NoError(t, err)
	err = b.Push(e)
	assert.NoError(t, err)
	err = b.Push(e)
	assert.NoError(t, err)

	e1, err := b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:1 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e1.String())
	e2, err := b.Pop()
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

	e3, err := b.Pop()
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

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()
	cfg.Location = dir
	be, err := NewBackend(cfg)
	assert.NoError(b, err)
	assert.NotNil(b, be)
	defer be.Close()

	q := NewPersistence(cfg, be, true)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(link.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

	b.ResetTimer()
	b.Run("Push", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Push(e)
		}
	})
	b.Run("Pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Push(e)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.Pop()
		}
	})
	b.Run("PushPop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Push(e)
		}
		for i := 0; i < b.N; i++ {
			q.Pop()
		}
	})
}

func BenchmarkPersistentQueueParallel(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()
	cfg.Location = dir
	be, err := NewBackend(cfg)
	assert.NoError(b, err)
	assert.NotNil(b, be)
	defer be.Close()

	q := NewPersistence(cfg, be, true)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(link.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Push(e)
			q.Pop()
		}
	})
}

func BenchmarkTemporaryQueueParallel(b *testing.B) {
	q := NewTemporary(b.Name(), 100, true)
	assert.NotNil(b, q)
	defer q.Close()

	m := new(link.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	e := common.NewEvent(m, 0, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Push(e)
			q.Pop()
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
	m := new(link.Message)
	m.Content = []byte("hi")
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

func TestChannelLB(t *testing.T) {
	t.Skip("only for dev test")
	var wg sync.WaitGroup
	quit := make(chan int)
	queue := make(chan int, 100)
	for index := 0; index < 10; index++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var count int
			for {
				select {
				case <-queue:
					count++
					time.Sleep(time.Millisecond * 10)
				case <-quit:
					fmt.Printf("%d --> %d\n", i, count)
					return
				}
			}
		}(index)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var count int
		for {
			count++
			select {
			case queue <- count:
			case <-quit:
				fmt.Printf("\n%d\n", count)
				return
			}
		}
	}()
	time.Sleep(time.Second * 10)
	close(quit)
	wg.Wait()
}
