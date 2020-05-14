package queue

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/store"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestTemporaryQueueSimple(t *testing.T) {
	b := NewTemporary(t.Name(), 100, true)
	assert.NotNil(t, b)
	defer b.Close(true)

	e1 := newMockEvent(uint64(1))
	err := b.Push(e1)
	assert.NoError(t, err)
	e2 := newMockEvent(uint64(2))
	err = b.Push(e2)
	assert.NoError(t, err)
	e3 := newMockEvent(uint64(3))
	err = b.Push(e3)
	assert.NoError(t, err)

	es, err := b.Pop()
	assert.NoError(t, err)
	fmt.Println(es)
	assert.Len(t, es, 3)
	assert.Equal(t, es[0], e1)
	assert.Equal(t, es[1], e2)
	assert.Equal(t, es[2], e3)
}

func TestTemporaryQueue(t *testing.T) {
	b := NewTemporary(t.Name(), 100000, true)
	assert.NotNil(t, b)
	defer b.Close(true)

	count := 1000
	var es []*common.Event
	for i := 0; i < count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es = append(es, e)
	}

	esr, err := b.Pop()
	assert.NoError(t, err)
	assert.Len(t, esr, count)
	for i := 0; i < count; i++ {
		assert.Equal(t, es[i], esr[i])
	}

	var es2 []*common.Event
	for i := 0; i < 2*count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es2 = append(es2, e)
	}

	es2r, err := b.Pop()
	assert.NoError(t, err)
	assert.Len(t, es2r, 2*count)
	for i := 0; i < 2*count; i++ {
		assert.Equal(t, es2[i], es2r[i])
	}

	var es3 []*common.Event
	for i := 0; i < 3*count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es3 = append(es3, e)
	}

	var es4 []*common.Event
	for i := 0; i < 4*count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es4 = append(es4, e)
	}

	es3r, err := b.Pop()
	assert.NoError(t, err)
	assert.Len(t, es3r, 7*count)
	for i := 0; i < 3*count; i++ {
		assert.Equal(t, es3[i], es3r[i])
	}
	for i := 3 * count; i < 7*count; i++ {
		assert.Equal(t, es4[i-3*count], es3r[i])
	}
}

func TestPersistentQueueSimple(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = t.Name()

	b := NewPersistence(cfg, bucket)
	assert.NotNil(t, b)

	e1 := newMockEvent(uint64(1))
	err = b.Push(e1)
	assert.NoError(t, err)
	e2 := newMockEvent(uint64(2))
	err = b.Push(e2)
	e3 := newMockEvent(uint64(3))
	err = b.Push(e3)

	time.Sleep(time.Second)

	var ms []mqtt.Message
	err = bucket.Get(1, 10, &ms)
	assert.NoError(t, err)
	assert.Len(t, ms, 3)
}

func TestPersistentQueue(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = t.Name()

	b := NewPersistence(cfg, bucket)
	assert.NotNil(t, b)

	count := 1000
	var es []*common.Event
	for i := 1; i <= count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es = append(es, e)
	}

	time.Sleep(time.Second)

	var ms []mqtt.Message
	err = bucket.Get(1, 100000, &ms)
	assert.NoError(t, err)
	assert.Len(t, ms, count)

	var esr []*common.Event
	esri, err := b.Pop()
	assert.NoError(t, err)
	for len(esri) != 0 {
		esr = append(esr, esri...)
		esri, err = b.Pop()
		assert.NoError(t, err)
	}
	assert.Len(t, esr, count)
	for i := 0; i < count; i++ {
		assert.Equal(t, es[i].Context, esr[i].Context)
		assert.Equal(t, es[i].Content, esr[i].Content)
	}

	for i := 0; i < count; i++ {
		esr[i].Done()
	}

	time.Sleep(time.Second)

	var ms2 []mqtt.Message
	err = bucket.Get(1, 100000, &ms2)
	assert.NoError(t, err)
	assert.Len(t, ms2, 0)

	var es2 []*common.Event
	for i := count + 1; i <= 2*count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es2 = append(es2, e)
	}

	time.Sleep(time.Second)

	var ms3 []mqtt.Message
	err = bucket.Get(1, 100000, &ms3)
	assert.NoError(t, err)
	assert.Len(t, ms3, count)

	err = b.Close(false)
	assert.NoError(t, err)

	bucket2, err := db.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket2)

	b2 := NewPersistence(cfg, bucket2)
	assert.NotNil(t, b2)

	var ms4 []mqtt.Message
	err = bucket2.Get(1, 100000, &ms4)
	assert.NoError(t, err)
	assert.Len(t, ms4, count)

	var esr4 []*common.Event
	esri4, err := b2.Pop()
	assert.NoError(t, err)
	for len(esri4) != 0 {
		esr4 = append(esr4, esri4...)
		esri4, err = b2.Pop()
		assert.NoError(t, err)
	}
	assert.Len(t, esr4, count)
	for i := 0; i < count; i++ {
		assert.Equal(t, es2[i].Context, esr4[i].Context)
		assert.Equal(t, es2[i].Content, esr4[i].Content)
	}

	err = b.Close(true)
	assert.NoError(t, err)

	bucket3, err := db.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket3)

	b3 := NewPersistence(cfg, bucket3)
	assert.NotNil(t, b3)

	var ms5 []mqtt.Message
	err = bucket3.Get(1, 100000, &ms5)
	assert.NoError(t, err)
	assert.Len(t, ms5, 0)
}

func TestPersistentQueueReopen(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = t.Name()

	b := NewPersistence(cfg, bucket)
	assert.NotNil(t, b)

	count := 1000
	var es []*common.Event
	for i := 1; i <= count; i++ {
		e := newMockEvent(uint64(i))
		err := b.Push(e)
		assert.NoError(t, err)
		es = append(es, e)
	}

	time.Sleep(time.Second)

	var ms []mqtt.Message
	err = bucket.Get(1, 100000, &ms)
	assert.NoError(t, err)
	assert.Len(t, ms, count)

	var esr []*common.Event
	esri, err := b.Pop()
	assert.NoError(t, err)
	for len(esri) != 0 {
		esr = append(esr, esri...)
		esri, err = b.Pop()
		assert.NoError(t, err)
	}
	assert.Len(t, esr, count)
	for i := 0; i < count; i++ {
		assert.Equal(t, es[i].Context, esr[i].Context)
		assert.Equal(t, es[i].Content, esr[i].Content)
	}

	err = b.Close(false)
	assert.NoError(t, err)

	err = db.Close()
	assert.NoError(t, err)

	db2, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db2)

	bucket2, err := db2.NewBucket(t.Name(), new(Encoder))
	assert.NoError(t, err)
	assert.NotNil(t, bucket2)

	b2 := NewPersistence(cfg, bucket2)
	assert.NotNil(t, b2)

	var ms2 []mqtt.Message
	err = bucket2.Get(1, 100000, &ms2)
	assert.NoError(t, err)
	assert.Len(t, ms2, count)

	var esr2 []*common.Event
	esri2, err := b2.Pop()
	assert.NoError(t, err)
	for len(esri2) != 0 {
		esr2 = append(esr2, esri2...)
		esri2, err = b2.Pop()
		assert.NoError(t, err)
	}
	assert.Len(t, esr2, count)
	for i := 0; i < count; i++ {
		assert.Equal(t, es[i].Context, esr2[i].Context)
		assert.Equal(t, es[i].Content, esr2[i].Content)
	}
}

func BenchmarkPersistentQueue(b *testing.B) {
	dir, err := ioutil.TempDir("", b.Name())
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, b.Name())})
	defer db.Close()
	assert.NoError(b, err)
	assert.NotNil(b, db)

	bucket, err := db.NewBucket(b.Name(), new(Encoder))
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()

	q := NewPersistence(cfg, bucket)
	assert.NotNil(b, q)
	defer q.Close(false)

	b.ResetTimer()
	b.Run("Push", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Push(newMockEvent(uint64(i)))
		}
	})

	b.Run("PushPop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			q.Push(newMockEvent(uint64(i)))
		}
		for i := 0; i < b.N; i++ {
			q.Pop()
		}
	})
}

func BenchmarkPersistentQueueParallel(b *testing.B) {
	dir, err := ioutil.TempDir("", b.Name())
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "boltdb", Source: path.Join(dir, b.Name())})
	defer db.Close()
	assert.NoError(b, err)
	assert.NotNil(b, db)

	bucket, err := db.NewBucket(b.Name(), new(Encoder))
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	q := NewPersistence(cfg, bucket)
	assert.NotNil(b, q)
	defer q.Close(false)

	m := new(mqtt.Message)
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
	defer q.Close(false)

	m := new(mqtt.Message)
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
	m := new(mqtt.Message)
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

func newMockEvent(i uint64) *common.Event {
	m := new(mqtt.Message)
	m.Content = []byte("hi")
	m.Context.ID = i
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	return common.NewEvent(m, 0, nil)
}
