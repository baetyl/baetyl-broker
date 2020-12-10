package queue

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/baetyl/baetyl-broker/v2/common"
	"github.com/baetyl/baetyl-broker/v2/store"

	_ "github.com/baetyl/baetyl-broker/v2/store/pebble"
)

func TestTemporaryQueue(t *testing.T) {
	b := NewTemporary(t.Name(), 100, true)
	assert.NotNil(t, b)
	defer b.Close(true)

	m := new(mqtt.Message)
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
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "pebble", Path: path.Join(dir, t.Name())})
	assert.NoError(t, err)
	assert.NotNil(t, db)

	bucket, err := db.NewBatchBucket(t.Name())
	assert.NoError(t, err)
	assert.NotNil(t, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = t.Name()

	b, err := NewPersistence(cfg, bucket)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	m := new(mqtt.Message)
	m.Content = []byte("hi")
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t1"
	e := common.NewEvent(m, 0, nil)

	err = b.Push(e)
	assert.NoError(t, err)

	e.Context.Topic = "t2"
	err = b.Push(e)
	assert.NoError(t, err)

	e.Context.Topic = "t3"
	err = b.Push(e)
	assert.NoError(t, err)

	e1, err := b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:1 TS:123 QOS:1 Topic:\"t1\" > Content:\"hi\" ", e1.String())
	e2, err := b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:2 TS:123 QOS:1 Topic:\"t2\" > Content:\"hi\" ", e2.String())

	var ms []mqtt.Message
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mqtt.Message{}
		if err := proto.Unmarshal(data, &v); err != nil {
			return err
		}
		ms = append(ms, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, ms, 3)

	e1.Done()
	e2.Done()

	time.Sleep(time.Second)

	var ms2 []mqtt.Message
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mqtt.Message{}
		if err := proto.Unmarshal(data, &v); err != nil {
			return err
		}
		ms2 = append(ms2, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, ms2, 1)
	fmt.Println("sd: ", ms2[0])

	e3, err := b.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:3 TS:123 QOS:1 Topic:\"t3\" > Content:\"hi\" ", e3.String())

	e3.Done()
	time.Sleep(time.Second)

	var ms4 []mqtt.Message
	err = bucket.Get(1, 10, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := mqtt.Message{}
		if err := proto.Unmarshal(data, &v); err != nil {
			return err
		}
		ms4 = append(ms4, v)
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, ms4, 0)

	err = b.Close(false)
	assert.NoError(t, err)
}

func BenchmarkPersistentQueue(b *testing.B) {
	dir, err := ioutil.TempDir("", b.Name())
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "pebble", Path: path.Join(dir, b.Name())})
	assert.NoError(b, err)
	assert.NotNil(b, db)

	bucket, err := db.NewBatchBucket(b.Name())
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()

	q, err := NewPersistence(cfg, bucket)
	assert.NoError(b, err)
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

func newMockEvent(i uint64) *common.Event {
	m := new(mqtt.Message)
	m.Content = []byte("hi")
	m.Context.ID = i
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "b"
	return common.NewEvent(m, 0, nil)
}

func BenchmarkPersistentQueueParallel(b *testing.B) {
	dir, err := ioutil.TempDir("", b.Name())
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := store.New(store.Conf{Driver: "pebble", Path: path.Join(dir, b.Name())})
	assert.NoError(b, err)
	assert.NotNil(b, db)

	bucket, err := db.NewBatchBucket(b.Name())
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	var cfg Config
	utils.SetDefaults(&cfg)
	cfg.Name = b.Name()
	assert.NoError(b, err)
	assert.NotNil(b, bucket)

	q, err := NewPersistence(cfg, bucket)
	assert.NoError(b, err)
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

type mockMessageContext struct {
	ID    uint64
	TS    uint64
	QOS   uint32
	Flags uint32
	Topic string
}

type mockMessage struct {
	Context mockMessageContext
	Content []byte
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
	b.Run("Proto", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			proto.Unmarshal(d, m)
		}
	})

	mm := new(mockMessage)
	mm.Content = []byte("hi")
	mm.Context.ID = 111
	mm.Context.TS = 123
	mm.Context.QOS = 1
	mm.Context.Topic = "b"
	d2, err := json.Marshal(mm)
	assert.NoError(b, err)
	b.ResetTimer()
	b.Run("Json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			json.Unmarshal(d2, mm)
		}
	})

	mmm := new(mockMessage)
	mmm.Content = []byte("hi")
	mmm.Context.ID = 111
	mmm.Context.TS = 123
	mmm.Context.QOS = 1
	mmm.Context.Topic = "b"
	d3, err := mockGobEncode(mmm)
	assert.NoError(b, err)
	b.ResetTimer()
	b.Run("Gob", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mockGobDecode(d3, mmm)
		}
	})
}

func mockGobEncode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer

	en := gob.NewEncoder(&buff)

	err := en.Encode(value)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func mockGobDecode(data []byte, value interface{}) error {
	var buff bytes.Buffer
	de := gob.NewDecoder(&buff)

	_, err := buff.Write(data)
	if err != nil {
		return err
	}

	return de.Decode(value)
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
