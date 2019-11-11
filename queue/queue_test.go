package queue

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/db/sqldb"
	message "github.com/baetyl/baetyl-broker/msg"
	"github.com/stretchr/testify/assert"
)

func TestMemoryQueue(t *testing.T) {
	b := NewQueueMemory(100, true)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := NewEvent(m, 0, nil)
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
	db, err := sqldb.NewSQLDB(path.Join(dir, "queue.db"))
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	b := NewQueuePersistent(100, db)
	assert.NotNil(t, b)
	defer b.Close()

	m := new(message.Message)
	m.Content = []byte("hi")
	m.Context = new(message.Context)
	m.Context.ID = 111
	m.Context.TS = 123
	m.Context.QOS = 1
	m.Context.Topic = "t"
	e := NewEvent(m, 0, nil)
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

	ms, err := db.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 3)

	e1.Done()
	e2.Done()

	ms, err = db.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 3)

	time.Sleep(time.Millisecond * 150)

	ms, err = db.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 1)

	e3, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:3 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e3.String())

	e3.Done()
	time.Sleep(time.Millisecond * 150)

	ms, err = db.Get(1, 10)
	assert.NoError(t, err)
	assert.Len(t, ms, 0)
}
