package broker

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/baetyl/baetyl-broker/db/sqldb"
	"github.com/baetyl/baetyl-broker/msg"
	"github.com/stretchr/testify/assert"
)

func TestNewQueueMemory(t *testing.T) {
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

	e1, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:111 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e1.String())
}

func TestNewQueuePersistent(t *testing.T) {

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

	e1, err := b.Get()
	assert.NoError(t, err)
	assert.Equal(t, "Context:<ID:1 TS:123 QOS:1 Topic:\"t\" > Content:\"hi\" ", e1.String())
}
