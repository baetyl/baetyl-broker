package sqldb

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-broker/msg"
	"github.com/stretchr/testify/assert"
)

func TestSQLDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	db, err := NewSQLDB(path.Join(dir, "queue.db"))
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	msgs, err := db.Get(0, 1)
	assert.NoError(t, err)
	assert.Len(t, msgs, 0)

	tt := uint64(time.Now().UnixNano())
	msg := new(message.Message)
	msg.Content = []byte("hi")
	msg.Context = new(message.Context)
	msg.Context.ID = 111
	msg.Context.TS = tt
	msg.Context.QOS = 1
	msg.Context.Topic = "t"
	err = db.Put([]*message.Message{msg})
	assert.NoError(t, err)

	msgs, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, "hi", string(msgs[0].Content))
	assert.Equal(t, uint64(1), msgs[0].Context.ID)
	assert.Equal(t, tt, msgs[0].Context.TS)
	assert.Equal(t, uint32(1), msgs[0].Context.QOS)
	assert.Equal(t, "t", msgs[0].Context.Topic)

	err = db.Del([]uint64{0, 1})
	assert.NoError(t, err)
	msgs, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, msgs, 0)

	err = db.Put([]*message.Message{msg, msg, msg, msg, msg})
	assert.NoError(t, err)

	msgs, err = db.Get(0, 10)
	assert.NoError(t, err)
	assert.Len(t, msgs, 5)
	assert.Equal(t, uint64(2), msgs[0].Context.ID)
	assert.Equal(t, uint64(3), msgs[1].Context.ID)
	assert.Equal(t, uint64(4), msgs[2].Context.ID)
	assert.Equal(t, uint64(5), msgs[3].Context.ID)
	assert.Equal(t, uint64(6), msgs[4].Context.ID)

	msgs, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, msgs, 2)
	assert.Equal(t, uint64(5), msgs[0].Context.ID)
	assert.Equal(t, uint64(6), msgs[1].Context.ID)

	err = db.Del([]uint64{5, 6})
	assert.NoError(t, err)
	msgs, err = db.Get(5, 10)
	assert.NoError(t, err)
	assert.Len(t, msgs, 0)
}
