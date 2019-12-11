package session

import (
	"os"
	"testing"

	"github.com/256dpi/gomqtt/packet"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/stretchr/testify/assert"
)

func TestWill(t *testing.T) {
	cfg := Config{
		PersistenceDriver:   "sqlite3",
		PersistenceLocation: "var/lib/baetyl",
	}
	backend, err := NewBackend(cfg)
	assert.Nil(t, err)
	manager := &Manager{
		backend: backend,
		log:     log.With(log.String("manager", "session")),
	}

	// round 1: get will message of test-1(id)
	msg, err := manager.getWill("test-1")
	assert.Nil(t, err)
	assert.Nil(t, msg)

	// round 2: set will message
	p := &packet.Message{
		Topic:   "t1",
		QOS:     0,
		Payload: []byte("just test"),
		Retain:  true,
	}
	err = manager.setWill("test-1", p)
	assert.Nil(t, err)

	// round 3: get will message of test-1(id)
	msg, err = manager.getWill("test-1")
	assert.Nil(t, err)
	assert.Equal(t, "t1", msg.Topic)
	assert.Equal(t, common.QOS(0), msg.QOS)
	assert.Equal(t, []byte("just test"), msg.Payload)
	assert.Equal(t, true, msg.Retain)

	// round 4: remove will message
	err = manager.removeWill("test-1")
	assert.Nil(t, err)

	// round 5: get will message of test-1(id)
	msg, err = manager.getWill("test-1")
	assert.Nil(t, err)
	assert.Nil(t, msg)
	defer os.RemoveAll("var")
}

func TestRetain(t *testing.T) {
	cfg := Config{
		PersistenceDriver:   "sqlite3",
		PersistenceLocation: "var/lib/baetyl",
	}
	backend, err := NewRetainBackend(cfg.PersistenceDriver, cfg.PersistenceLocation)
	assert.Nil(t, err)
	manager := &Manager{
		retain: backend,
		log:    log.With(log.String("manager", "session")),
	}

	// round 1: get retain message
	msgs, err := manager.getRetain()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(msgs))

	// round 2: set retain message
	p := &packet.Message{
		Topic:   "t1",
		QOS:     0,
		Payload: []byte("just test"),
		Retain:  true,
	}
	err = manager.setRetain(p.Topic, p)
	assert.Nil(t, err)

	// round 3: get retain message
	msgs, err = manager.getRetain()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(msgs))
	assert.Equal(t, "t1", msgs[0].Topic)
	assert.Equal(t, common.QOS(0), msgs[0].QOS)
	assert.Equal(t, []byte("just test"), msgs[0].Payload)
	assert.Equal(t, true, msgs[0].Retain)

	// round 4: remove retain message
	err = manager.removeRetain("t1")
	assert.Nil(t, err)

	// round 5: get retain message of t1
	msgs, err = manager.getRetain()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(msgs))
	defer os.RemoveAll("var")
}
