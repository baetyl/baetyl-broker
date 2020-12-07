package session

import (
	"sync"

	"github.com/baetyl/baetyl-go/v2/mqtt"
)

type cache struct {
	data   sync.Map
	offset mqtt.ID
}

func (c *cache) store(m *eventWrapper) error {
	if prev, ok := c.data.LoadOrStore(m.id, m); ok && prev != m {
		prev.(*eventWrapper).Done()
		return ErrSessionClientPacketIDConflict
	}
	return nil
}

func (c *cache) delete(id uint64) error {
	if id != uint64(c.offset) {
		return nil
	}
	m, ok := c.data.Load(id)
	if !ok {
		return ErrSessionClientPacketNotFound
	}
	c.data.Delete(id)
	m.(*eventWrapper).Done()
	c.offset = mqtt.NextCounterID(c.offset)
	return nil
}
