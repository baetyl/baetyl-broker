package session

import (
	"sync"
)

type syncmap struct {
	data map[string]interface{}
	mut  sync.RWMutex
}

func newSyncMap() *syncmap {
	return &syncmap{
		data: make(map[string]interface{}),
	}
}

func (m *syncmap) store(k string, v interface{}) (actual interface{}, loaded bool) {
	m.mut.Lock()
	defer m.mut.Unlock()

	old, ok := m.data[k]
	m.data[k] = v
	return old, ok
}

func (m *syncmap) delete(k string) {
	m.mut.Lock()
	defer m.mut.Unlock()
	delete(m.data, k)
}

func (m *syncmap) empty() []interface{} {
	m.mut.Lock()
	defer m.mut.Unlock()
	var res []interface{}
	for k, v := range m.data {
		delete(m.data, k)
		res = append(res, v)
	}
	return res
}

func (m *syncmap) load(k string) (interface{}, bool) {
	m.mut.RLock()
	defer m.mut.RUnlock()
	v, ok := m.data[k]
	return v, ok
}

func (m *syncmap) count() int {
	m.mut.RLock()
	defer m.mut.RUnlock()
	return len(m.data)
}
