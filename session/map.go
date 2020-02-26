package session

import (
	"errors"
	"sync"
)

var errExceedsLimit = errors.New("exceeds limit")

type syncmap struct {
	max  int
	data map[string]interface{}
	mut  sync.RWMutex
}

func newSyncMap(max int) *syncmap {
	return &syncmap{
		max:  max,
		data: map[string]interface{}{},
	}
}

func (m *syncmap) store(k string, v interface{}) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.max > 0 && len(m.data) >= m.max {
		return errExceedsLimit
	}
	m.data[k] = v
	return nil
}

func (m *syncmap) delete(k string) {
	m.mut.Lock()
	defer m.mut.Unlock()
	delete(m.data, k)
}

func (m *syncmap) empty() []interface{} {
	m.mut.Lock()
	defer m.mut.Unlock()
	res := []interface{}{}
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

func (m *syncmap) copy() []interface{} {
	m.mut.RLock()
	defer m.mut.RUnlock()
	res := []interface{}{}
	for _, v := range m.data {
		res = append(res, v)
	}
	return res
}
