package broker

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/db"
	message "github.com/baetyl/baetyl-broker/msg"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// // QueuePersistentConfig is the configuration of in-memory queue
// type QueuePersistentConfig struct {
// 	Capacity   int
// 	DropIfFull bool
// }

// QueuePersistent is an in-memory queue
type QueuePersistent struct {
	backend db.DB
	input   chan *Event
	output  chan *Event
	edel    chan uint64 // del events with message id
	eget    chan bool   // get events
	tomb    utils.Tomb
	once    sync.Once
}

// NewQueuePersistent creates a new in-memory queue
func NewQueuePersistent(capacity int, backend db.DB) Queue {
	q := &QueuePersistent{
		backend: backend,
		input:   make(chan *Event, capacity),
		output:  make(chan *Event, capacity),
		edel:    make(chan uint64, capacity),
		eget:    make(chan bool, 1),
	}
	// to read persistent message
	q.trigger()
	q.tomb.Go(q.batchingPut)
	q.tomb.Go(q.batchingGet)
	q.tomb.Go(q.batchingDel)
	return q
}

// Get gets a message
func (q *QueuePersistent) Get() (*Event, error) {
	select {
	case e := <-q.output:
		return e, nil
	case <-q.tomb.Dying():
		return nil, ErrQueueClosed
	}
}

// Put puts a message
func (q *QueuePersistent) Put(e *Event) (err error) {
	select {
	case q.input <- e:
		return nil
	case <-q.tomb.Dying():
		return ErrQueueClosed
	}
}

func (q *QueuePersistent) batchingPut() error {
	log.Debugf("batching put")
	defer utils.Trace("batching put")()

	buf := []*Event{}
	max := cap(q.input)
	// ? Is it possible to remove the ticker?
	ticker := time.NewTicker(time.Millisecond * 10)

loop:
	for {
		select {
		case e := <-q.input:
			log.Debugf("received a message: %s", e)
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.put(buf)
			}
		case <-ticker.C:
			buf = q.put(buf)
		case <-q.tomb.Dying():
			break loop
		}
	}

	// try to put all messages
	for {
		select {
		case e := <-q.input:
			buf = append(buf, e)
		default:
			q.put(buf)
			return nil
		}
	}
}

func (q *QueuePersistent) batchingGet() error {
	log.Debugf("batching get")
	defer utils.Trace("batching get")()

	var err error
	var buf []*Event
	length := 0
	offset := uint64(1)
	max := cap(q.output)

	for {
		select {
		case <-q.eget:
			log.Debugf("received a get event")
			buf, err = q.get(offset, max)
			if err != nil {
				log.Fatalf("failed to get message from backend database: %s", err.Error())
				continue
			}
			length = len(buf)
			if length == 0 {
				continue
			}
			for _, e := range buf {
				select {
				case q.output <- e:
				case <-q.tomb.Dying():
					return nil
				}
			}
			offset += uint64(length)
			// keep reading if any message is read
			q.trigger()
		case <-q.tomb.Dying():
			return nil
		}
	}
}

func (q *QueuePersistent) batchingDel() error {
	log.Debugf("batching del")
	defer utils.Trace("batching del")()

	buf := []uint64{}
	max := cap(q.edel)

	for {
		select {
		case e := <-q.edel:
			log.Debugf("received a delete event")
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.delete(buf)
			}
		case <-q.tomb.Dying():
			buf = q.delete(buf)
			return nil
		}
	}
}

// put all buffered messages to backend database in batch mode
func (q *QueuePersistent) get(offset uint64, length int) ([]*Event, error) {
	start := time.Now()

	msgs, err := q.backend.Get(offset, length)
	if err != nil {
		return nil, err
	}
	events := []*Event{}
	for _, m := range msgs {
		events = append(events, NewEvent(m, 1, q.acknowledge))
	}

	log.Debugf("get %d message(s) from backend database <-- elapsed time: %v", len(msgs), time.Since(start))
	return events, nil
}

// put all buffered messages to backend database in batch mode
func (q *QueuePersistent) put(buf []*Event) []*Event {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace("put %d message(s) to backend database", len(buf))()
	msgs := []*message.Message{}
	for _, e := range buf {
		msgs = append(msgs, e.message)
	}
	err := q.backend.Put(msgs)
	if err == nil {
		// new message arrives
		q.trigger()
		for _, e := range buf {
			e.Done()
		}
	} else {
		log.Errorf("failed to put messages to backend database: %s", err.Error())
	}
	return []*Event{}
}

// deletes all acknowledged message from backend database in batch mode
func (q *QueuePersistent) delete(buf []uint64) []uint64 {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace("deleted %d message(s) from backend database", len(buf))()
	err := q.backend.Del(buf)
	if err != nil {
		log.Errorf("failed to delete messages from backend database: %s", err.Error())
	}
	return []uint64{}
}

// triggers an event to get message from backend database in batch mode
func (q *QueuePersistent) trigger() {
	select {
	case q.eget <- true:
	default:
	}
}

// acknowledge all acknowledged message from backend database in batch mode
func (q *QueuePersistent) acknowledge(id uint64) {
	select {
	case q.edel <- id:
	case <-q.tomb.Dying():
	}
}

// Close closes this queue
func (q *QueuePersistent) Close() error {
	q.once.Do(func() {
		q.tomb.Kill(nil)
	})
	return q.tomb.Wait()
}
