package queue

import (
	"sync"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// Persistence is a persistent queue
type Persistence struct {
	backend *Backend
	input   chan *common.Event
	output  chan *common.Event
	edel    chan uint64 // del events with message id
	eget    chan bool   // get events
	tomb    utils.Tomb
	once    sync.Once
}

// NewPersistence creates a new in-memory queue
func NewPersistence(capacity int, backend *Backend) Queue {
	q := &Persistence{
		backend: backend,
		input:   make(chan *common.Event, capacity),
		output:  make(chan *common.Event, capacity),
		edel:    make(chan uint64, capacity),
		eget:    make(chan bool, 1),
	}
	// to read persistent message
	q.trigger()
	q.tomb.Go(q.writing)
	q.tomb.Go(q.reading)
	q.tomb.Go(q.deleting)
	return q
}

// Chan returns message channel
func (q *Persistence) Chan() <-chan *common.Event {
	return q.output
}

// Pop pops a message from queue
func (q *Persistence) Pop() (*common.Event, error) {
	select {
	case e := <-q.output:
		return e, nil
	case <-q.tomb.Dying():
		return nil, ErrQueueClosed
	}
}

// Push pushes a message into queue
func (q *Persistence) Push(e *common.Event) (err error) {
	select {
	case q.input <- e:
		return nil
	case <-q.tomb.Dying():
		return ErrQueueClosed
	}
}

func (q *Persistence) writing() error {
	log.Infof("queue (%s) starts to write messages into backend in batch mode", q.backend.Name())
	defer utils.Trace(log.Infof, "queue (%s) has stopped writing messages", q.backend.Name())()

	buf := []*common.Event{}
	max := cap(q.input)
	// ? Is it possible to remove the timer?
	duration := time.Millisecond * 100
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case e := <-q.input:
			log.Debugf("received a message: %s", e)
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.add(buf)
			}
			//  if receive timeout to add messages in buffer
			timer.Reset(duration)
		case <-timer.C:
			log.Debugf("add when timeout")
			buf = q.add(buf)
		case <-q.tomb.Dying():
			log.Debugf("add when close")
			buf = q.add(buf)
			return nil
		}
	}
}

func (q *Persistence) reading() error {
	log.Infof("queue (%s) starts to read messages from backend in batch mode", q.backend.Name())
	defer utils.Trace(log.Infof, "queue (%s) has stopped reading messages", q.backend.Name())()

	var err error
	var buf []*common.Event
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

func (q *Persistence) deleting() error {
	log.Infof("queue (%s) starts to delete messages from backend in batch mode", q.backend.Name())
	defer utils.Trace(log.Infof, "queue (%s) has stopped deleting messages", q.backend.Name())()

	buf := []uint64{}
	max := cap(q.edel)
	// ? Is it possible to remove the timer?
	duration := time.Millisecond * 500
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case e := <-q.edel:
			timer.Reset(time.Second)
			log.Debugf("received a delete event")
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.delete(buf)
			}
			timer.Reset(duration)
		case <-timer.C:
			log.Debugf("delete when timeout")
			buf = q.delete(buf)
		case <-q.tomb.Dying():
			log.Debugf("delete when close")
			buf = q.delete(buf)
			return nil
		}
	}
}

// get gets messages from backend database in batch mode
func (q *Persistence) get(offset uint64, length int) ([]*common.Event, error) {
	start := time.Now()

	msgs, err := q.backend.Get(offset, length)
	if err != nil {
		return nil, err
	}
	events := []*common.Event{}
	for _, m := range msgs {
		events = append(events, common.NewEvent(m.(*common.Message), 1, q.acknowledge))
	}

	log.Debugf("queue (%s) has read %d message(s) from backend <-- elapsed time: %v", q.backend.Name(), len(msgs), time.Since(start))
	return events, nil
}

// add all buffered messages to backend database in batch mode
func (q *Persistence) add(buf []*common.Event) []*common.Event {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(log.Debugf, "queue (%s) has written %d message(s) to backend", q.backend.Name(), len(buf))()

	msgs := []interface{}{}
	for _, e := range buf {
		msgs = append(msgs, e.Message)
	}
	err := q.backend.Put(msgs)
	if err == nil {
		// new message arrives
		q.trigger()
		for _, e := range buf {
			e.Done()
		}
	} else {
		log.Errorf("failed to add messages to backend database: %s", err.Error())
	}
	return []*common.Event{}
}

// deletes all acknowledged message from backend database in batch mode
func (q *Persistence) delete(buf []uint64) []uint64 {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(log.Debugf, "queue (%s) has deleted %d message(s) to backend", q.backend.Name(), len(buf))()

	err := q.backend.Del(buf)
	if err != nil {
		log.Errorf("failed to delete messages from backend database: %s", err.Error())
	}
	return []uint64{}
}

// triggers an event to get message from backend database in batch mode
func (q *Persistence) trigger() {
	select {
	case q.eget <- true:
	default:
	}
}

// acknowledge all acknowledged message from backend database in batch mode
func (q *Persistence) acknowledge(id uint64) {
	select {
	case q.edel <- id:
	case <-q.tomb.Dying():
	}
}

// Close closes this queue
func (q *Persistence) Close() error {
	q.once.Do(func() {
		q.tomb.Kill(nil)
	})
	return q.tomb.Wait()
}
