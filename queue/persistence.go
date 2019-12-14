package queue

import (
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
)

// Persistence is a persistent queue
type Persistence struct {
	backend *Backend
	cfg     Config
	input   chan *common.Event
	output  chan *common.Event
	edel    chan uint64 // del events with message id
	eget    chan bool   // get events
	log     *log.Logger
	utils.Tomb
}

// NewPersistence creates a new in-memory queue
func NewPersistence(cfg Config, backend *Backend, capacity int) Queue {
	q := &Persistence{
		backend: backend,
		cfg:     cfg,
		input:   make(chan *common.Event, capacity),
		output:  make(chan *common.Event, capacity),
		edel:    make(chan uint64, capacity),
		eget:    make(chan bool, 1),
		log:     log.With(log.String("queue", "persist"), log.String("id", cfg.Name)),
	}
	// to read persistent message
	q.trigger()
	q.Go(q.writing, q.reading, q.deleting)
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
	case <-q.Dying():
		return nil, ErrQueueClosed
	}
}

// Push pushes a message into queue
func (q *Persistence) Push(e *common.Event) (err error) {
	select {
	case q.input <- e:
		return nil
	case <-q.Dying():
		return ErrQueueClosed
	}
}

func (q *Persistence) writing() error {
	q.log.Info("queue starts to write messages into backend in batch mode")
	defer utils.Trace(q.log.Info, "queue has stopped writing messages")()

	var buf []*common.Event
	max := cap(q.input)
	// ? Is it possible to remove the timer?
	duration := time.Millisecond * 100
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case e := <-q.input:
			if ent := q.log.Check(log.DebugLevel, "queue received a message"); ent != nil {
				ent.Write(log.String("event", e.String()))
			}
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.add(buf)
			}
			//  if receive timeout to add messages in buffer
			timer.Reset(duration)
		case <-timer.C:
			q.log.Debug("queue writes message to backend when timeout")
			buf = q.add(buf)
		case <-q.Dying():
			q.log.Debug("queue writes message to backend during closing")
			buf = q.add(buf)
			return nil
		}
	}
}

func (q *Persistence) reading() error {
	q.log.Info("queue starts to read messages from backend in batch mode")
	defer utils.Trace(q.log.Info, "queue has stopped reading messages")()

	var err error
	var buf []*common.Event
	length := 0
	offset := uint64(1)
	max := cap(q.output)

	for {
		select {
		case <-q.eget:
			q.log.Debug("queue received a get event")
			buf, err = q.get(offset, max)
			if err != nil {
				q.log.Error("failed to get message from backend database", log.Error(err))
				continue
			}
			length = len(buf)
			if length == 0 {
				continue
			}
			for _, e := range buf {
				select {
				case q.output <- e:
				case <-q.Dying():
					return nil
				}
			}
			// set next message id
			offset = buf[length-1].Context.ID + 1
			// keep reading if any message is read
			q.trigger()
		case <-q.Dying():
			return nil
		}
	}
}

func (q *Persistence) deleting() error {
	q.log.Info("queue starts to delete messages from backend in batch mode")
	defer utils.Trace(q.log.Info, "queue has stopped deleting messages")()

	var buf []uint64
	max := cap(q.edel)
	// ? Is it possible to remove the timer?
	duration := time.Millisecond * 500
	cleanDuration := q.cfg.CleanInterval
	timer := time.NewTimer(duration)
	cleanTimer := time.NewTicker(cleanDuration)
	defer timer.Stop()
	defer cleanTimer.Stop()

	for {
		select {
		case e := <-q.edel:
			q.log.Debug("queue received a delete event")
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.delete(buf)
			}
			timer.Reset(duration)
		case <-timer.C:
			q.log.Debug("queue deletes message from backend when timeout")
			buf = q.delete(buf)
		case <-cleanTimer.C:
			q.log.Debug("queue starts to clean expired messages from backend")
			q.clean()
		case <-q.Dying():
			q.log.Debug("queue deletes message from backend during closing")
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
	var events []*common.Event
	for _, m := range msgs {
		events = append(events, common.NewEvent(m.(*common.Message), 1, q.acknowledge))
	}

	if ent := q.log.Check(log.DebugLevel, "queue has read message from backend"); ent != nil {
		ent.Write(log.Int("count", len(msgs)), log.Duration("cost", time.Since(start)))
	}
	return events, nil
}

// add all buffered messages to backend database in batch mode
func (q *Persistence) add(buf []*common.Event) []*common.Event {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(q.log.Debug, "queue has written message to backend", log.Int("count", len(buf)))()

	var msgs []interface{}
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
		q.log.Error("failed to add messages to backend database", log.Error(err))
	}
	return []*common.Event{}
}

// deletes all acknowledged message from backend database in batch mode
func (q *Persistence) delete(buf []uint64) []uint64 {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(q.log.Debug, "queue has deleted message from backend", log.Int("count", len(buf)))()

	err := q.backend.Del(buf)
	if err != nil {
		q.log.Error("failed to delete messages from backend database", log.Error(err))
	}
	return []uint64{}
}

// clean expired messages
func (q *Persistence) clean() {
	defer utils.Trace(q.log.Debug, "queue has cleaned expired messages from backend")
	err := q.backend.DelBefore(time.Now().Add(-q.cfg.ExpireTime))
	if err != nil {
		q.log.Error("failed to clean expired messages from backend", log.Error(err))
	}
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
	case <-q.Dying():
	}
}

// Close closes this queue
func (q *Persistence) Close() error {
	q.log.Info("queue is closing")
	defer q.log.Info("queue has closed")

	q.Kill(nil)
	return q.Wait()
}
