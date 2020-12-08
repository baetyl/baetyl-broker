package queue

import (
	"fmt"
	"time"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"

	"github.com/baetyl/baetyl-broker/v2/common"
	"github.com/baetyl/baetyl-broker/v2/store"

	"github.com/gogo/protobuf/proto"
)

// Config queue config
type Config struct {
	Name          string        `yaml:"name" json:"name"`
	BatchSize     int           `yaml:"batchSize" json:"batchSize" default:"10"`
	ExpireTime    time.Duration `yaml:"expireTime" json:"expireTime" default:"168h"`
	CleanInterval time.Duration `yaml:"cleanInterval" json:"cleanInterval" default:"1h"`
	WriteTimeout  time.Duration `yaml:"writeTimeout" json:"writeTimeout" default:"100ms"`
	DeleteTimeout time.Duration `yaml:"deleteTimeout" json:"deleteTimeout" default:"500ms"`
}

// Persistence is a persistent queue
type Persistence struct {
	cfg    Config
	input  chan *common.Event
	output chan *common.Event
	edel   chan uint64 // del events with message id
	eget   chan bool   // get events
	bucket store.BatchBucket
	log    *log.Logger
	utils.Tomb
}

// NewPersistence creates a new persistent queue
func NewPersistence(cfg Config, bucket store.BatchBucket) Queue {
	q := &Persistence{
		bucket: bucket,
		cfg:    cfg,
		input:  make(chan *common.Event, cfg.BatchSize),
		output: make(chan *common.Event, cfg.BatchSize),
		edel:   make(chan uint64, cfg.BatchSize),
		eget:   make(chan bool, 1),
		log:    log.With(log.Any("queue", "persist"), log.Any("id", cfg.Name)),
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
		if ent := q.log.Check(log.DebugLevel, "queue poped a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
		return e, nil
	case <-q.Dying():
		return nil, ErrQueueClosed
	}
}

// Push pushes a message into queue
func (q *Persistence) Push(e *common.Event) (err error) {
	select {
	case q.input <- e:
		if ent := q.log.Check(log.DebugLevel, "queue pushed a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
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
	timer := time.NewTimer(q.cfg.WriteTimeout)
	defer timer.Stop()

	for {
		select {
		case e := <-q.input:
			if ent := q.log.Check(log.DebugLevel, "queue received a message"); ent != nil {
				ent.Write(log.Any("event", e.String()))
			}
			buf = append(buf, e)
			if len(buf) == max {
				buf = q.add(buf)
			}
			//  if receive timeout to add messages in buffer
			timer.Reset(q.cfg.WriteTimeout)
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
	cleanDuration := q.cfg.CleanInterval
	timer := time.NewTimer(q.cfg.DeleteTimeout)
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
			timer.Reset(q.cfg.DeleteTimeout)
		case <-timer.C:
			q.log.Debug("queue deletes message from backend when timeout")
			buf = q.delete(buf)
		case <-cleanTimer.C:
			q.log.Debug("queue starts to clean expired messages from backend")
			q.clean()
			q.log.Info(fmt.Sprintf("queue state: input size %d, output size %d, deletion size %d", len(q.input), len(q.output), len(q.edel)))
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

	var msgs []*mqtt.Message
	if err := q.bucket.Get(offset, length, func(data []byte, offset uint64) error {
		if len(data) == 0 {
			return store.ErrDataNotFound
		}
		v := new(mqtt.Message)
		err := proto.Unmarshal(data, v)
		if err != nil {
			return errors.Trace(err)
		}
		v.Context.ID = offset

		msgs = append(msgs, v)
		return nil
	}); err != nil {
		return nil, err
	}

	var events []*common.Event
	for _, m := range msgs {
		events = append(events, common.NewEvent(m, 1, q.acknowledge))
	}

	if ent := q.log.Check(log.DebugLevel, "queue has read message from backend"); ent != nil {
		ent.Write(log.Any("count", len(msgs)), log.Any("cost", time.Since(start)))
	}
	return events, nil
}

// add all buffered messages to backend database in batch mode
func (q *Persistence) add(buf []*common.Event) []*common.Event {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(q.log.Debug, "queue has written message to backend", log.Any("count", len(buf)))()

	var msgs [][]byte
	for _, e := range buf {
		data, err := proto.Marshal(e.Message)
		if err != nil {
			// TODO: how to process marshal properly ?
			q.log.Error("failed to add messages to backend database", log.Error(err))
			return []*common.Event{}
		}
		msgs = append(msgs, data)
	}
	err := q.bucket.Put(msgs)
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

	id := buf[len(buf)-1]
	defer utils.Trace(q.log.Debug, "queue has deleted message from backend", log.Any("count", len(buf)), log.Any("id", id))

	err := q.bucket.DelBeforeID(id)
	if err != nil {
		q.log.Error("failed to delete messages from backend database", log.Any("count", len(buf)), log.Any("id", id), log.Error(err))
	}
	return []uint64{}
}

// clean expired messages
func (q *Persistence) clean() {
	defer utils.Trace(q.log.Debug, "queue has cleaned expired messages from backend")
	err := q.bucket.DelBeforeTS(uint64(time.Now().Add(-q.cfg.ExpireTime).Unix()))
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

// Close closes this queue and clean queue data when cleanSession is true
func (q *Persistence) Close(clean bool) error {
	q.log.Info("queue is closing", log.Any("clean", clean))
	defer q.log.Info("queue has closed")

	q.Kill(nil)
	q.Wait()
	return q.bucket.Close(clean)
}
