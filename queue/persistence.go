package queue

import (
	"fmt"
	"time"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/store"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
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
	edel   chan uint64        // del events with message id
	eget   chan *common.Event // get events
	bucket store.Bucket
	log    *log.Logger
	offset uint64
	utils.Tomb
}

// NewPersistence creates a new persistent queue
func NewPersistence(cfg Config, bucket store.Bucket) Queue {
	q := &Persistence{
		bucket: bucket,
		cfg:    cfg,
		input:  make(chan *common.Event, cfg.BatchSize),
		edel:   make(chan uint64, cfg.BatchSize),
		eget:   make(chan *common.Event, 1),
		offset: uint64(1),
		log:    log.With(log.Any("queue", "persist"), log.Any("id", cfg.Name)),
	}
	// to read persistent message
	q.trigger()
	q.Go(q.writing, q.deleting)
	return q
}

// Chan returns message channel
func (q *Persistence) Chan() <-chan *common.Event {
	return q.eget
}

// Fetch fetch messages from queue
func (q *Persistence) Fetch() ([]*common.Event, error) {
	start := time.Now()

	var msgs []*mqtt.Message
	err := q.bucket.Get(q.offset, q.cfg.BatchSize, &msgs)
	if err != nil {
		return nil, err
	}
	var events []*common.Event
	for _, m := range msgs {
		events = append(events, common.NewEvent(m, 1, q.acknowledge))
	}
	length := len(events)
	if length == 0 {
		return events, nil
	}
	q.offset = events[length-1].Context.ID + 1
	q.trigger()
	if ent := q.log.Check(log.DebugLevel, "queue has poped messages from storage"); ent != nil {
		ent.Write(log.Any("count", len(msgs)), log.Any("cost", time.Since(start)))
	}
	return events, nil
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
	q.log.Info("queue starts to write messages into storage in batch mode")
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
			q.log.Debug("queue writes message to storage when timeout")
			buf = q.add(buf)
		case <-q.Dying():
			q.log.Debug("queue writes message to storage during closing")
			buf = q.add(buf)
			return nil
		}
	}
}

func (q *Persistence) deleting() error {
	q.log.Info("queue starts to delete messages from storage in batch mode")
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
			q.log.Debug("queue deletes message from storage when timeout")
			buf = q.delete(buf)
		case <-cleanTimer.C:
			q.log.Debug("queue starts to clean expired messages from storage")
			q.clean()
			q.log.Info(fmt.Sprintf("queue state: input size %d, deletion size %d", len(q.input), len(q.edel)))
		case <-q.Dying():
			q.log.Debug("queue deletes message from storage during closing")
			buf = q.delete(buf)
			return nil
		}
	}
}

// add all buffered messages to storage in batch mode
func (q *Persistence) add(buf []*common.Event) []*common.Event {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(q.log.Debug, "queue has written message to storage", log.Any("count", len(buf)))()

	var msgs []interface{}
	for _, e := range buf {
		msgs = append(msgs, e.Message)
	}
	err := q.bucket.Put(msgs)
	if err == nil {
		// new message arrives
		q.trigger()
		for _, e := range buf {
			e.Done()
		}
	} else {
		q.log.Error("failed to add messages to storage", log.Error(err))
	}
	return []*common.Event{}
}

// deletes all acknowledged message from storage in batch mode
func (q *Persistence) delete(buf []uint64) []uint64 {
	if len(buf) == 0 {
		return buf
	}

	defer utils.Trace(q.log.Debug, "queue has deleted message from storage", log.Any("count", len(buf)))()

	err := q.bucket.Del(buf)
	if err != nil {
		q.log.Error("failed to delete messages from storage", log.Any("count", len(buf)), log.Error(err))
	}
	return []uint64{}
}

// clean expired messages
func (q *Persistence) clean() {
	defer utils.Trace(q.log.Debug, "queue has cleaned expired messages from storage")
	err := q.bucket.DelBefore(time.Now().Add(-q.cfg.ExpireTime))
	if err != nil {
		q.log.Error("failed to clean expired messages from storage", log.Error(err))
	}
}

// triggers an event to get message from storage in batch mode
func (q *Persistence) trigger() {
	select {
	case q.eget <- &common.Event{}:
	default:
	}
}

// acknowledge all acknowledged message from storage in batch mode
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
