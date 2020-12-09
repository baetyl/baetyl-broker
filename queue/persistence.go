package queue

import (
	"sync"
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
	id         string
	cfg        Config
	counter    *counter
	events     chan *common.Event
	edel       chan uint64 // del events with message id
	bucket     store.BatchBucket
	recovering bool
	disable    bool
	log        *log.Logger
	utils.Tomb
	sync.Mutex
}

type counter struct {
	offset uint64
	sync.Mutex
}

func (c *counter) Next() uint64 {
	c.Lock()
	defer c.Unlock()

	next := c.offset + 1
	c.offset = next
	return next
}

// NewPersistence creates a new persistent queue
func NewPersistence(cfg Config, bucket store.BatchBucket) (Queue, error) {
	offset, err := bucket.MaxOffset()
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := &counter{
		offset: offset,
	}

	q := &Persistence{
		id:         cfg.Name,
		bucket:     bucket,
		counter:    c,
		recovering: true,
		cfg:        cfg,
		events:     make(chan *common.Event, cfg.BatchSize),
		edel:       make(chan uint64, cfg.BatchSize),
		log:        log.With(log.Any("queue", "persistence"), log.Any("id", cfg.Name)),
	}

	q.Go(q.deleting, q.recovery)
	return q, nil
}

// ID return id
func (q *Persistence) ID() string {
	return q.id
}

// Chan returns message channel
func (q *Persistence) Chan() <-chan *common.Event {
	return q.events
}

// Pop pops a message from queue
func (q *Persistence) Pop() (*common.Event, error) {
	select {
	case e := <-q.events:
		if ent := q.log.Check(log.DebugLevel, "queue poped a message"); ent != nil {
			ent.Write(log.Any("message", e.String()))
		}
		return e, nil
	case <-q.Dying():
		return nil, ErrQueueClosed
	}
}

// Disable disable
func (q *Persistence) Disable() {
	q.Lock()
	defer q.Unlock()
	q.disable = true
}

// Push pushes a message into queue
func (q *Persistence) Push(e *common.Event) (err error) {
	// need to reset msg context id
	ee := common.NewEvent(&mqtt.Message{
		Context: mqtt.Context{
			ID:    q.counter.Next(),
			TS:    e.Context.TS,
			QOS:   e.Context.QOS,
			Flags: e.Context.Flags,
			Topic: e.Context.Topic,
		},
		Content: e.Content,
	}, 1, q.acknowledge)

	err = q.add(ee)
	if err != nil {
		return errors.Trace(err)
	}
	e.Done()

	q.Lock()
	if q.recovering || q.disable {
		// if in recovery mode, send the msg to db, and do not pass to out channel
		// otherwise send to the db and pass to out channel
		q.Unlock()
		return nil
	}
	q.Unlock()

	select {
	case q.events <- ee:
		if ent := q.log.Check(log.DebugLevel, "queue pushed a message"); ent != nil {
			ent.Write(log.Any("message", ee.String()))
		}
		return nil
	case <-q.Dying():
		return ErrQueueClosed
	}
}

// recovery from db
func (q *Persistence) recovery() error {
	q.log.Debug("queue starts to recovery msgs from db in batch mode")
	defer utils.Trace(q.log.Debug, "queue has finished reading messages from db in batch mode")()

	var err error
	var buf []*common.Event
	length, offset, max := 0, uint64(1), cap(q.events)

	for {
		q.Lock()
		buf, err = q.get(offset, max)
		if err != nil {
			return errors.Trace(err)
		}
		length = len(buf)
		if length == 0 {
			q.recovering = false
			q.Unlock()
			return nil
		}
		q.Unlock()
		for _, e := range buf {
			select {
			case q.events <- e:
			case <-q.Dying():
				return nil
			}
		}
		// set next message id
		offset = buf[length-1].Context.ID + 1
	}
}

func (q *Persistence) deleting() error {
	q.log.Info("queue starts to delete messages from db in batch mode")
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
			q.log.Debug("queue deletes message from db when timeout")
			buf = q.delete(buf)
		case <-cleanTimer.C:
			q.log.Debug("queue starts to clean expired messages from db")
			q.clean()
			//q.log.Info(fmt.Sprintf("queue state: input size %d, events size %d, deletion size %d", len(q.input), len(q.events), len(q.edel)))
		case <-q.Dying():
			q.log.Debug("queue deletes message from db during closing")
			buf = q.delete(buf)
			return nil
		}
	}
}

// get gets messages from db in batch mode
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

	if ent := q.log.Check(log.DebugLevel, "queue has read message from db"); ent != nil {
		ent.Write(log.Any("count", len(msgs)), log.Any("cost", time.Since(start)))
	}
	return events, nil
}

// add all buffered messages to db in batch mode
func (q *Persistence) add(event *common.Event) error {
	if ent := q.log.Check(log.DebugLevel, "queue received a message"); ent != nil {
		ent.Write(log.Any("event", event.String()))
	}
	if event == nil {
		return nil
	}
	defer utils.Trace(q.log.Debug, "queue has written message to db", log.Any("msg", event))()

	data, err := proto.Marshal(event.Message)
	if err != nil {
		return errors.Trace(err)
	}

	return q.bucket.Set(event.Context.ID, data)
}

// deletes all acknowledged message from db in batch mode
func (q *Persistence) delete(buf []uint64) []uint64 {
	if len(buf) == 0 {
		return buf
	}

	id := buf[len(buf)-1]
	defer utils.Trace(q.log.Debug, "queue has deleted message from db", log.Any("count", len(buf)), log.Any("id", id))

	err := q.bucket.DelBeforeID(id)
	if err != nil {
		q.log.Error("failed to delete messages from db", log.Any("count", len(buf)), log.Any("id", id), log.Error(err))
	}
	return []uint64{}
}

// clean expired messages
func (q *Persistence) clean() {
	defer utils.Trace(q.log.Debug, "queue has cleaned expired messages from db")
	err := q.bucket.DelBeforeTS(uint64(time.Now().Add(-q.cfg.ExpireTime).Unix()))
	if err != nil {
		q.log.Error("failed to clean expired messages from db", log.Error(err))
	}
}

// acknowledge all acknowledged message from db in batch mode
func (q *Persistence) acknowledge(id uint64) {
	select {
	case q.edel <- id:
	case <-q.Dying():
	}
}

// Close closes this queue and clean queue data when cleanSession is true
func (q *Persistence) Close(clean bool) error {
	q.log.Debug("queue is closing", log.Any("clean", clean))
	defer q.log.Debug("queue has closed")

	q.Kill(nil)
	err := q.Wait()
	if err != nil {
		q.log.Error("failed to wait tomb goroutines", log.Error(err))
	}
	return q.bucket.Close(clean)
}
