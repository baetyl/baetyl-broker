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
	Name                 string        `yaml:"name" json:"name"`
	BatchSize            int           `yaml:"batchSize" json:"batchSize" default:"10"`
	BatchCacheSize       int           `yaml:"batchCacheSize" json:"batchCacheSize" default:"5"`
	BatchCacheExpireTime time.Duration `yaml:"batchCacheExpireTime" json:"batchCacheExpireTime" default:"1min"`
	ExpireTime           time.Duration `yaml:"expireTime" json:"expireTime" default:"168h"`
	CleanInterval        time.Duration `yaml:"cleanInterval" json:"cleanInterval" default:"1h"`
	WriteTimeout         time.Duration `yaml:"writeTimeout" json:"writeTimeout" default:"100ms"`
	DeleteTimeout        time.Duration `yaml:"deleteTimeout" json:"deleteTimeout" default:"500ms"`
}

// Persistence is a persistent queue
type Persistence struct {
	id      string
	cfg     Config
	offset  uint64
	cache   []*BatchMsg
	bucket  store.BatchBucket
	disable bool
	input   chan *common.Event
	output  chan *common.Event
	edel    chan uint64 // del events with message id
	eget    chan bool   // get events
	log     *log.Logger
	utils.Tomb
	sync.Mutex
}

type BatchMsg struct {
	offset uint64
	data   []*common.Event
}

// NewPersistence creates a new persistent queue
func NewPersistence(cfg Config, bucket store.BatchBucket) (Queue, error) {
	offset, err := bucket.MaxOffset()
	if err != nil {
		return nil, errors.Trace(err)
	}

	q := &Persistence{
		id:     cfg.Name,
		bucket: bucket,
		offset: offset,
		cfg:    cfg,
		input:  make(chan *common.Event, cfg.BatchSize),
		output: make(chan *common.Event, cfg.BatchSize),
		edel:   make(chan uint64, cfg.BatchSize),
		eget:   make(chan bool, 1),
		cache:  []*BatchMsg{},
		log:    log.With(log.Any("queue", "persistence"), log.Any("id", cfg.Name)),
	}

	q.trigger()
	q.Go(q.writing, q.reading, q.deleting)
	return q, nil
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

	interval := cap(q.input)
	// ? Is it possible to remove the timer?
	timer := time.NewTimer(q.cfg.WriteTimeout)
	defer timer.Stop()

	for {
		var buf []*common.Event
		select {
		case e := <-q.input:
			if ent := q.log.Check(log.DebugLevel, "queue received a message"); ent != nil {
				ent.Write(log.Any("event", e.String()))
			}
			buf = append(buf, e)
			if len(buf) == interval {
				q.add(buf)
			}
			//  if receive timeout to add messages in buffer
			timer.Reset(q.cfg.WriteTimeout)
		case <-timer.C:
			q.log.Debug("queue writes message to backend when timeout")
			q.add(buf)
		case <-q.Dying():
			q.log.Debug("queue writes message to backend during closing")
			q.add(buf)
			return nil
		}
	}
}

func (q *Persistence) reading() error {
	q.log.Info("queue starts to read messages from backend in batch mode")
	defer utils.Trace(q.log.Info, "queue has stopped reading messages")()

	interval := cap(q.output)
	begin, err := q.bucket.MinOffset()
	if err != nil {
		q.log.Debug("failed to get min offset of bucket")
		return errors.Trace(err)
	}

	for {
		select {
		case <-q.eget:
			q.log.Debug("queue received a get event")

			var end uint64
			var buf []*common.Event
			q.Lock()
			if len(q.cache) > 0 {
				end = q.cache[0].offset
				if begin == end {
					buf = q.cache[0].data
					q.cache = q.cache[1:]
				}
			}
			if end == 0 {
				end = q.offset + 1
			}
			if t := begin + uint64(interval); t < end {
				end = t
			}
			q.Unlock()

			if len(buf) == 0 && begin != end {
				buf, err = q.get(begin, end)
				if err != nil {
					q.log.Error("failed to get message from backend database", log.Error(err))
					continue
				}
			}

			if len(buf) == 0 {
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
			begin = buf[len(buf)-1].Context.ID + 1
			// keep reading if any message is read
			q.trigger()
		case <-q.Dying():
			return nil
		}
	}
}

func (q *Persistence) add(buf []*common.Event) {
	if len(buf) == 0 {
		return
	}
	defer utils.Trace(q.log.Debug, "queue has written message to backend", log.Any("count", len(buf)))()

	begin := q.offset
	var ds [][]byte
	var msgs []*common.Event
	for _, e := range buf {
		begin++
		// need to reset msg context id
		ee := common.NewEvent(&mqtt.Message{
			Context: mqtt.Context{
				ID:    begin,
				TS:    e.Context.TS,
				QOS:   e.Context.QOS,
				Flags: e.Context.Flags,
				Topic: e.Context.Topic,
			},
			Content: e.Content,
		}, 1, q.acknowledge)

		data, err := proto.Marshal(ee.Message)
		if err != nil {
			// TODO: how to process marshal properly ?
			q.log.Error("failed to add messages to backend database", log.Error(err))
			return
		}
		ds = append(ds, data)
		msgs = append(msgs, ee)
	}

	begin = q.offset + 1
	err := q.bucket.Put(begin, ds)
	if err == nil {
		for _, e := range buf {
			e.Done()
		}

		q.Lock()
		if q.disable {
			q.Unlock()
			return
		}
		if len(q.cache) < q.cfg.BatchCacheSize {
			batch := &BatchMsg{
				offset: begin,
				data:   msgs,
			}
			q.cache = append(q.cache, batch)
		}
		q.offset += uint64(len(buf))
		q.Unlock()

		// new message arrives
		q.trigger()
	} else {
		q.log.Error("failed to add messages to backend database", log.Error(err))
	}

	return
}

// get gets messages from db in batch mode
func (q *Persistence) get(begin, end uint64) ([]*common.Event, error) {
	start := time.Now()

	var msgs []*mqtt.Message
	if err := q.bucket.Get(begin, end, func(data []byte, offset uint64) error {
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

// triggers an event to get message from backend database in batch mode
func (q *Persistence) trigger() {
	select {
	case q.eget <- true:
	default:
	}
}

// ID return id
func (q *Persistence) ID() string {
	return q.id
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

func (q *Persistence) deleting() error {
	q.log.Info("queue starts to delete messages from db in batch mode")
	defer q.log.Info("queue has stopped deleting messages")

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

	// TODO: how to compact msg cache ?
	// get timestamp, if timestamp more than expire time clean it

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

// Disable disable
func (q *Persistence) Disable() {
	q.Lock()
	defer q.Unlock()
	q.disable = true
}
