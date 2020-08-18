package exchange

import (
	"strings"

	"github.com/baetyl/baetyl-broker/v2/common"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
)

// Exchange the message exchange
type Exchange struct {
	bindings map[string]*mqtt.Trie
	log      *log.Logger
}

// NewExchange creates a new exchange
func NewExchange(sysTopics []string) *Exchange {
	ex := &Exchange{
		bindings: make(map[string]*mqtt.Trie),
		log:      log.With(log.Any("broker", "exchange")),
	}
	for _, v := range sysTopics {
		ex.bindings[v] = mqtt.NewTrie()
	}
	// common
	ex.bindings["/"] = mqtt.NewTrie()
	return ex
}

// Bindings gets bindings
func (b *Exchange) Bindings() map[string]*mqtt.Trie {
	return b.bindings
}

// Bind binds a new queue with a specify topic
func (b *Exchange) Bind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		bind.Add(parts[1], queue)
		return
	}
	// common
	b.bindings["/"].Add(topic, queue)
}

// Unbind unbinds a queue from a specify topic
func (b *Exchange) Unbind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		bind.Remove(parts[1], queue)
		return
	}
	// common
	b.bindings["/"].Remove(topic, queue)
}

// UnbindAll unbinds queues from all topics
func (b *Exchange) UnbindAll(queue common.Queue) {
	for _, bind := range b.bindings {
		bind.Clear(queue)
	}
}

// Route routes message to binding queues
func (b *Exchange) Route(msg *mqtt.Message, cb func(uint64)) {
	var sss []interface{}
	parts := strings.SplitN(msg.Context.Topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok {
		sss = bind.Match(parts[1])
	} else {
		sss = b.bindings["/"].Match(msg.Context.Topic)
	}
	length := len(sss)
	b.log.Debug("exchange routes a message to queues", log.Any("count", length))
	if length == 0 {
		if cb != nil {
			cb(msg.Context.ID)
		}
		return
	}
	var err error
	event := common.NewEvent(msg, int32(length), cb)
	for _, s := range sss {
		err = s.(common.Queue).Push(event)
		if err != nil {
			b.log.Error("failed to push message into queue", log.Error(err))
		}
	}
}
