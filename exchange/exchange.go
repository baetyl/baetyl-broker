package exchange

import (
	"strings"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// Exchange the message exchange
type Exchange struct {
	Bindings map[string]*mqtt.Trie
	log      *log.Logger
}

// NewExchange creates a new exchange
func NewExchange(sysTopics []string) *Exchange {
	ex := &Exchange{
		Bindings: make(map[string]*mqtt.Trie),
		log:      log.With(log.Any("broker", "exchange")),
	}
	for _, v := range sysTopics {
		ex.Bindings[v] = mqtt.NewTrie()
	}
	// common
	ex.Bindings["/"] = mqtt.NewTrie()
	return ex
}

// Bind binds a new queue with a specify topic
func (b *Exchange) Bind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.Bindings[parts[0]]; ok {
		bind.Add(parts[1], queue)
		return
	}
	// common
	b.Bindings["/"].Add(topic, queue)
}

// Unbind unbinds a queue from a specify topic
func (b *Exchange) Unbind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.Bindings[parts[0]]; ok {
		bind.Remove(parts[1], queue)
		return
	}
	// common
	b.Bindings["/"].Remove(topic, queue)
}

// UnbindAll unbinds queues from all topics
func (b *Exchange) UnbindAll(queue common.Queue) {
	for _, bind := range b.Bindings {
		bind.Clear(queue)
	}
}

// Route routes message to binding queues
func (b *Exchange) Route(msg *common.Message, cb func(uint64)) {
	sss := make([]interface{}, 0)
	parts := strings.SplitN(msg.Context.Topic, "/", 2)
	if bind, ok := b.Bindings[parts[0]]; ok {
		sss = bind.Match(parts[1])
	} else {
		sss = b.Bindings["/"].Match(msg.Context.Topic)
	}
	length := len(sss)
	b.log.Debug("exchange routes a message to queues", log.Any("count", length))
	if length == 0 {
		if cb != nil {
			cb(msg.Context.ID)
		}
		return
	}
	event := common.NewEvent(msg, int32(length), cb)
	for _, s := range sss {
		s.(common.Queue).Push(event)
	}
}
