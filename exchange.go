package main

import (
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
)

// Exchange the message exchange
type Exchange struct {
	bindings *common.Trie
	log      *log.Logger
}

// NewExchange creates a new exchange
func NewExchange() *Exchange {
	return &Exchange{
		bindings: common.NewTrie(),
		log:      log.With(log.String("broker", "exchange")),
	}
}

// Bind binds a new queue with a specify topic
func (b *Exchange) Bind(topic string, queue common.Queue) {
	b.bindings.Add(topic, queue)
}

// Unbind unbinds a queue from a specify topic
func (b *Exchange) Unbind(topic string, queue common.Queue) {
	b.bindings.Remove(topic, queue)
}

// UnbindAll unbinds a queue from all topics
func (b *Exchange) UnbindAll(queue common.Queue) {
	b.bindings.Clear(queue)
}

// Route routes message to binding queues
func (b *Exchange) Route(msg *common.Message, cb func(uint64)) {
	sss := b.bindings.Match(msg.Context.Topic)
	length := len(sss)
	b.log.Debug("exchange routes a message to queues", log.Int("count", length))
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
