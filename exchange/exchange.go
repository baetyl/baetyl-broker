package exchange

import (
	"github.com/baetyl/baetyl-broker/common"
)

// Exchange the message exchange
type Exchange struct {
	bindings *common.Trie
}

// NewExchange creates a new exchange
func NewExchange() *Exchange {
	return &Exchange{bindings: common.NewTrie()}
}

// Bind binds a new queue
func (b *Exchange) Bind(topic string, queue common.Queue) {
	b.bindings.Add(topic, queue)
}

// Unbind Unbinds a queue
func (b *Exchange) Unbind(topic string, queue common.Queue) {
	b.bindings.Remove(topic, queue)
}

// Route routes message to binding queues
func (b *Exchange) Route(msg *common.Message, cb func(uint64)) {
	sss := b.bindings.Match(msg.Context.Topic)
	length := len(sss)
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
