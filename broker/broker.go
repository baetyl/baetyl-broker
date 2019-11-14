package broker

import (
	"github.com/256dpi/gomqtt/topic"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/queue"
)

// Broker broker interface
type Broker interface {
	Add(topic string, sub queue.Queue)
	Put(*common.Message, func(uint64))
}

type broker struct {
	t *topic.Tree
}

// NewBroker creates a new broker
func NewBroker() Broker {
	return &broker{t: topic.NewTree()}
}

func (b *broker) Add(topic string, sub queue.Queue) {
	b.t.Add(topic, sub)
}

func (b *broker) Put(msg *common.Message, cb func(uint64)) {
	subs := b.t.Match(msg.Context.Topic)
	length := len(subs)
	if length == 0 {
		if cb != nil {
			cb(msg.Context.ID)
		}
		return
	}
	event := common.NewEvent(msg, int32(length), cb)
	for _, s := range subs {
		s.(common.Subscribe).Put(event)
	}
}
