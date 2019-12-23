package main

import (
	"strings"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

type systemTopic struct {
	shadow string `default:"$baidu"`
	link   string `default:"$link"`
}

// Exchange the message exchange
type Exchange struct {
	bindings     map[string]*mqtt.Trie
	systemTopics systemTopic
	log          *log.Logger
}

// NewExchange creates a new exchange
func NewExchange() *Exchange {
	ex := &Exchange{}
	return &Exchange{
		bindings: map[string]*mqtt.Trie{ex.systemTopics.shadow: mqtt.NewTrie(), ex.systemTopics.link: mqtt.NewTrie(), "/": mqtt.NewTrie()},
		log:      log.With(log.Any("broker", "exchange")),
	}
}

// Bind binds a new queue with a specify topic
func (b *Exchange) Bind(topic string, queue common.Queue) {
	parts := strings.SplitN(topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok && (parts[0] == b.systemTopics.shadow || parts[0] == b.systemTopics.link) {
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

// UnbindShadowAll unbinds a queue from $baidu topics
func (b *Exchange) UnbindShadowAll(queue common.Queue) {
	b.bindings[b.systemTopics.shadow].Clear(queue)
}

// UnbindLinkAll unbinds a queue from $link topics
func (b *Exchange) UnbindLinkAll(queue common.Queue) {
	b.bindings[b.systemTopics.link].Clear(queue)
}

// UnbindCommonAll unbinds a queue from common topics
func (b *Exchange) UnbindCommonAll(queue common.Queue) {
	b.bindings["/"].Clear(queue)
}

// UnbindAll unbinds queues from all topics
func (b *Exchange) UnbindAll(queue common.Queue) {
	b.UnbindShadowAll(queue)
	b.UnbindLinkAll(queue)
	b.UnbindCommonAll(queue)
}

// Route routes message to binding queues
func (b *Exchange) Route(msg *common.Message, cb func(uint64)) {
	length := 1
	sss := make([]interface{}, 0)
	parts := strings.SplitN(msg.Context.Topic, "/", 2)
	if bind, ok := b.bindings[parts[0]]; ok && (parts[0] == b.systemTopics.shadow || parts[0] == b.systemTopics.link) {
		qq := bind.Get(parts[1])
		sss = append(sss, qq...)
	} else {
		sss = b.bindings["/"].Match(msg.Context.Topic)
	}
	length = len(sss)
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
