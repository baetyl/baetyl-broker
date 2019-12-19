package main

import (
	"strings"

	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
)

// Exchange the message exchange
type Exchange struct {
	bindings map[string]*mqtt.Trie
	log      *log.Logger
}

// NewExchange creates a new exchange
func NewExchange() *Exchange {
	return &Exchange{
		bindings: map[string]*mqtt.Trie{"$baidu": mqtt.NewTrie(), "$link": mqtt.NewTrie(), "$common": mqtt.NewTrie()},
		log:      log.With(log.Any("broker", "exchange")),
	}
}

// Bind binds a new queue with a specify topic
func (b *Exchange) Bind(topic string, queue common.Queue) {
	if strings.HasPrefix(topic, "$baidu/") {
		b.bindings["$baidu"].Add(topic, queue)
	} else if strings.HasPrefix(topic, "$link/") {
		b.bindings["$link"].Add(topic, queue)
	} else {
		b.bindings["$common"].Add(topic, queue)
	}
}

// Unbind unbinds a queue from a specify topic
func (b *Exchange) Unbind(topic string, queue common.Queue) {
	if strings.HasPrefix(topic, "$baidu/") {
		b.bindings["$baidu"].Remove(topic, queue)
	} else if strings.HasPrefix(topic, "$link/") {
		b.bindings["$link"].Remove(topic, queue)
	} else {
		b.bindings["$common"].Remove(topic, queue)
	}
}

// UnbindShadowAll unbinds a queue from $baidu topics
func (b *Exchange) UnbindShadowAll(queue common.Queue) {
	b.bindings["$baidu"].Clear(queue)
}

// UnbindLinkAll unbinds a queue from $link topics
func (b *Exchange) UnbindLinkAll(queue common.Queue) {
	b.bindings["$link"].Clear(queue)
}

// UnbindCommonAll unbinds a queue from common topics
func (b *Exchange) UnbindCommonAll(queue common.Queue) {
	b.bindings["$common"].Clear(queue)
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
	if strings.HasPrefix(msg.Context.Topic, "$baidu/") {
		qq := b.bindings["$baidu"].Get(msg.Context.Topic)
		sss = append(sss, qq...)
	} else if strings.HasPrefix(msg.Context.Topic, "$link/") {
		qq := b.bindings["$link"].Get(msg.Context.Topic)
		sss = append(sss, qq...)
	} else {
		sss = b.bindings["$common"].Match(msg.Context.Topic)
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
