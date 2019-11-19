package common

import (
	"github.com/256dpi/gomqtt/packet"
	"github.com/256dpi/gomqtt/session"
	"github.com/256dpi/gomqtt/topic"
)

// The supported MQTT versions.
const (
	Version311 byte = 4
	Version31  byte = 3
)

// The ConnackCode represents the return code in a Connack packet.
type ConnackCode = packet.ConnackCode

// All available ConnackCodes.
const (
	ConnectionAccepted ConnackCode = iota
	InvalidProtocolVersion
	IdentifierRejected
	ServerUnavailable
	BadUsernameOrPassword
	NotAuthorized
)

// ID the packet id
type ID = packet.ID

// QOS the quality of service levels of MQTT
type QOS = packet.QOS

// Packet the generic packet of MQTT
type Packet = packet.Generic

// Publish the publish packet of MQTT
type Publish = packet.Publish

// Puback the puback packet of MQTT
type Puback = packet.Puback

// Subscribe the subscribe packet of MQTT
type Subscribe = packet.Subscribe

// Suback the suback packet of MQTT
type Suback = packet.Suback

// Unsuback the unsuback packet of MQTT
type Unsuback = packet.Unsuback

// Pingreq the pingreq packet of MQTT
type Pingreq = packet.Pingreq

// Pingresp the pingresp packet of MQTT
type Pingresp = packet.Pingresp

// Disconnect the disconnect packet of MQTT
type Disconnect = packet.Disconnect

// Unsubscribe the unsubscribe packet of MQTT
type Unsubscribe = packet.Unsubscribe

// Connect the connect packet of MQTT
type Connect = packet.Connect

// Connack the connack packet of MQTT
type Connack = packet.Connack

// Subscription the topic and qos of subscription
type Subscription = packet.Subscription

// Counter the packet id counter
type Counter = session.IDCounter

// NewCounter creates a new counter.
func NewCounter() *Counter {
	return session.NewIDCounter()
}

// Trie the trie of topic subscription
type Trie = topic.Tree

// NewTrie creates a new trie
func NewTrie() *Trie {
	return topic.NewTree()
}
