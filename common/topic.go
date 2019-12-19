package common

import (
	"strings"

	"github.com/baetyl/baetyl-go/mqtt"
)

const (
	maxTopicLength = 255
	// maxTopicLevels = 9
	// topicSeparator    = "/"
	// singleWildcard    = "+"
	// multipleWildcard  = "#"
)

// CheckTopic checks the topic
func CheckTopic(topic string, wildcard bool) bool {
	var maxTopicLevels = 9
	if topic == "" {
		return false
	}
	if len(topic) > maxTopicLength || strings.Contains(topic, "\u0000") {
		return false
	}
	segments := strings.Split(topic, "/")
	if strings.HasPrefix(topic, "$") {
		if !strings.EqualFold(segments[0], "$baidu") && !strings.EqualFold(segments[0], "$link") {
			return false
		}
		segments = segments[1:]
		maxTopicLevels = maxTopicLevels - 1
	}
	levels := len(segments)
	if levels > maxTopicLevels {
		return false
	}
	for index := 0; index < levels; index++ {
		segment := segments[index]
		// check use of wildcards
		if len(segment) > 1 && (strings.Contains(segment, "+") || strings.Contains(segment, "#")) {
			return false
		}
		// check if wildcards are allowed
		if !wildcard && (segment == "#" || segment == "+") {
			return false
		}
		// check if # is the last level
		if segment == "#" && index != levels-1 {
			return false
		}
	}
	return true
}

// IsShadowTopic check the given topic is shadow topic or not
func IsShadowTopic(topic string) bool {
	return strings.HasPrefix(topic, "$baidu/")
}

// IsLinkTopic check the given topic is link topic or not
func IsLinkTopic(topic string) bool {
	return strings.HasPrefix(topic, "$link/")
}

// MatchTopicQOS if topic matched, return the lowest qos
func MatchTopicQOS(t *mqtt.Trie, topic string) (bool, uint32) {
	ss := t.Match(topic)
	ok := len(ss) != 0
	qos := uint32(1)
	for _, s := range ss {
		us := uint32(s.(mqtt.QOS))
		if us < qos {
			qos = us
			break
		}
	}
	return ok, qos
}
