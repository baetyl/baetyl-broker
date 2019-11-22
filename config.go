package main

import (
	"fmt"
	"strings"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-broker/utils"
)

const (
	maxTopicLevels = 9
	maxTopicLength = 255
	// topicSeparator    = "/"
	// singleWildcard    = "+"
	// multipleWildcard  = "#"
	// systemTopicPrefix = "$SYS"
)

// Config all config of edge
type config struct {
	Addresses   []string          `yaml:"addresses" json:"addresses"`
	Certificate utils.Certificate `yaml:"certificate" json:"certificate"`
	Principals  []auth.Principal  `yaml:"principals" json:"principals" validate:"principals"`
	Session     session.Config    `yaml:"session" json:"session"`

	InternalEndpoint struct {
		Disable bool   `yaml:"disable" json:"disable"`
		Address string `yaml:"address" json:"address" default:"tcp://0.0.0.0:11883"`
	} `yaml:"internalEndpoint" json:"internalEndpoint"`
}

// principalsValidate validate principals config is valid or not
func principalsValidate(v interface{}, param string) error {
	principals := v.([]auth.Principal)
	err := userValidate(principals)
	if err != nil {
		return err
	}
	for _, principal := range principals {
		for _, permission := range principal.Permissions {
			for _, permit := range permission.Permits {
				if !topicValidate(permit, true) {
					return fmt.Errorf("%s topic(%s) invalid", permission.Action, permit)
				}
			}
		}
	}
	return nil
}

// userValidate validate username duplicate or not
func userValidate(principals []auth.Principal) error {
	userMap := make(map[string]struct{})
	for _, principal := range principals {
		if _, ok := userMap[principal.Username]; ok {
			return fmt.Errorf("username (%s) duplicate", principal.Username)
		}
		userMap[principal.Username] = struct{}{}
	}

	return nil
}

func topicValidate(topic string, wildcard bool) bool {
	if topic == "" {
		return false
	}
	if len(topic) > maxTopicLength || strings.Contains(topic, "\u0000") {
		return false
	}
	segments := strings.Split(topic, "/")
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
