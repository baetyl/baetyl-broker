package main

import (
	"fmt"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/utils"
)

// Config all config of broker
type config struct {
	Addresses   []string          `yaml:"addresses" json:"addresses"`
	Certificate utils.Certificate `yaml:"certificate" json:"certificate"`
	Principals  []auth.Principal  `yaml:"principals" json:"principals" validate:"principals"`
	Session     session.Config    `yaml:"session" json:"session"`
	Link        link.ServerConfig `yaml:"link" json:"link"`
	SysTopics   []string          `yaml:"sysTopics" json:"sysTopics" default:"[\"$link\"]"`
}

// principalsValidate validate principals config is valid or not
func (c *config) principalsValidate(v interface{}, param string) error {
	principals := v.([]auth.Principal)
	err := userValidate(principals)
	if err != nil {
		return err
	}
	tc := common.NewTopicChecker(c.SysTopics)
	for _, principal := range principals {
		for _, permission := range principal.Permissions {
			for _, permit := range permission.Permits {
				if !tc.CheckTopic(permit, true) {
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
