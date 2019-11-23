package main

import (
	"fmt"

	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-broker/utils"
)

// Config all config of broker
type config struct {
	Addresses   []string          `yaml:"addresses" json:"addresses"`
	Certificate utils.Certificate `yaml:"certificate" json:"certificate"`
	Principals  []auth.Principal  `yaml:"principals" json:"principals" validate:"principals"`
	Session     session.Config    `yaml:"session" json:"session"`

	InternalEndpoint InternalEndpoint `yaml:"internalEndpoint" json:"internalEndpoint"`
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
				if !common.CheckTopic(permit, true) {
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
