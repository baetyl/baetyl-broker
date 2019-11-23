// +build !linux

package main

import (
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-broker/utils"
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
