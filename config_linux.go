// +build linux

package main

// InternalEndpoint internal endpoint config
type InternalEndpoint struct {
	Disable bool   `yaml:"disable" json:"disable"`
	Address string `yaml:"address" json:"address" default:"unix://var/run/baetyl/broker.sock"`
}
