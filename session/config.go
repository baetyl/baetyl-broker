package session

import (
	"time"

	"github.com/baetyl/baetyl-broker/queue"
)

// Config session config
type Config struct {
	SessionConfig `yaml:"session,omitempty" json:"session,omitempty"`
	Principals    []Principal `yaml:"principals,omitempty" json:"principals,omitempty" validate:"principals"`
}

// SessionConfig session config without principals
type SessionConfig struct {
	MaxConnections          int           `yaml:"maxConnections,omitempty" json:"maxConnections,omitempty"`
	MaxInflightQOS0Messages int           `yaml:"maxInflightQOS0Messages" json:"maxInflightQOS0Messages" default:"1000" validate:"min=1"`
	MaxInflightQOS1Messages int           `yaml:"maxInflightQOS1Messages" json:"maxInflightQOS1Messages" default:"20" validate:"min=1"`
	ResendInterval          time.Duration `yaml:"resendInterval" json:"resendInterval" default:"20s"`
	Persistence             queue.Config  `yaml:"persistence,omitempty" json:"persistence,omitempty"`
	SysTopics               []string      `yaml:"sysTopics,omitempty" json:"sysTopics,omitempty" default:"[\"$link\"]"`
}
