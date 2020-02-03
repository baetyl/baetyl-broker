package session

import (
	"time"

	"github.com/baetyl/baetyl-broker/queue"
	"github.com/baetyl/baetyl-go/utils"
)

// Config session config
type Config struct {
	SessionConfig `yaml:"session,omitempty" json:"session,omitempty"`
	Principals    []Principal `yaml:"principals,omitempty" json:"principals,omitempty" validate:"principals"`
}

// SessionConfig session config without principals
type SessionConfig struct {
	MaxConnections          int           `yaml:"maxConnections,omitempty" json:"maxConnections,omitempty"`
	MaxMessagePayload       utils.Size    `yaml:"maxMessagePayload,omitempty" json:"maxMessagePayload,omitempty" default:"32768" validate:"min=1,max=268435455"` // max size of message payload is (256MB - 1)
	MaxInflightQOS0Messages int           `yaml:"maxInflightQOS0Messages" json:"maxInflightQOS0Messages" default:"1000" validate:"min=1"`
	MaxInflightQOS1Messages int           `yaml:"maxInflightQOS1Messages" json:"maxInflightQOS1Messages" default:"20" validate:"min=1"`
	ResendInterval          time.Duration `yaml:"resendInterval" json:"resendInterval" default:"20s"`
	Persistence             queue.Config  `yaml:"persistence,omitempty" json:"persistence,omitempty"`
	SysTopics               []string      `yaml:"sysTopics,omitempty" json:"sysTopics,omitempty" default:"[\"$link\"]"`
}
