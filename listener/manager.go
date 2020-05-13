package listener

import (
	"crypto/tls"
	"fmt"
	"io"

	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
)

// Config listener config
type Config struct {
	Listeners []Listener `yaml:"listeners" json:"listeners"`
}

// Listener listener config
type Listener struct {
	Address              string     `yaml:"address" json:"address"`
	MaxMessageSize       utils.Size `yaml:"maxMessageSize" json:"maxMessageSize"`
	MaxConcurrentStreams uint32     `yaml:"maxConcurrentStreams" json:"maxConcurrentStreams"`
	utils.Certificate    `yaml:",inline" json:",inline"`
}

// Handler listener handler
type Handler interface {
	Handle(mqtt.Connection)
}

// Manager listener manager
type Manager struct {
	mqtts []mqtt.Server
	log   *log.Logger
}

// NewManager creates a new listener manager
func NewManager(cfg []Listener, handler Handler) (*Manager, error) {
	m := &Manager{
		mqtts: make([]mqtt.Server, 0),
		log:   log.With(log.Any("listener", "manager")),
	}
	var err error
	tlsconfigs := map[string]*tls.Config{}
	for _, c := range cfg {
		var tlsconfig *tls.Config
		if c.Key != "" || c.Cert != "" {
			tlsconfig = tlsconfigs[fmt.Sprintf(c.CA, "`", c.Key, "`", c.Cert)]
			if tlsconfig == nil {
				tlsconfig, err = utils.NewTLSConfigServer(c.Certificate)
				if err != nil {
					m.Close()
					return nil, err
				}
			}
		}

		svr, err := m.launchMQTTServer(c.Address, tlsconfig, handler)
		if err != nil {
			m.Close()
			return nil, err
		}
		m.mqtts = append(m.mqtts, svr)
		m.log.Info("listener has initialized", log.Any("listener", svr.Addr()))
	}
	return m, nil
}

func (m *Manager) launchMQTTServer(address string, tlsconfig *tls.Config, handler Handler) (mqtt.Server, error) {
	svr, err := mqtt.NewLauncher(tlsconfig).Launch(address)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := svr.Accept()
			if err != nil {
				if err == io.EOF {
					m.log.Debug("failed to accept connection", log.Error(err))
				} else {
					m.log.Error("failed to accept connection", log.Error(err))
				}
				return
			}
			handler.Handle(conn)
		}
	}()
	return svr, nil
}

// Close closes listener
func (m *Manager) Close() error {
	m.log.Info("listener manager is closing")
	defer m.log.Info("listener manager has closed")

	for _, svr := range m.mqtts {
		svr.Close()
		m.log.Info("listener has stopped", log.Any("listener", svr.Addr()))
	}
	return nil
}
