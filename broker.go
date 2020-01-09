package main

import (
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"google.golang.org/grpc"
)

// Config all config of broker
type Config struct {
	Addresses   []string          `yaml:"addresses" json:"addresses"`
	Certificate utils.Certificate `yaml:"certificate" json:"certificate"`
	Session     session.Config    `yaml:",inline" json:",inline"`
	Link        link.ServerConfig `yaml:"link" json:"link"`
}

// Broker message broker
type Broker struct {
	cfg        Config
	manager    *session.Manager
	transport  *mqtt.Transport
	linkserver *grpc.Server
	log        *log.Logger
}

// NewBroker creates a new broker
func NewBroker(cfg Config) (*Broker, error) {
	var err error
	b := &Broker{
		cfg: cfg,
		log: log.With(log.Any("main", "broker")),
	}
	b.manager, err = session.NewManager(cfg.Session)
	if err != nil {
		return nil, err
	}
	// to start link server
	if cfg.Link.Address != "" {
		b.linkserver, err = link.NewServer(cfg.Link, nil)
		if err != nil {
			b.Close()
			return nil, err
		}
		link.RegisterLinkServer(b.linkserver, b.manager)
	}
	// to start mqtt server
	if len(cfg.Addresses) == 0 && cfg.Link.Address == "" {
		// if no address configured, to use default address
		cfg.Addresses = append(cfg.Addresses, common.DefaultMqttAddress)
	}
	b.transport, err = mqtt.NewTransport(mqtt.ServerConfig{
		Addresses:   cfg.Addresses,
		Certificate: cfg.Certificate,
	}, b.manager.ClientMQTTHandler)
	if err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// Close closes broker
func (b *Broker) Close() {
	if b.transport != nil {
		b.transport.Close()
	}
	if b.linkserver != nil {
		b.log.Info("link server is closing")
		b.linkserver.Stop()
		b.log.Info("link server has closed")
	}
	if b.manager != nil {
		b.manager.Close()
	}
}
