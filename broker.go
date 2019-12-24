package main

import (
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-go/mqtt"
)

type broker struct {
	cfg       config
	manager   *session.Manager
	transport *mqtt.Transport
}

func newBroker(cfg config) (*broker, error) {
	var err error
	b := &broker{
		cfg: cfg,
	}
	b.manager, err = session.NewManager(cfg.Session, NewExchange(cfg.SysTopics), auth.NewAuth(cfg.Principals))
	if err != nil {
		return nil, err
	}

	var endpoints []*mqtt.Endpoint
	for _, addr := range cfg.Addresses {
		endpoints = append(endpoints, &mqtt.Endpoint{
			Address: addr,
			Handle:  b.manager.ClientMQTTHandler,
		})
	}
	b.transport, err = mqtt.NewTransport(endpoints, cfg.Certificate)
	if err != nil {
		b.close()
		return nil, err
	}

	return b, nil
}

func (b *broker) close() {
	if b.transport != nil {
		b.transport.Close()
	}
	if b.manager != nil {
		b.manager.Close()
	}
}
