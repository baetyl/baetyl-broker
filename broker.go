package main

import (
	"fmt"
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/mqtt"
	"google.golang.org/grpc"
)

type broker struct {
	cfg       config
	manager   *session.Manager
	transport *mqtt.Transport
	server    *grpc.Server
}

func newBroker(cfg config) (*broker, error) {
	var err error
	b := &broker{
		cfg: cfg,
	}
	fmt.Println("cert Path: ", cfg.Certificate.Cert)
	b.manager, err = session.NewManager(cfg.Session, auth.NewAuth(cfg.Principals, cfg.Certificate.Cert))
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
	if len(endpoints) == 0 && cfg.Link.Address == "" {
		// if no address configured, to use default address with anonymous
		endpoints = append(endpoints, &mqtt.Endpoint{
			Address:   "tcp://0.0.0.0:1883",
			Handle:    b.manager.ClientMQTTHandler,
			Anonymous: true,
		})
	}
	b.transport, err = mqtt.NewTransport(endpoints, cfg.Certificate)
	if err != nil {
		b.close()
		return nil, err
	}

	// to start link server
	if cfg.Link.Address != "" {
		b.server, err = link.NewServer(cfg.Link, nil)
		if err != nil {
			b.close()
			return nil, err
		}
		link.RegisterLinkServer(b.server, b.manager)
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
