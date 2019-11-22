package main

import (
	"github.com/baetyl/baetyl-broker/auth"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils/log"
)

type broker struct {
	cfg       config
	store     *session.Store
	transport *transport.Transport
}

func newBroker(cfg config) (*broker, error) {

	var err error
	b := &broker{
		cfg: cfg,
	}

	b.store, err = session.NewStore(cfg.Session, NewExchange(), auth.NewAuth(cfg.Principals))
	if err != nil {
		return nil, err
	}
	log.Info("session store starts")

	endpoints := []*transport.Endpoint{}
	for _, addr := range cfg.Addresses {
		endpoints = append(endpoints, &transport.Endpoint{
			Address: addr,
			Handle:  b.store.NewClientMQTT,
		})
	}
	if !cfg.InternalEndpoint.Disable {
		endpoints = append(endpoints, &transport.Endpoint{
			Address:   cfg.InternalEndpoint.Address,
			Handle:    b.store.NewClientMQTT,
			Anonymous: true,
		})
	}
	b.transport, err = transport.NewTransport(endpoints, nil)
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
	if b.store != nil {
		b.store.Close()
	}
}
