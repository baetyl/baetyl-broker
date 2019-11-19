package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/trace"
	"syscall"

	"github.com/baetyl/baetyl-broker/exchange"
	"github.com/baetyl/baetyl-broker/session"
	"github.com/baetyl/baetyl-broker/transport"
	"github.com/baetyl/baetyl-broker/utils/log"
	"github.com/creasty/defaults"
	_ "github.com/mattn/go-sqlite3"
)

// Config all config of edge
type config struct {
	Session          session.Config `yaml:"session" json:"session"`
	Addresses        []string       `yaml:"addresses" json:"addresses"`
	InternalEndpoint struct {
		Disable bool   `yaml:"disable" json:"disable"`
		Address string `yaml:"address" json:"address"`
		// Anonymous bool `yaml:"anonymous" json:"anonymous"`
	} `yaml:"internalEndpoint" json:"internalEndpoint"`
}

type broker struct {
	cfg       config
	store     *session.Store
	transport *transport.Transport
}

func newBroker(cfg config) (*broker, error) {

	var err error
	b := &broker{cfg: cfg}

	b.store, err = session.NewStore(cfg.Session, exchange.NewExchange())
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

func main() {

	// go tool pprof http://localhost:6060/debug/pprof/profile
	go func() {
		err := http.ListenAndServe("localhost:6060", nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Start profile failed: ", err.Error())
			return
		}
	}()

	f, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = trace.Start(f)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()

	// baetyl.Run(func(ctx baetyl.Context) error {
	var cfg config
	defaults.Set(&cfg)
	cfg.Addresses = []string{"tcp://127.0.0.1:11883"}
	b, err := newBroker(cfg)
	if err != nil {
		log.Fatal("failed to create broker", err)
	}
	defer b.close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE)
	<-sig
	// })
}
