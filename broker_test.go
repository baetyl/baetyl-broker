package main

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
)

const (
	cfgFile = "./example/etc/baetyl/service.yml"
)

func TestBrokerMqttConnectErrorMissingAddress(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("parse : empty url"))
}

func TestBrokerMqttConnectErrorWrongAddress(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address: "tcp://127.0.0.1:2333",
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("dial tcp 127.0.0.1:2333: connect: connection refused"))
}

func TestBrokerMqttConnectErrorMissingClinetID(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:      "tcp://0.0.0.0:1883",
		Username:     "test",
		Password:     "hahaha",
		CleanSession: false,
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("clean session must be 1 if client id is zero length"))
}

func TestBrokerMqttConnectErrorCertificate(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "ssl://0.0.0.0:1884",
		ClientID: "ssl-1",
		Timeout:  time.Millisecond * 100,
		// Certificate: utils.Certificate{
		// 	CA:                 "./example/var/lib/baetyl/testcert/ca.pem",
		// 	Cert:               "./example/var/lib/baetyl/testcert/client.pem",
		// 	Key:                "./example/var/lib/baetyl/testcert/client.key",
		// 	InsecureSkipVerify: true,
		// },
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("x509: cannot validate certificate for 0.0.0.0 because it doesn't contain any IP SANs"))
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword1(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "tcp://0.0.0.0:1883",
		Username: "test",
		// Password: "hahaha",
		ClientID: "tcp-1",
		Timeout:  time.Millisecond * 100,
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("connection refused: bad user name or password"))
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword2(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address: "tcp://0.0.0.0:1883",
		// Username: "test",
		Password: "hahaha",
		ClientID: "tcp-1",
		Timeout:  time.Millisecond * 100,
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("password set without username"))
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword3(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "tcp://0.0.0.0:1883",
		Username: "temp",
		Password: "hahaha",
		ClientID: "tcp-1",
		Timeout:  time.Millisecond * 100,
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	defer cli.Close()

	obs.assertErrs(errors.New("connection refused: bad user name or password"))
}

func TestBrokerMqttConnectTCPNormal(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for tcp connection, normal
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "tcp://127.0.0.1:1883",
		Username: "test",
		Password: "hahaha",
		ClientID: "tcp-1",
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectSSLNormal(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for ssl connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "ssl://0.0.0.0:1884",
		ClientID: "ssl-1",
		Certificate: utils.Certificate{
			CA:                 "./example/var/lib/baetyl/testcert/ca.pem",
			Cert:               "./example/var/lib/baetyl/testcert/client.pem",
			Key:                "./example/var/lib/baetyl/testcert/client.key",
			InsecureSkipVerify: true,
		},
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectWebsocketNormal(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for websocket connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "ws://0.0.0.0:8883/mqtt",
		Username: "test",
		Password: "hahaha",
		ClientID: "websocket-1",
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectWebSocketSSLNormal(t *testing.T) {
	b := initBroker(t)
	defer b.Close()

	// for wss(websocket + ssl) connection
	obs := newMockObserver(t)
	cli, err := mqtt.NewClient(mqtt.ClientConfig{
		Address:  "wss://0.0.0.0:8884/mqtt",
		ClientID: "wss-1",
		Certificate: utils.Certificate{
			CA:                 "./example/var/lib/baetyl/testcert/ca.pem",
			Cert:               "./example/var/lib/baetyl/testcert/client.pem",
			Key:                "./example/var/lib/baetyl/testcert/client.key",
			InsecureSkipVerify: true,
		},
	}, obs)
	assert.NoError(t, err)
	assert.NotNil(t, cli)
	assert.NoError(t, cli.Close())
}

func initBroker(t *testing.T) *Broker {
	os.RemoveAll("./var")
	var cfg Config
	utils.LoadYAML(cfgFile, &cfg)
	b, err := NewBroker(cfg)
	assert.NoError(t, err)
	return b
}

type mockObserver struct {
	t    *testing.T
	pkts chan mqtt.Packet
	errs chan error
}

func newMockObserver(t *testing.T) *mockObserver {
	return &mockObserver{
		t:    t,
		pkts: make(chan mqtt.Packet, 10),
		errs: make(chan error, 10),
	}
}

func (o *mockObserver) OnPublish(pkt *mqtt.Publish) error {
	fmt.Println("--> OnPublish:", pkt)
	o.pkts <- pkt
	return nil
}

func (o *mockObserver) OnPuback(pkt *mqtt.Puback) error {
	fmt.Println("--> OnPuback:", pkt)
	o.pkts <- pkt
	return nil
}

func (o *mockObserver) OnError(err error) {
	fmt.Println("--> OnError:", err)
	o.errs <- err
}

func (o *mockObserver) assertPkts(pkts ...mqtt.Packet) {
	for _, pkt := range pkts {
		select {
		case <-time.After(6 * time.Minute):
			panic("nothing received")
		case p := <-o.pkts:
			assert.Equal(o.t, pkt, p)
		}
	}
}

func (o *mockObserver) assertErrs(errs ...error) {
	for _, err := range errs {
		select {
		case <-time.After(100 * time.Millisecond):
			panic("nothing received")
		case e := <-o.errs:
			assert.Equal(o.t, err.Error(), e.Error())
		}
	}
}
