package broker

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/stretchr/testify/assert"
)

var conf = `
listeners:
  - address: tcp://0.0.0.0:1883
  - address: ssl://0.0.0.0:1884
    ca: ../example/var/lib/baetyl/testcert/ca.pem
    key: ../example/var/lib/baetyl/testcert/server.key
    cert: ../example/var/lib/baetyl/testcert/server.pem
  - address: ws://0.0.0.0:8883/mqtt
  - address: wss://0.0.0.0:8884/mqtt
    ca: ../example/var/lib/baetyl/testcert/ca.pem
    key: ../example/var/lib/baetyl/testcert/server.key
    cert: ../example/var/lib/baetyl/testcert/server.pem
principals:
  - username: test
    password: hahaha
    permissions:
      - action: pub
        permit: ["test", "#"]
      - action: sub
        permit: ["test", "#"]
  - username: client.example.org
    permissions:
      - action: pub
        permit: ["test", "#"]
      - action: sub
        permit: ["test", "#"]
session:
  sysTopics: ["$link", "$baidu"]
`

func TestBrokerMqttConnectErrorMissingAddress(t *testing.T) {
	// error: empty url
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorWrongAddress(t *testing.T) {
	// error: address not accessible
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://127.0.0.1:2333"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorMissingClinetID(t *testing.T) {
	// error: clean session must be 1 if client id is zero length
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://0.0.0.0:1883"
	ops.Username = "test"
	ops.Password = "hahaha"
	ops.CleanSession = false
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorCertificate(t *testing.T) {
	// error: x509: cannot validate certificate for 0.0.0.0 because it doesn't contain any IP SANs
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "ssl://0.0.0.0:1884"
	ops.ClientID = "ssl-1"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword1(t *testing.T) {
	// error: connection refused: bad user name or password
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://0.0.0.0:1883"
	ops.ClientID = "tcp-1"
	ops.Username = "test"
	//ops.Password = "hahaha"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword2(t *testing.T) {
	// error: password set without username
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://0.0.0.0:1883"
	ops.ClientID = "tcp-1"
	//ops.Username = "test"
	ops.Password = "hahaha"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectErrorBadUsernameOrPassword3(t *testing.T) {
	//error: connection refused: bad user name or password
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://0.0.0.0:1883"
	ops.Username = "temp"
	ops.Password = "hahaha"
	ops.ClientID = "tcp-1"
	ops.Timeout = time.Millisecond * 1000

	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	obs.assertTimeout()
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectTCPNormal(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for tcp connection, normal
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "tcp://127.0.0.1:1883"
	ops.Username = "test"
	ops.Password = "hahaha"
	ops.ClientID = "tcp-1"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	pkt := &mqtt.Puback{ID: 1}
	obs.assertPkts(pkt)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectSSLNormal(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	tlsconfig, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "../example/var/lib/baetyl/testcert/ca.pem",
		Cert:               "../example/var/lib/baetyl/testcert/client.pem",
		Key:                "../example/var/lib/baetyl/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, tlsconfig)

	// for ssl connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "ssl://0.0.0.0:1884"
	ops.ClientID = "ssl-1"
	ops.TLSConfig = tlsconfig
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	pkt := &mqtt.Puback{ID: 1}
	obs.assertPkts(pkt)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectWebsocketNormal(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	// for websocket connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "ws://0.0.0.0:8883/mqtt"
	ops.ClientID = "websocket-1"
	ops.Username = "test"
	ops.Password = "hahaha"
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	pkt := &mqtt.Puback{ID: 1}
	obs.assertPkts(pkt)
	assert.NoError(t, cli.Close())
}

func TestBrokerMqttConnectWebSocketSSLNormal(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	file := path.Join(dir, "service.yml")
	err = ioutil.WriteFile(file, []byte(conf), 0644)
	assert.NoError(t, err)

	b := initBroker(t, file)
	defer b.Close()

	tlsconfig, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "../example/var/lib/baetyl/testcert/ca.pem",
		Cert:               "../example/var/lib/baetyl/testcert/client.pem",
		Key:                "../example/var/lib/baetyl/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, tlsconfig)

	// for wss(websocket + ssl) connection
	obs := newMockObserver(t)
	ops := mqtt.NewClientOptions()
	ops.Address = "wss://0.0.0.0:8884/mqtt"
	ops.ClientID = "wss-1"
	ops.TLSConfig = tlsconfig
	cli := mqtt.NewClient(ops)

	err = cli.Start(obs)
	assert.NoError(t, err)

	pub2 := newPublishPacket(1, 1, "test", `"name":"topic1"`)
	err = cli.Send(pub2)
	assert.NoError(t, err)

	pkt := &mqtt.Puback{ID: 1}
	obs.assertPkts(pkt)
	assert.NoError(t, cli.Close())
}

func initBroker(t *testing.T, confPath string) *Broker {
	os.RemoveAll("./var")

	var cfg Config
	utils.LoadYAML(confPath, &cfg)
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
		case <-time.After(5 * time.Second):
			panic("nothing received")
		case p := <-o.pkts:
			assert.Equal(o.t, pkt, p)
		}
	}
}

func (o *mockObserver) assertTimeout() {
	select {
	case pkt := <-o.pkts:
		assert.Fail(o.t, "receive unexpected packet:", pkt.String())
	case <-time.After(2 * time.Second):
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

func newPublishPacket(id, qos int, topic, payload string) *mqtt.Publish {
	pub := mqtt.NewPublish()
	pub.ID = mqtt.ID(id)
	pub.Message.Topic = topic
	pub.Message.Payload = []byte(payload)
	pub.Message.QOS = mqtt.QOS(qos)
	return pub
}
