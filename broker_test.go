package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/mqtt"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
)

const (
	cfg = "./example/etc/baetyl/service.yml"
)

func TestSessionMqttConnect(t *testing.T) {
	var config Config
	utils.LoadYAML(cfg, &config)
	b, err := NewBroker(config)
	assert.NoError(t, err)
	assert.True(t, b.transport.Alive())
	defer b.Close()
	time.Sleep(time.Millisecond * 100)

	// for tcp connection
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = b.transport.GetServers()[0].Addr().String()
	conn, err := dailer.Dial(getURL(b.transport.GetServers()[0], "tcp"))
	assert.NoError(t, err)
	err = conn.Send(pkt, true)
	assert.NoError(t, err)
	conn.Close()

	// for ssl connection
	ctc, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "./example/testcert/ca.pem",
		Cert:               "./example/testcert/client.pem",
		Key:                "./example/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer = mqtt.NewDialer(ctc, time.Duration(0))
	pkt.ClientID = b.transport.GetServers()[1].Addr().String()
	conn, err = dailer.Dial(getURL(b.transport.GetServers()[1], "ssl"))
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	conn.Close()

	// for websocket connection
	dailer = mqtt.NewDialer(nil, time.Duration(0))
	pkt.ClientID = b.transport.GetServers()[2].Addr().String()
	conn, err = dailer.Dial(getURL(b.transport.GetServers()[2], "ws") + "/mqtt")
	assert.NoError(t, err)
	err = conn.Send(pkt, true)
	assert.NoError(t, err)
	conn.Close()

	// for wss(websocket + ssl) connection
	ctc, err = utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "./example/testcert/ca.pem",
		Cert:               "./example/testcert/client.pem",
		Key:                "./example/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer = mqtt.NewDialer(ctc, time.Duration(0))
	pkt.ClientID = b.transport.GetServers()[3].Addr().String()
	conn, err = dailer.Dial(getURL(b.transport.GetServers()[3], "wss") + "/mqtt")
	assert.NoError(t, err)
	defer conn.Close()
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
}

// getURL gets URL from the given server and protocol
func getURL(s mqtt.Server, protocol string) string {
	return fmt.Sprintf("%s://%s", protocol, s.Addr().String())
}
