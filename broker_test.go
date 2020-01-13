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

func TestSessionMqttConnectTCP(t *testing.T) {
	var config Config
	utils.LoadYAML(cfg, &config)
	b, err := NewBroker(config)
	assert.NoError(t, err)
	defer b.Close()
	time.Sleep(time.Millisecond * 100)

	// for tcp connection
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = "tcp-1"
	pkt.Username = "test"
	pkt.Password = "hahaha"
	tcpConn, err := dailer.Dial(getURL(b.transport.GetServers()[0], "tcp"))
	assert.NoError(t, err)
	defer tcpConn.Close()
	err = tcpConn.Send(pkt, true)
	assert.NoError(t, err)
	pktRes, err := tcpConn.Receive()
	assert.NoError(t, err)
	pktString := pktRes.String()
	assert.Equal(t, "<Connack SessionPresent=false ReturnCode=0>", pktString)
}

func TestSessionMqttConnectSSL(t *testing.T) {
	var config Config
	utils.LoadYAML(cfg, &config)
	b, err := NewBroker(config)
	assert.NoError(t, err)
	defer b.Close()
	time.Sleep(time.Millisecond * 100)

	// for ssl connection
	ctc, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "./example/testcert/ca.pem",
		Cert:               "./example/testcert/client.pem",
		Key:                "./example/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer := mqtt.NewDialer(ctc, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = "ssl-1"
	sslConn, err := dailer.Dial(getURL(b.transport.GetServers()[1], "ssl"))
	assert.NoError(t, err)
	defer sslConn.Close()
	err = sslConn.Send(pkt, true)
	assert.NoError(t, err)
	pktRes, err := sslConn.Receive()
	assert.NoError(t, err)
	pktString := pktRes.String()
	assert.Equal(t, "<Connack SessionPresent=false ReturnCode=0>", pktString)
}

func TestSessionMqttConnectWebsocket(t *testing.T) {
	var config Config
	utils.LoadYAML(cfg, &config)
	b, err := NewBroker(config)
	assert.NoError(t, err)
	defer b.Close()
	time.Sleep(time.Millisecond * 100)

	// for websocket connection
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = "websocket-1"
	pkt.Username = "test"
	pkt.Password = "hahaha"
	wsConn, err := dailer.Dial(getURL(b.transport.GetServers()[2], "ws") + "/mqtt")
	assert.NoError(t, err)
	defer wsConn.Close()
	err = wsConn.Send(pkt, true)
	assert.NoError(t, err)
	pktRes, err := wsConn.Receive()
	assert.NoError(t, err)
	pktString := pktRes.String()
	assert.Equal(t, "<Connack SessionPresent=false ReturnCode=0>", pktString)
}

func TestSessionMqttConnectWebscoketSSL(t *testing.T) {
	var config Config
	utils.LoadYAML(cfg, &config)
	b, err := NewBroker(config)
	assert.NoError(t, err)
	defer b.Close()
	time.Sleep(time.Millisecond * 100)

	// for wss(websocket + ssl) connection
	ctc, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "./example/testcert/ca.pem",
		Cert:               "./example/testcert/client.pem",
		Key:                "./example/testcert/client.key",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer := mqtt.NewDialer(ctc, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = "wss-1"
	wssConn, err := dailer.Dial(getURL(b.transport.GetServers()[3], "wss") + "/mqtt")
	assert.NoError(t, err)
	defer wssConn.Close()
	err = wssConn.Send(pkt, true)
	pktRes, err := wssConn.Receive()
	assert.NoError(t, err)
	pktString := pktRes.String()
	assert.Equal(t, "<Connack SessionPresent=false ReturnCode=0>", pktString)
	assert.NoError(t, err)
}

// getURL gets URL from the given server and protocol
func getURL(s mqtt.Server, protocol string) string {
	return fmt.Sprintf("%s://%s", protocol, s.Addr().String())
}
