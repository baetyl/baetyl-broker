package listener

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/v2/mqtt"
	"github.com/baetyl/baetyl-go/v2/utils"
	"github.com/stretchr/testify/assert"
)

type mockHandler struct {
	t      *testing.T
	handle func(conn mqtt.Connection)
}

func newMockHandler(t *testing.T) *mockHandler {
	return &mockHandler{
		t: t,
		handle: func(conn mqtt.Connection) {
			p, err := conn.Receive()
			t.Log(p, err)
			assert.NoError(t, err)
			err = conn.Send(p, false)
			t.Log(err)
			assert.NoError(t, err)
		},
	}
}

func (m *mockHandler) Handle(conn mqtt.Connection) {
	if m.handle != nil {
		m.handle(conn)
	}
}

func TestMqttTcp(t *testing.T) {
	cfg := []Listener{
		{Address: "tcp://:0"},
		{Address: "tcp://127.0.0.1:0"},
	}
	m, err := NewManager(cfg, newMockHandler(t))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(m.mqtts))
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	// TODO: test timeout
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = m.mqtts[0].Addr().String()
	conn, err := dailer.Dial(getURL(m.mqtts[0], "tcp"))
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()

	pkt.ClientID = m.mqtts[1].Addr().String()
	conn, err = dailer.Dial(getURL(m.mqtts[1], "tcp"))
	assert.NoError(t, err)
	err = conn.Send(pkt, true)
	assert.NoError(t, err)
	res, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()
}

func TestMqttTcpTls(t *testing.T) {
	count := int32(0)
	handler := newMockHandler(t)
	handler.handle = func(conn mqtt.Connection) {
		c := atomic.AddInt32(&count, 1)
		p, err := conn.Receive()
		if c == 1 {
			assert.EqualError(t, err, "remote error: tls: bad certificate")
			assert.Nil(t, p)
			return
		}
		assert.NoError(t, err)
		assert.NotNil(t, p)
		cn, isBidAuth := mqtt.GetTLSCommonName(conn)
		assert.True(t, isBidAuth)
		assert.NotNil(t, cn)
		assert.Equal(t, "client.example.org", cn)
		err = conn.Send(p, false)
		assert.NoError(t, err)
	}

	cfg := []Listener{
		{
			Address: "ssl://localhost:0",
			Certificate: utils.Certificate{
				CA:   "../example/var/lib/baetyl/testcert/ca.pem", // ca.pem is a certificate chain
				Key:  "../example/var/lib/baetyl/testcert/server.key",
				Cert: "../example/var/lib/baetyl/testcert/server.pem",
			},
		},
	}
	m, err := NewManager(cfg, handler)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m.mqtts))
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	url := getURL(m.mqtts[0], "ssl")
	pkt := mqtt.NewConnect()
	pkt.ClientID = m.mqtts[0].Addr().String()

	// count: 1
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	conn, err := dailer.Dial(url)
	assert.Nil(t, conn)
	switch err.Error() {
	case "x509: certificate signed by unknown authority":
	case "x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs":
	default:
		assert.FailNow(t, "error expected")
	}

	// count: 2
	tlscli, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "../example/var/lib/baetyl/testcert/ca.pem", // ca.pem is a certificate chain
		Key:                "../example/var/lib/baetyl/testcert/client.key",
		Cert:               "../example/var/lib/baetyl/testcert/client.pem",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer = mqtt.NewDialer(tlscli, time.Duration(0))
	conn, err = dailer.Dial(url)
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()
}

func TestMqttTcpTlsDebug(t *testing.T) {
	// for debug
	t.Skip(t.Name())

	url := "ssl://0.0.0.0:1884"
	pkt := mqtt.NewConnect()
	pkt.ClientID = t.Name()
	pkt.CleanSession = true
	pkt.Username = ""

	tlscli, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "../example/var/lib/baetyl/testcert/ca.pem", // ca.pem is a certificate chain
		Key:                "../example/var/lib/baetyl/testcert/client.key",
		Cert:               "../example/var/lib/baetyl/testcert/client.pem",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer := mqtt.NewDialer(tlscli, time.Duration(0))
	conn, err := dailer.Dial(url)
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, res.String(), "<Connack SessionPresent=false ReturnCode=4>")
	_, err = conn.Receive()
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "EOF")
}

func TestMqttWebSocket(t *testing.T) {
	cfg := []Listener{
		{Address: "ws://localhost:0"},
		{Address: "ws://127.0.0.1:0/mqtt"},
	}
	m, err := NewManager(cfg, newMockHandler(t))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(m.mqtts))
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	dailer := mqtt.NewDialer(nil, time.Duration(0))
	pkt := mqtt.NewConnect()
	pkt.ClientID = m.mqtts[0].Addr().String()
	conn, err := dailer.Dial(getURL(m.mqtts[0], "ws"))
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()

	pkt.ClientID = m.mqtts[1].Addr().String()
	conn, err = dailer.Dial(getURL(m.mqtts[1], "ws") + "/mqtt")
	assert.NoError(t, err)
	err = conn.Send(pkt, true)
	assert.NoError(t, err)
	res, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()

	pkt.ClientID = m.mqtts[1].Addr().String() + "-1"
	conn, err = dailer.Dial(getURL(m.mqtts[1], "ws") + "/notexist")
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()
}

func TestMqttWebSocketTls(t *testing.T) {
	handler := newMockHandler(t)
	handler.handle = func(conn mqtt.Connection) {
		p, err := conn.Receive()
		assert.NoError(t, err)
		assert.NotNil(t, p)
		cn, isBidAuth := mqtt.GetTLSCommonName(conn)
		assert.True(t, isBidAuth)
		assert.NotNil(t, cn)
		assert.Equal(t, "client.example.org", cn)
		err = conn.Send(p, false)
		assert.NoError(t, err)
	}
	cfg := []Listener{
		{
			Address: "wss://localhost:0/mqtt",
			Certificate: utils.Certificate{
				CA:   "../example/var/lib/baetyl/testcert/ca.pem", // ca.pem is a certificate chain
				Key:  "../example/var/lib/baetyl/testcert/server.key",
				Cert: "../example/var/lib/baetyl/testcert/server.pem",
			},
		},
	}
	m, err := NewManager(cfg, handler)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(m.mqtts))
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	url := getURL(m.mqtts[0], "wss") + "/mqtt"
	pkt := mqtt.NewConnect()
	pkt.ClientID = m.mqtts[0].Addr().String()

	// count: 1
	dailer := mqtt.NewDialer(nil, time.Duration(0))
	conn, err := dailer.Dial(url)
	assert.Nil(t, conn)
	switch err.Error() {
	case "x509: certificate signed by unknown authority":
	case "x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs":
	default:
		assert.FailNow(t, "error expected")
	}

	// count: 2
	ctc, err := utils.NewTLSConfigClient(utils.Certificate{
		CA:                 "../example/var/lib/baetyl/testcert/ca.pem", // ca.pem is a certificate chain
		Key:                "../example/var/lib/baetyl/testcert/client.key",
		Cert:               "../example/var/lib/baetyl/testcert/client.pem",
		InsecureSkipVerify: true,
	})
	assert.NoError(t, err)
	dailer = mqtt.NewDialer(ctc, time.Duration(0))
	conn, err = dailer.Dial(url)
	assert.NoError(t, err)
	err = conn.Send(pkt, false)
	assert.NoError(t, err)
	res, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, pkt.String(), res.String())
	conn.Close()
}

func TestServerException(t *testing.T) {
	cfg := []Listener{
		{Address: "tcp://:28767"},
		{Address: "tcp://:28767"},
	}
	_, err := NewManager(cfg, newMockHandler(t))
	switch err.Error() {
	case "listen tcp :28767: bind: address already in use":
	case "listen tcp :28767: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.":
	default:
		assert.FailNow(t, "error expected")
	}

	cfg = []Listener{
		{Address: "tcp://:28767"},
		{Address: "ssl://:28767"},
	}
	_, err = NewManager(cfg, newMockHandler(t))
	assert.EqualError(t, err, "tls: neither Certificates nor GetCertificate set in Config")

	cfg = []Listener{
		{Address: "ws://:28767/v1"},
		{Address: "wss://:28767/v2"},
	}
	_, err = NewManager(cfg, newMockHandler(t))
	assert.EqualError(t, err, "tls: neither Certificates nor GetCertificate set in Config")

	cfg = []Listener{
		{Address: "ws://:28767/v1"},
		{Address: "ws://:28767/v1"},
	}
	_, err = NewManager(cfg, newMockHandler(t))
	switch err.Error() {
	case "listen tcp :28767: bind: address already in use":
	case "listen tcp :28767: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.":
	default:
		assert.FailNow(t, "error expected")
	}

	// TODO: test more special case
	// cfg = []string{"ws://:28767/v1", "ws://0.0.0.0:28767/v2"}
	// cfg = []string{"ws://localhost:28767/v1", "ws://127.0.0.1:28767/v2"}
}

// func getPort(s Server) string {
// 	_, port, _ := net.SplitHostPort(s.Addr().String())
// 	return port
// }

func getURL(s mqtt.Server, protocol string) string {
	return fmt.Sprintf("%s://%s", protocol, s.Addr().String())
}
