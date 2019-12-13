package transport

import (
	"fmt"
	"net"
	"testing"
	"time"

	trans "github.com/256dpi/gomqtt/transport"
	"github.com/baetyl/baetyl-broker/common"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/stretchr/testify/assert"
)

func TestMqttTcp(t *testing.T) {
	handle := func(conn Connection, _ bool) {
		p, err := conn.Receive()
		assert.NoError(t, err)
		err = conn.Send(p, false)
		assert.NoError(t, err)
	}
	endpoints := []*Endpoint{
		&Endpoint{
			Address: "tcp://:0",
			Handle:  handle,
		},
		&Endpoint{
			Address: "tcp://127.0.0.1:0",
			Handle:  handle,
		},
	}
	m, err := NewTransport(endpoints, nil)
	assert.NoError(t, err)
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	dailer, err := NewDialer(nil)
	request := &common.Connect{CleanSession: true, Version: 4}
	request.ClientID = m.servers[0].Addr().String()
	conn, err := dailer.Dial(getURL(m.servers[0], "tcp"))
	assert.NoError(t, err)
	err = conn.Send(request, false)
	assert.NoError(t, err)
	response, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, request.String(), response.String())
	conn.Close()

	request.ClientID = m.servers[1].Addr().String()
	conn, err = dailer.Dial(getURL(m.servers[1], "tcp"))
	assert.NoError(t, err)
	err = conn.Send(request, true)
	assert.NoError(t, err)
	response, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, request.String(), response.String())
	conn.Close()
}

// func TestMqttTcpTls(t *testing.T) {
// 	count := int32(0)
// 	handle := func(conn Connection, _ bool) {
// 		c := atomic.AddInt32(&count, 1)
// 		p, err := conn.Receive()

// 		assert.NoError(t, err)
// 		assert.NotNil(t, p)

// 		ok := utils.IsBidirectionalAuthentication(conn)
// 		if c == 3 {
// 			assert.Truef(t, ok, "count: %d", c)
// 		} else {
// 			assert.Falsef(t, ok, "count: %d", c)
// 		}

// 		err = conn.Send(p, false)
// 		assert.NoError(t, err)
// 	}
// 	endpoints := []*Endpoint{
// 		&Endpoint{
// 			Address: "ssl://localhost:0",
// 			Handle:  handle,
// 		},
// 	}
// 	cert := &utils.Certificate{
// 		CA:   "./testcert/ca.pem",
// 		Key:  "./testcert/server.key",
// 		Cert: "./testcert/server.pem",
// 	}
// 	m, err := NewTransport(endpoints, cert)
// 	assert.NoError(t, err)
// 	defer m.Close()
// 	time.Sleep(time.Millisecond * 100)

// 	url := getURL(m.servers[0], "ssl")
// 	request := packet.NewConnect()
// 	request.ClientID = m.servers[0].Addr().String()

// 	// count: 1
// 	dailer, err := NewDialer(&utils.Certificate{Insecure: true})
// 	assert.NoError(t, err)
// 	conn, err := dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err := conn.Receive()
// 	assert.NoError(t, err)
// 	conn.Close()

// 	// count: 2
// 	dailer, err = NewDialer(&utils.Certificate{
// 		CA:       "./testcert/ca.pem",
// 		Insecure: true,
// 	})
// 	assert.NoError(t, err)
// 	conn, err = dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err = conn.Receive()
// 	assert.NoError(t, err)
// 	assert.Equal(t, request.String(), response.String())
// 	conn.Close()

// 	// count: 3
// 	dailer, err = NewDialer(&utils.Certificate{
// 		CA:       "./testcert/ca.pem",
// 		Key:      "./testcert/testssl2.key",
// 		Cert:     "./testcert/testssl2.pem",
// 		Insecure: true,
// 	})
// 	assert.NoError(t, err)
// 	conn, err = dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err = conn.Receive()
// 	assert.NoError(t, err)
// 	assert.Equal(t, request.String(), response.String())
// 	conn.Close()
// }

func TestMqttWebSocket(t *testing.T) {
	handle := func(conn Connection, _ bool) {
		p, err := conn.Receive()
		assert.NoError(t, err)
		err = conn.Send(p, false)
		assert.NoError(t, err)
	}
	endpoints := []*Endpoint{
		&Endpoint{
			Address: "ws://localhost:0",
			Handle:  handle,
		},
		&Endpoint{
			Address: "ws://127.0.0.1:0/mqtt",
			Handle:  handle,
		},
	}
	m, err := NewTransport(endpoints, nil)
	assert.NoError(t, err)
	defer m.Close()
	time.Sleep(time.Millisecond * 100)

	dailer, err := NewDialer(&utils.Certificate{InsecureSkipVerify: true})
	assert.NoError(t, err)
	request := &common.Connect{CleanSession: true, Version: 4}
	request.ClientID = m.servers[0].Addr().String()
	conn, err := dailer.Dial(getURL(m.servers[0], "ws"))
	assert.NoError(t, err)
	err = conn.Send(request, false)
	assert.NoError(t, err)
	response, err := conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, request.String(), response.String())
	conn.Close()

	request.ClientID = m.servers[1].Addr().String()
	conn, err = dailer.Dial(getURL(m.servers[1], "ws") + "/mqtt")
	assert.NoError(t, err)
	err = conn.Send(request, true)
	assert.NoError(t, err)
	response, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, request.String(), response.String())
	conn.Close()

	request.ClientID = m.servers[1].Addr().String() + "-1"
	conn, err = dailer.Dial(getURL(m.servers[1], "ws") + "/notexist")
	assert.NoError(t, err)
	err = conn.Send(request, false)
	assert.NoError(t, err)
	response, err = conn.Receive()
	assert.NoError(t, err)
	assert.Equal(t, request.String(), response.String())
	conn.Close()
}

// func TestMqttWebSocketTls(t *testing.T) {
// 	count := int32(0)
// 	handle := func(conn Connection, _ bool) {
// 		c := atomic.AddInt32(&count, 1)
// 		fmt.Println(count, conn.LocalAddr())
// 		p, err := conn.Receive()
// 		assert.NoError(t, err)
// 		assert.NotNil(t, p)

// 		ok := utils.IsBidirectionalAuthentication(conn)
// 		if c == 3 {
// 			assert.Truef(t, ok, "count: %d", c)
// 		} else {
// 			assert.Falsef(t, ok, "count: %d", c)
// 		}

// 		err = conn.Send(p, false)
// 		assert.NoError(t, err)
// 	}
// 	endpoints := []*Endpoint{
// 		&Endpoint{
// 			Address: "wss://localhost:0/mqtt",
// 			Handle:  handle,
// 		},
// 	}
// 	cert := &utils.Certificate{
// 		CA:   "./testcert/ca.pem",
// 		Key:  "./testcert/server.key",
// 		Cert: "./testcert/server.pem",
// 	}
// 	m, err := NewTransport(endpoints, cert)
// 	assert.NoError(t, err)
// 	defer m.Close()
// 	time.Sleep(time.Millisecond * 100)

// 	url := getURL(m.servers[0], "wss") + "/mqtt"
// 	request := packet.NewConnect()
// 	request.ClientID = m.servers[0].Addr().String()

// 	dailer, err := NewDialer(nil)
// 	assert.NoError(t, err)
// 	conn, err := dailer.Dial(url)
// 	assert.Nil(t, conn)
// 	switch err.Error() {
// 	case "x509: certificate signed by unknown authority":
// 	case "x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs":
// 	default:
// 		assert.FailNow(t, "error expected")
// 	}

// 	// count: 1
// 	dailer, err = NewDialer(&utils.Certificate{Insecure: true})
// 	assert.NoError(t, err)
// 	conn, err = dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err := conn.Receive()
// 	assert.NoError(t, err)
// 	conn.Close()

// 	// count: 2
// 	dailer, err = NewDialer(&utils.Certificate{
// 		CA:       "./testcert/ca.pem",
// 		Insecure: true,
// 	})
// 	assert.NoError(t, err)
// 	conn, err = dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err = conn.Receive()
// 	assert.NoError(t, err)
// 	conn.Close()

// 	// count: 3
// 	dailer, err = NewDialer(&utils.Certificate{
// 		CA:       "./testcert/ca.pem",
// 		Key:      "./testcert/testssl2.key",
// 		Cert:     "./testcert/testssl2.pem",
// 		Insecure: true,
// 	})
// 	assert.NoError(t, err)
// 	conn, err = dailer.Dial(url)
// 	assert.NoError(t, err)
// 	err = conn.Send(request, false)
// 	assert.NoError(t, err)
// 	response, err = conn.Receive()
// 	assert.NoError(t, err)
// 	assert.Equal(t, request.String(), response.String())
// 	conn.Close()
// }

func TestServerException(t *testing.T) {
	handle := func(conn Connection, _ bool) {
		p, err := conn.Receive()
		assert.NoError(t, err)
		err = conn.Send(p, false)
		assert.NoError(t, err)
	}
	endpoints := []*Endpoint{
		&Endpoint{
			Address: "tcp://:28767",
			Handle:  handle,
		},
		&Endpoint{
			Address: "tcp://:28767",
			Handle:  handle,
		},
	}
	_, err := NewTransport(endpoints, nil)
	switch err.Error() {
	case "listen tcp :28767: bind: address already in use":
	case "listen tcp :28767: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.":
	default:
		assert.FailNow(t, "error expected")
	}

	endpoints = []*Endpoint{
		&Endpoint{
			Address: "tcp://:28767",
			Handle:  handle,
		},
		&Endpoint{
			Address: "ssl://:28767",
			Handle:  handle,
		},
	}
	_, err = NewTransport(endpoints, nil)
	assert.EqualError(t, err, "tls: neither Certificates nor GetCertificate set in Config")

	endpoints = []*Endpoint{
		&Endpoint{
			Address: "ws://:28767/v1",
			Handle:  handle,
		},
		&Endpoint{
			Address: "wss://:28767/v2",
			Handle:  handle,
		},
	}
	_, err = NewTransport(endpoints, nil)
	assert.EqualError(t, err, "tls: neither Certificates nor GetCertificate set in Config")

	endpoints = []*Endpoint{
		&Endpoint{
			Address: "ws://:28767/v1",
			Handle:  handle,
		},
		&Endpoint{
			Address: "ws://:28767/v1",
			Handle:  handle,
		},
	}
	_, err = NewTransport(endpoints, nil)
	switch err.Error() {
	case "listen tcp :28767: bind: address already in use":
	case "listen tcp :28767: bind: Only one usage of each socket address (protocol/network address/port) is normally permitted.":
	default:
		assert.FailNow(t, "error expected")
	}

	// TODO: test more special case
	// endpoints = []string{"ws://:28767/v1", "ws://0.0.0.0:28767/v2"}
	// endpoints = []string{"ws://localhost:28767/v1", "ws://127.0.0.1:28767/v2"}
}

func getPort(s trans.Server) string {
	_, port, _ := net.SplitHostPort(s.Addr().String())
	return port
}

func getURL(s trans.Server, protocol string) string {
	return fmt.Sprintf("%s://%s", protocol, s.Addr().String())
}
