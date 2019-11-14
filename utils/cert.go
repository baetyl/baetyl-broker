package utils

import (
	"crypto/tls"
	"net"

	"github.com/256dpi/gomqtt/transport"
	"github.com/docker/go-connections/tlsconfig"
)

// Certificate certificate config for mqtt server
type Certificate struct {
	CA       string `yaml:"ca" json:"ca"`
	Key      string `yaml:"key" json:"key"`
	Cert     string `yaml:"cert" json:"cert"`
	Insecure bool   `yaml:"insecure" json:"insecure"` // for client, for test purpose
}

// NewTLSConfigServer loads tls config for server
func NewTLSConfigServer(c Certificate) (*tls.Config, error) {
	if c.Cert == "" && c.Key == "" {
		return nil, nil
	}
	return tlsconfig.Server(tlsconfig.Options{CAFile: c.CA, KeyFile: c.Key, CertFile: c.Cert, ClientAuth: tls.VerifyClientCertIfGiven})
}

// NewTLSConfigClient loads tls config for client
func NewTLSConfigClient(c Certificate) (*tls.Config, error) {
	return tlsconfig.Client(tlsconfig.Options{CAFile: c.CA, KeyFile: c.Key, CertFile: c.Cert, InsecureSkipVerify: c.Insecure})
}

// IsBidirectionalAuthentication check bidirectional authentication
func IsBidirectionalAuthentication(conn transport.Conn) bool {
	var inner net.Conn
	if tcps, ok := conn.(*transport.NetConn); ok {
		inner = tcps.UnderlyingConn()
	} else if wss, ok := conn.(*transport.WebSocketConn); ok {
		inner = wss.UnderlyingConn().UnderlyingConn()
	}
	tlsconn, ok := inner.(*tls.Conn)
	if !ok {
		return false
	}
	state := tlsconn.ConnectionState()
	if !state.HandshakeComplete {
		return false
	}
	return len(state.PeerCertificates) > 0
}
