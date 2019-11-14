package transport

import (
	"net/url"

	trans "github.com/256dpi/gomqtt/transport"
	"github.com/baetyl/baetyl-broker/utils"
)

// Dialer handles connecting to a server and creating a connection.
type Dialer struct {
	*trans.Dialer
}

// NewDialer returns a new Dialer.
func NewDialer(c utils.Certificate) (*Dialer, error) {
	tls, err := utils.NewTLSConfigClient(c)
	if err != nil {
		return nil, err
	}
	d := &Dialer{Dialer: trans.NewDialer()}
	d.TLSConfig = tls
	return d, nil
}

// Dial initiates a connection based in information extracted from an URL.
func (d *Dialer) Dial(urlString string) (trans.Conn, error) {
	uri, err := url.ParseRequestURI(urlString)
	if err != nil {
		return nil, err
	}
	if uri.Scheme == "ssl" {
		uri.Scheme = "tls"
	}
	return d.Dialer.Dial(uri.String())
}
