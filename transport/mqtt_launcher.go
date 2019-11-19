package transport

import (
	"net/url"

	trans "github.com/256dpi/gomqtt/transport"
	"github.com/baetyl/baetyl-broker/utils"
)

// The Launcher helps with launching a server and accepting connections.
type Launcher struct {
	trans.Launcher
}

// NewLauncher returns a new Launcher.
func NewLauncher(c *utils.Certificate) (*Launcher, error) {
	t, err := utils.NewTLSConfigServer(c)
	if err != nil {
		return nil, err
	}
	return &Launcher{Launcher: trans.Launcher{TLSConfig: t}}, nil
}

// Launch will launch a server based on information extracted from an URL.
func (l *Launcher) Launch(urlString string) (trans.Server, error) {
	uri, err := url.ParseRequestURI(urlString)
	if err != nil {
		return nil, err
	}
	if uri.Scheme == "ssl" {
		uri.Scheme = "tls"
	}
	return l.Launcher.Launch(uri.String())
}
