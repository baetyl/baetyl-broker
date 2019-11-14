package transport

import (
	trans "github.com/256dpi/gomqtt/transport"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// Handle handles connection
type Handle func(trans.Conn)

// Transport transport
type Transport struct {
	servers []trans.Server
	handle  Handle
	tomb    utils.Tomb
}

// NewService creates a new mqtt service
func NewService(addrs []string, cert utils.Certificate, handle Handle) (*Transport, error) {
	launcher, err := NewLauncher(cert)
	if err != nil {
		return nil, err
	}
	s := &Transport{
		servers: make([]trans.Server, 0),
		handle:  handle,
	}
	for _, addr := range addrs {
		svr, err := launcher.Launch(addr)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.servers = append(s.servers, svr)
		s.tomb.Go(func() error {
			for {
				conn, err := svr.Accept()
				if err != nil {
					if !s.tomb.Alive() {
						return nil
					}
					log.Errorf("failed to accept connection")
					continue
				}
				go s.handle(conn)
			}
		})
	}
	return s, nil
}

// Close closes service
func (s *Transport) Close() error {
	s.tomb.Kill(nil)
	for _, svr := range s.servers {
		svr.Close()
	}
	return s.tomb.Wait()
}
