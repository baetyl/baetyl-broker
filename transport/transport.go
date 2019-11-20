package transport

import (
	trans "github.com/256dpi/gomqtt/transport"
	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
)

// Server the server to accept connections
type Server = trans.Server

// Connection the connection between a client and a server
type Connection = trans.Conn

// Handle handles connection
type Handle func(Connection)

// Endpoint the endpoint
type Endpoint struct {
	Address   string
	Anonymous bool
	Handle    Handle
}

// Transport transport
type Transport struct {
	endpoints []*Endpoint
	servers   []Server
	utils.Tomb
}

// NewTransport creates a new transport
func NewTransport(endpoints []*Endpoint, cert *utils.Certificate) (*Transport, error) {
	launcher, err := NewLauncher(cert)
	if err != nil {
		return nil, err
	}
	tp := &Transport{
		endpoints: endpoints,
		servers:   make([]Server, 0),
	}
	for _, endpoint := range endpoints {
		if endpoint.Handle == nil {
			panic("endpoint handle cannot be nil")
		}
		svr, err := launcher.Launch(endpoint.Address)
		if err != nil {
			tp.Close()
			return nil, err
		}
		tp.servers = append(tp.servers, svr)
		tp.accepting(svr, endpoint.Handle)
	}
	log.Info("transport has initialized")
	return tp, nil
}

func (tp *Transport) accepting(svr Server, handle Handle) {
	tp.Go(func() error {
		log.Infof("server (%s) starts to accept", svr.Addr().String())
		defer utils.Trace(log.Infof, "server (%s) has stopped accepting", svr.Addr().String())()

		for {
			conn, err := svr.Accept()
			if err != nil {
				if !tp.Alive() {
					log.Debugf("failed to accept connection: %s", err.Error())
					return nil
				}
				log.Error("failed to accept connection", err)
				return err
			}
			handle(conn)
		}
	})
}

// Close closes service
func (tp *Transport) Close() error {
	log.Info("transport is closing")
	defer log.Info("transport has closed")

	tp.Kill(nil)
	for _, svr := range tp.servers {
		svr.Close()
	}
	return tp.Wait()
}
