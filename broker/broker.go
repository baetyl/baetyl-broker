package broker

import (
	"github.com/baetyl/baetyl-broker/v2/listener"
	"github.com/baetyl/baetyl-broker/v2/session"
	"github.com/baetyl/baetyl-go/v2/log"
)

// Config all config of broker
type Config struct {
	Listeners []listener.Listener `yaml:"listeners" json:"listeners"`
	Session   session.Config      `yaml:",inline" json:",inline"`
}

// Broker message broker
type Broker struct {
	cfg Config
	ses *session.Manager
	lis *listener.Manager
	log *log.Logger
}

// NewBroker creates a new broker
func NewBroker(cfg Config) (*Broker, error) {
	var err error
	b := &Broker{
		cfg: cfg,
		log: log.With(log.Any("main", "broker")),
	}
	b.ses, err = session.NewManager(cfg.Session)
	if err != nil {
		return nil, err
	}

	b.lis, err = listener.NewManager(cfg.Listeners, b.ses)
	if err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// Close closes broker
func (b *Broker) Close() {
	if b.lis != nil {
		b.lis.Close()
	}
	if b.ses != nil {
		b.ses.Close()
	}
}
