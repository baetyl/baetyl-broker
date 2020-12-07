package session

import (
	"time"

	"github.com/baetyl/baetyl-go/v2/mqtt"

	"github.com/baetyl/baetyl-broker/v2/common"
)

type eventWrapper struct {
	*common.Event
	id  uint64
	qos mqtt.QOS
	lst time.Time // last send time
}

func newEventWrapper(id uint64, qos mqtt.QOS, evt *common.Event) *eventWrapper {
	return &eventWrapper{
		Event: evt,
		id:    id,
		qos:   qos,
		lst:   time.Now(),
	}
}

func (i *eventWrapper) packet(dup bool) *mqtt.Publish {
	pkt := i.Packet()
	pkt.ID = mqtt.ID(i.id)
	pkt.Dup = dup
	pkt.Message.QOS = i.qos
	return pkt
}
