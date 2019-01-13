package builder

import (
	"database/sql"

	"github.com/rhinof/grabbit/gbus"
)

type defaultInvocationContext struct {
	invocingSvc string
	bus         gbus.Messaging
	inboundMsg  *gbus.BusMessage
	tx          *sql.Tx
}

func (dfi *defaultInvocationContext) Reply(replyMessage gbus.BusMessage) {
	if dfi.inboundMsg != nil {
		replyMessage.CorrelationID = dfi.inboundMsg.ID
		replyMessage.SagaCorrelationID = dfi.inboundMsg.SagaID
	}
	dfi.bus.Send(dfi.invocingSvc, replyMessage)
}

func (dfi *defaultInvocationContext) Bus() gbus.Messaging {
	return dfi.bus
}

func (dfi *defaultInvocationContext) Tx() *sql.Tx {
	return dfi.tx
}
