package gbus

import (
	"database/sql"
)

type defaultInvocationContext struct {
	invocingSvc string
	bus         Messaging
	inboundMsg  *BusMessage
	tx          *sql.Tx
}

func (dfi *defaultInvocationContext) Reply(replyMessage BusMessage) {
	if dfi.inboundMsg != nil {
		replyMessage.CorrelationID = dfi.inboundMsg.ID
		replyMessage.SagaCorrelationID = dfi.inboundMsg.SagaID
	}
	dfi.bus.Send(dfi.invocingSvc, replyMessage)
}

func (dfi *defaultInvocationContext) Bus() Messaging {
	return dfi.bus
}

func (dfi *defaultInvocationContext) Tx() *sql.Tx {
	return dfi.tx
}
