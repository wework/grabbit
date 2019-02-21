package gbus

import (
	"context"
	"database/sql"

	"github.com/sirupsen/logrus"
)

type defaultInvocationContext struct {
	invocingSvc string
	bus         Messaging
	inboundMsg  *BusMessage
	tx          *sql.Tx
	ctx         context.Context
}

func (dfi *defaultInvocationContext) Reply(ctx context.Context, replyMessage *BusMessage) error {
	if dfi.inboundMsg != nil {
		replyMessage.CorrelationID = dfi.inboundMsg.ID
		replyMessage.SagaCorrelationID = dfi.inboundMsg.SagaID
		replyMessage.RPCID = dfi.inboundMsg.RPCID
	}
	var err error
	if err = dfi.bus.Send(ctx, dfi.invocingSvc, replyMessage); err != nil {
		//TODO: add logs?
		logrus.WithError(err).Error("could not send reply")

	}
	return err
}

func (dfi *defaultInvocationContext) Bus() Messaging {
	return dfi.bus
}

func (dfi *defaultInvocationContext) Tx() *sql.Tx {
	return dfi.tx
}

func (dfi *defaultInvocationContext) Ctx() context.Context {
	return dfi.ctx
}
