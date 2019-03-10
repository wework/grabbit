package gbus

import (
	"context"
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

type defaultInvocationContext struct {
	invocingSvc string
	bus         *DefaultBus
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

	if dfi.tx != nil {
		return dfi.bus.sendWithTx(ctx, dfi.tx, false, dfi.invocingSvc, replyMessage)
	}
	if err = dfi.bus.Send(ctx, dfi.invocingSvc, replyMessage); err != nil {
		//TODO: add logs?
		logrus.WithError(err).Error("could not send reply")

	}
	return err
}

func (dfi *defaultInvocationContext) Send(ctx context.Context, toService string, command *BusMessage, policies ...MessagePolicy) error {
	if dfi.tx != nil {
		return dfi.bus.sendWithTx(ctx, dfi.tx, false, toService, command, policies...)
	}
	return dfi.bus.Send(ctx, toService, command, policies...)
}

func (dfi *defaultInvocationContext) Publish(ctx context.Context, exchange, topic string, event *BusMessage, policies ...MessagePolicy) error {

	if dfi.tx != nil {
		return dfi.bus.publishWithTx(ctx, dfi.tx, false, exchange, topic, event, policies...)
	}
	return dfi.bus.Publish(ctx, exchange, topic, event, policies...)
}

func (dfi *defaultInvocationContext) RPC(ctx context.Context, service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error) {
	return dfi.bus.RPC(ctx, service, request, reply, timeout)
}

func (dfi *defaultInvocationContext) Bus() Messaging {
	return dfi
}

func (dfi *defaultInvocationContext) Tx() *sql.Tx {
	return dfi.tx
}

func (dfi *defaultInvocationContext) Ctx() context.Context {
	return dfi.ctx
}
