package gbus

import (
	"context"
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

var _ Invocation = &defaultInvocationContext{}
var _ Messaging = &defaultInvocationContext{}

type defaultInvocationContext struct {
	*Glogged
	invocingSvc  string
	bus          *DefaultBus
	inboundMsg   *BusMessage
	tx           *sql.Tx
	ctx          context.Context
	exchange     string
	routingKey   string
	deliveryInfo DeliveryInfo
}

type DeliveryInfo struct {
	Attempt       uint
	MaxRetryCount uint
}

func (dfi *defaultInvocationContext) Log() logrus.FieldLogger {
	return dfi.Glogged.Log().WithFields(logrus.Fields{"routing_key": dfi.routingKey, "message_id": dfi.inboundMsg.ID})
}

func (dfi *defaultInvocationContext) Reply(ctx context.Context, replyMessage *BusMessage) error {
	if dfi.inboundMsg != nil {
		replyMessage.CorrelationID = dfi.inboundMsg.ID
		replyMessage.SagaCorrelationID = dfi.inboundMsg.SagaID
		replyMessage.RPCID = dfi.inboundMsg.RPCID
	}
	var err error

	if dfi.tx != nil {
		return dfi.bus.sendWithTx(ctx, dfi.tx, dfi.invocingSvc, replyMessage)
	}
	if err = dfi.bus.Send(ctx, dfi.invocingSvc, replyMessage); err != nil {
		//TODO: add logs?
		logrus.WithError(err).Error("could not send reply")

	}
	return err
}

func (dfi *defaultInvocationContext) RawSend(ctx context.Context, serializer, toService, replyTo string, message *BusMessage, policies ...MessagePolicy) error {
	if dfi.tx != nil {
		return dfi.bus.sendRawWithTx(ctx, dfi.tx, toService, replyTo, message, policies...)
	}
	return dfi.bus.RawSend(ctx, toService, replyTo, message, policies...)
}

func (dfi *defaultInvocationContext) Send(ctx context.Context, toService string, command *BusMessage, policies ...MessagePolicy) error {
	if dfi.tx != nil {
		return dfi.bus.sendWithTx(ctx, dfi.tx, toService, command, policies...)
	}
	return dfi.bus.Send(ctx, toService, command, policies...)
}

func (dfi *defaultInvocationContext) Publish(ctx context.Context, exchange, topic string, event *BusMessage, policies ...MessagePolicy) error {

	if dfi.tx != nil {
		return dfi.bus.publishWithTx(ctx, dfi.tx, exchange, topic, event, policies...)
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

func (dfi *defaultInvocationContext) Routing() (exchange, routingKey string) {
	return dfi.exchange, dfi.routingKey
}

func (dfi *defaultInvocationContext) DeliveryInfo() DeliveryInfo {
	return dfi.deliveryInfo
}
