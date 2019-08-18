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

//DeliveryInfo provdes information as to the attempted deilvery of the invocation
type DeliveryInfo struct {
	Attempt       uint
	MaxRetryCount uint
}

func (dfi *defaultInvocationContext) Log() logrus.FieldLogger {
	return dfi.Glogged.Log().WithFields(logrus.Fields{"routing_key": dfi.routingKey, "message_id": dfi.inboundMsg.ID})
}

//Reply implements the Invocation.Reply signature
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

//Send implements the Invocation.Send signature
func (dfi *defaultInvocationContext) Send(ctx context.Context, toService string, command *BusMessage, policies ...MessagePolicy) error {
	if dfi.tx != nil {
		return dfi.bus.sendWithTx(ctx, dfi.tx, toService, command, policies...)
	}
	return dfi.bus.Send(ctx, toService, command, policies...)
}

//Publish implements the Invocation.Publish signature
func (dfi *defaultInvocationContext) Publish(ctx context.Context, exchange, topic string, event *BusMessage, policies ...MessagePolicy) error {

	if dfi.tx != nil {
		return dfi.bus.publishWithTx(ctx, dfi.tx, exchange, topic, event, policies...)
	}
	return dfi.bus.Publish(ctx, exchange, topic, event, policies...)
}

//RPC implements the Invocation.RPC signature
func (dfi *defaultInvocationContext) RPC(ctx context.Context, service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error) {
	return dfi.bus.RPC(ctx, service, request, reply, timeout)
}

//Bus implements the Invocation.Bus signature
func (dfi *defaultInvocationContext) Bus() Messaging {
	return dfi
}

//Tx implements the Invocation.Tx signature
func (dfi *defaultInvocationContext) Tx() *sql.Tx {
	return dfi.tx
}

//Ctx implements the Invocation.Ctx signature
func (dfi *defaultInvocationContext) Ctx() context.Context {
	return dfi.ctx
}

//Routing implements the Invocation.Routing signature
func (dfi *defaultInvocationContext) Routing() (exchange, routingKey string) {
	return dfi.exchange, dfi.routingKey
}

//DeliveryInfo implements the Invocation.DeliveryInfo signature
func (dfi *defaultInvocationContext) DeliveryInfo() DeliveryInfo {
	return dfi.deliveryInfo
}
