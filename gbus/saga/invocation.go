package saga

import (
	"context"
	"database/sql"
	"time"

	"github.com/wework/grabbit/gbus"
)

type sagaInvocation struct {
	decoratedBus        gbus.Messaging
	decoratedInvocation gbus.Invocation
	inboundMsg          *gbus.BusMessage
	sagaID              string
	ctx                 context.Context
	invokingService     string
}

func (si *sagaInvocation) setCorrelationIDs(message *gbus.BusMessage, isEvent bool) {

	message.CorrelationID = si.inboundMsg.ID
	message.SagaID = si.sagaID

	if !isEvent {
		//support saga-to-saga communication
		if si.inboundMsg.SagaID != "" {
			message.SagaCorrelationID = si.inboundMsg.SagaID
		}
		//if the saga is potentially invoking itself then set the SagaCorrelationID to reflect that
		//https://github.com/wework/grabbit/issues/64
		_, targetService := si.decoratedInvocation.Routing()
		if targetService == si.invokingService {
			message.SagaCorrelationID = message.SagaID
		}

	}

}

func (si *sagaInvocation) Reply(ctx context.Context, message *gbus.BusMessage) error {

	si.setCorrelationIDs(message, false)
	return si.decoratedInvocation.Reply(ctx, message)
}

func (si *sagaInvocation) Bus() gbus.Messaging {
	return si
}

func (si *sagaInvocation) Tx() *sql.Tx {
	return si.decoratedInvocation.Tx()
}

func (si *sagaInvocation) Ctx() context.Context {
	return si.ctx
}

func (si *sagaInvocation) Send(ctx context.Context, toService string,
	command *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	si.setCorrelationIDs(command, false)
	return si.decoratedBus.Send(ctx, toService, command, policies...)
}

func (si *sagaInvocation) Publish(ctx context.Context, exchange, topic string,
	event *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	si.setCorrelationIDs(event, true)
	return si.decoratedBus.Publish(ctx, exchange, topic, event, policies...)
}

func (si *sagaInvocation) RPC(ctx context.Context, service string, request,
	reply *gbus.BusMessage, timeout time.Duration) (*gbus.BusMessage, error) {
	return si.decoratedBus.RPC(ctx, service, request, reply, timeout)
}

func (si *sagaInvocation) Routing() (exchange, routingKey string) {
	return si.decoratedInvocation.Routing()
}
