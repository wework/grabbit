package saga

import (
	"context"
	"database/sql"
	"time"

	"github.com/wework/grabbit/gbus"
)

var _ gbus.Invocation = &sagaInvocation{}
var _ gbus.SagaInvocation = &sagaInvocation{}

type sagaInvocation struct {
	*gbus.Glogged
	decoratedBus        gbus.Messaging
	decoratedInvocation gbus.Invocation
	inboundMsg          *gbus.BusMessage
	sagaID              string
	ctx                 context.Context
	//the service that is executing the saga instance
	hostingSvc string
	//the service that sent the command/event that triggered the creation of the saga
	startedBy string
	/*
		in case the command/event that triggered the creation of the saga was sent from a saga
		then this field will hold the saga id of that instance
	*/
	startedBySaga string

	/* the message-id of the message that created the saga */
	startedByMessageID string
	/* the rpc id of the message that created the saga */
	startedByRPCID string
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
		if targetService == si.hostingSvc {
			message.SagaCorrelationID = message.SagaID
		}

	}

}
func (si *sagaInvocation) HostingSvc() string {
	return si.hostingSvc
}

func (si *sagaInvocation) InvokingSvc() string {
	return si.decoratedInvocation.InvokingSvc()
}

func (si *sagaInvocation) Reply(ctx context.Context, message *gbus.BusMessage) error {

	si.setCorrelationIDs(message, false)
	return si.decoratedInvocation.Reply(ctx, message)
}

func (si *sagaInvocation) ReplyToInitiator(ctx context.Context, message *gbus.BusMessage) error {

	si.setCorrelationIDs(message, false)

	//override the correlation ids to those of the message creating the saga
	message.SagaCorrelationID = si.startedBySaga
	message.RPCID = si.startedByRPCID
	message.CorrelationID = si.startedByMessageID
	return si.decoratedInvocation.Bus().Send(ctx, si.startedBy, message)
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

func (si *sagaInvocation) DeliveryInfo() gbus.DeliveryInfo {
	return si.decoratedInvocation.DeliveryInfo()
}
