package saga

import (
	"database/sql"
	"time"

	"github.com/rhinof/grabbit/gbus"
)

type sagaInvocation struct {
	decoratedBus        gbus.Messaging
	decoratedInvocation gbus.Invocation
	inboundMsg          *gbus.BusMessage
	sagaID              string
}

func (si *sagaInvocation) setCorrelationIDs(message *gbus.BusMessage) {

	message.CorrelationID = si.inboundMsg.ID

	//support saga-to-saga communication
	if si.inboundMsg.SagaID != "" {
		message.SagaCorrelationID = message.SagaID
	}

	message.SagaID = si.sagaID
}

func (si *sagaInvocation) Reply(message *gbus.BusMessage) {

	si.setCorrelationIDs(message)
	si.decoratedInvocation.Reply(message)
}

func (si *sagaInvocation) Bus() gbus.Messaging {
	return si
}

func (si *sagaInvocation) Tx() *sql.Tx {
	return si.decoratedInvocation.Tx()
}

func (si *sagaInvocation) Send(toService string, command *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	si.setCorrelationIDs(command)
	return si.decoratedBus.Send(toService, command, policies...)
}

func (si *sagaInvocation) Publish(exchange, topic string, event *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	si.setCorrelationIDs(event)
	return si.decoratedBus.Publish(exchange, topic, event, policies...)
}

func (si *sagaInvocation) RPC(service string, request, reply *gbus.BusMessage, timeout time.Duration) (*gbus.BusMessage, error) {
	return si.decoratedBus.RPC(service, request, reply, timeout)
}
