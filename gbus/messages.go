package gbus

import (
	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

//BusMessage the structure that gets sent to the underlying transport
type BusMessage struct {
	ID                string
	CorrelationID     string
	SagaID            string
	SagaCorrelationID string
	Semantics         string /*cmd or evt*/
	Payload           Message
	PayloadFQN        string
	RPCID             string
}

//NewBusMessage factory method for creating a BusMessage that wraps the given payload
func NewBusMessage(payload Message) *BusMessage {
	bm := &BusMessage{
		ID: xid.New().String(),
	}
	bm.SetPayload(payload)
	return bm
}

//NewFromAMQPHeaders creates a BusMessage from headers of an amqp message
func NewFromAMQPHeaders(headers amqp.Table) *BusMessage {
	bm := &BusMessage{}
	bm.SetFromAMQPHeaders(headers)
	return bm
}

//GetAMQPHeaders convert to AMQP headers Table everything but a payload
func (bm *BusMessage) GetAMQPHeaders() (headers amqp.Table) {
	headers = amqp.Table{}
	headers["x-msg-saga-id"] = bm.SagaID
	headers["x-msg-saga-correlation-id"] = bm.SagaCorrelationID
	headers["x-grabbit-msg-rpc-id"] = bm.RPCID
	headers["x-msg-name"] = bm.Payload.SchemaName()

	return
}

//SetFromAMQPHeaders convert from AMQP headers Table everything but a payload
func (bm *BusMessage) SetFromAMQPHeaders(headers amqp.Table) {

	bm.SagaID = castToString(headers["x-msg-saga-id"])
	bm.SagaCorrelationID = castToString(headers["x-msg-saga-correlation-id"])
	bm.RPCID = castToString(headers["x-grabbit-msg-rpc-id"])
	bm.PayloadFQN = castToString(headers["x-msg-name"])

}

//SetPayload sets the payload and makes sure that Name is saved
func (bm *BusMessage) SetPayload(payload Message) {
	bm.PayloadFQN = payload.SchemaName()
	bm.Payload = payload
}

func castToString(i interface{}) string {
	v, ok := i.(string)
	if !ok {
		return ""
	}
	return v
}

var _ Message = &SagaTimeoutMessage{}

//SagaTimeoutMessage is the timeout message for Saga's
type SagaTimeoutMessage struct {
	SagaID string
}

//SchemaName implements gbus.Message
func (SagaTimeoutMessage) SchemaName() string {
	return "grabbit.timeout"
}
