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

func NewFromAMQPHeaders(headers amqp.Table) *BusMessage {
	bm := &BusMessage{}
	bm.SetFromAMQPHeaders(headers)
	return bm
}

//GetAMQPHeaders convert to AMQP headers Table everything but a payload
func (bm *BusMessage) GetAMQPHeaders() (headers amqp.Table) {
	headers = amqp.Table{}
	headers["x-msg-id"] = bm.ID
	headers["x-msg-saga-id"] = bm.SagaID
	headers["x-msg-semantics"] = bm.Semantics
	headers["x-msg-correlation-id"] = bm.CorrelationID
	headers["x-msg-saga-correlation-id"] = bm.SagaCorrelationID
	headers["x-grabbit-rpc-id"] = bm.RPCID
	headers["x-msg-name"] = bm.Payload.Name()
	headers["x-msg-type"] = bm.Semantics
	return
}

//SetFromAMQPHeaders convert from AMQP headers Table everything but a payload
func (bm *BusMessage) SetFromAMQPHeaders(headers amqp.Table) {
	bm.ID = castToSgtring(headers["x-msg-id"])
	bm.SagaID = castToSgtring(headers["x-msg-saga-id"])
	bm.Semantics = castToSgtring(headers["x-msg-semantics"])
	bm.CorrelationID = castToSgtring(headers["x-msg-correlation-id"])
	bm.SagaCorrelationID = castToSgtring(headers["x-msg-saga-correlation-id"])
	bm.RPCID = castToSgtring(headers["x-grabbit-rpc-id"])
	bm.PayloadFQN = castToSgtring(headers["x-msg-name"])
	bm.Semantics = castToSgtring(headers["x-msg-type"])
}

//SetPayload sets the payload and makes sure that Name is saved
func (bm *BusMessage) SetPayload(payload Message) {
	bm.PayloadFQN = payload.Name()
	bm.Payload = payload
}

func castToSgtring(i interface{}) string {
	v, ok := i.(string)
	if !ok {
		return ""
	}
	return v
}
