package gbus

import (
	"errors"
	"fmt"

	"github.com/opentracing/opentracing-go/log"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

//BusMessage the structure that gets sent to the underlying transport
type BusMessage struct {
	ID                string
	CorrelationID     string
	SagaID            string
	SagaCorrelationID string
	Semantics         Semantics /*cmd or evt*/
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

//NewFromDelivery creates a BusMessage from an amqp delivery
func NewFromDelivery(delivery amqp.Delivery) (*BusMessage, error) {
	bm := &BusMessage{}
	bm.SetFromAMQPHeaders(delivery)

	bm.ID = delivery.MessageId
	bm.CorrelationID = delivery.CorrelationId
	if delivery.Exchange != "" {
		bm.Semantics = EVT
	} else {
		bm.Semantics = CMD
	}
	if bm.PayloadFQN == "" || bm.Semantics == "" {
		errMsg := fmt.Sprintf("missing critical headers. message_name:%s semantics: %s", bm.PayloadFQN, bm.Semantics)
		return nil, errors.New(errMsg)
	}
	return bm, nil
}

//GetMessageName extracts the valuee of the custom x-msg-name header from an amq delivery
func GetMessageName(delivery amqp.Delivery) string {
	return castToString(delivery.Headers["x-msg-name"])
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
func (bm *BusMessage) SetFromAMQPHeaders(delivery amqp.Delivery) {
	headers := delivery.Headers
	bm.SagaID = castToString(headers["x-msg-saga-id"])
	bm.SagaCorrelationID = castToString(headers["x-msg-saga-correlation-id"])
	bm.RPCID = castToString(headers["x-grabbit-msg-rpc-id"])
	bm.PayloadFQN = GetMessageName(delivery)

}

//SetPayload sets the payload and makes sure that Name is saved
func (bm *BusMessage) SetPayload(payload Message) {
	bm.PayloadFQN = payload.SchemaName()
	bm.Payload = payload
}

//TargetSaga allows sending the message to a specific Saga instance
func (bm *BusMessage) TargetSaga(sagaID string) {
	bm.SagaCorrelationID = sagaID
}

//GetTraceLog returns an array of log entires containing all of the message properties
func (bm *BusMessage) GetTraceLog() (fields []log.Field) {
	return []log.Field{
		log.String("message", bm.PayloadFQN),
		log.String("ID", bm.ID),
		log.String("SagaID", bm.SagaID),
		log.String("CorrelationID", bm.CorrelationID),
		log.String("SagaCorrelationID", bm.SagaCorrelationID),
		log.String("Semantics", string(bm.Semantics)),
		log.String("RPCID", bm.RPCID),
	}
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

// In order to route "resurrected" events to the correct handler it is necessary to use the original exchange and routing-key which
// were added by grabbit previously as headers, as opposed to the native values on the delivery
// Issue reference:	https://github.com/wework/grabbit/issues/191
func exchangeAndRoutingFromDelivery(delivery amqp.Delivery) (exchange string, routingKey string, err error) {
	if isResurrectedMessage(delivery) {
		exchange, ok := delivery.Headers["x-first-death-exchange"].(string)
		if !ok {
			return "", "", errors.New("failed extracting exchange from resurrected message, bad x-first-death-exchange")
		}
		routingKey, ok := delivery.Headers[FirstDeathRoutingKeyHeaderName].(string)
		if !ok {
			return "", "", errors.New("failed extracting routing-key from resurrected message, bad x-first-death-routing-key")
		}
		return exchange, routingKey, nil
	}

	return delivery.Exchange, delivery.RoutingKey, nil
}

func isResurrectedMessage(delivery amqp.Delivery) bool {
	isResurrected, ok := delivery.Headers[ResurrectedHeaderName].(bool)
	return ok && isResurrected
}
