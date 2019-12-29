package rabbitmq

import (
	"emperror.dev/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/wework/grabbit/gbus"
)

var (
	ResurrectedHeaderName  = "x-resurrected-from-death"
)

func newFromDelivery(delivery amqp.Delivery) (*gbus.BusMessage, error) {
	bm := &gbus.BusMessage{}
	setFromAMQPHeaders(delivery, bm)

	bm.ID = delivery.MessageId
	bm.CorrelationID = delivery.CorrelationId
	if delivery.Exchange != "" {
		bm.Semantics = gbus.EVT
	} else {
		bm.Semantics = gbus.CMD
	}
	if bm.PayloadFQN == "" || bm.Semantics == "" {
		return nil, errors.NewWithDetails("missing critical headers", "message_name", bm.PayloadFQN, "semantics", bm.Semantics)
	}
	return bm, nil
}

func setFromAMQPHeaders(delivery amqp.Delivery, bm *gbus.BusMessage) {
	headers := delivery.Headers
	bm.IdempotencyKey = castToString(headers["x-idempotency-key"])
	bm.SagaID = castToString(headers["x-msg-saga-id"])
	bm.SagaCorrelationID = castToString(headers["x-msg-saga-correlation-id"])
	bm.RPCID = castToString(headers["x-grabbit-msg-rpc-id"])
	bm.PayloadFQN = castToString(delivery.Headers["x-msg-name"])
}

func getAMQPHeaders(bm *gbus.BusMessage) (headers amqp.Table) {
	headers = amqp.Table{
		"x-msg-name":        bm.Payload.SchemaName(),
		"x-idempotency-key": bm.IdempotencyKey,
	}

	/*
	 only set the following headers if they contain a value
	 https://github.com/wework/grabbit/issues/221
	*/
	setNonEmpty(headers, "x-msg-saga-id", bm.SagaID)
	setNonEmpty(headers, "x-msg-saga-correlation-id", bm.SagaCorrelationID)
	setNonEmpty(headers, "x-grabbit-msg-rpc-id", bm.RPCID)

	return
}
func setNonEmpty(headers amqp.Table, headerName, headerValue string) {
	if headerValue != "" {
		headers[headerName] = headerValue
	}
}
func castToString(i interface{}) string {
	v, ok := i.(string)
	if !ok {
		return ""
	}
	return v
}

func getDeliveryLogEntries(delivery amqp.Delivery) logrus.Fields {

	return logrus.Fields{
		"message_name":    castToString(delivery.Headers["x-msg-name"]),
		"message_id":      delivery.MessageId,
		"routing_key":     delivery.RoutingKey,
		"exchange":        delivery.Exchange,
		"idempotency_key": castToString(delivery.Headers["x-idempotency-key"]),
		"correlation_id":  castToString(delivery.CorrelationId),
		"rpc_id":          castToString(delivery.Headers["x-grabbit-msg-rpc-id"]),
	}

}

func isResurrectedMessage(delivery amqp.Delivery) bool {
	isResurrected, ok := delivery.Headers[ResurrectedHeaderName].(bool)
	return ok && isResurrected
}
