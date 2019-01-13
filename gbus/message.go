package gbus

import "github.com/rs/xid"

type BusMessage struct {
	ID                string
	CorrelationID     string
	SagaID            string
	SagaCorrelationID string
	Semantics         string /*cmd or evt*/
	Payload           interface{}
}

func NewBusMessage(payload interface{}) BusMessage {
	return BusMessage{
		ID:      xid.New().String(),
		Payload: payload}
}
