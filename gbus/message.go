package gbus

import "github.com/rs/xid"

//BusMessage the structure that gets sent to the underlying transport
type BusMessage struct {
	ID                string
	CorrelationID     string
	SagaID            string
	SagaCorrelationID string
	Semantics         string /*cmd or evt*/
	Payload           interface{}
}

//NewBusMessage factory mehtod for creating a BusMessage that wraps the given payload
func NewBusMessage(payload interface{}) BusMessage {
	return BusMessage{
		ID:      xid.New().String(),
		Payload: payload}
}
