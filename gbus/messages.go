package gbus

import (
	"strings"

	"github.com/opentracing/opentracing-go/log"
	"github.com/rs/xid"
)

//BusMessage the structure that gets sent to the underlying transport
type BusMessage struct {
	ID                   string
	IdempotencyKey       string
	CorrelationID        string
	SagaID               string
	SagaCorrelationID    string
	Semantics            Semantics /*cmd or evt*/
	Payload              Message
	PayloadFQN           string
	RPCID                string
	ResurrectedFromDeath bool
	RawPayload           []byte
	ContentType          string
}

//NewBusMessage factory method for creating a BusMessage that wraps the given payload
func NewBusMessage(payload Message) *BusMessage {
	bm := &BusMessage{
		ID: xid.New().String(),
	}
	bm.SetIdempotencyKey(bm.ID)
	bm.SetPayload(payload)
	return bm
}

//SetPayload sets the payload and makes sure that Name is saved
func (bm *BusMessage) SetPayload(payload Message) {
	bm.PayloadFQN = payload.SchemaName()
	bm.Payload = payload
}

func (bm *BusMessage) SetIdempotencyKey(idempotencyKey string) {
	bm.IdempotencyKey = strings.TrimSpace(idempotencyKey)
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
		log.String("IdempotencyKey", bm.IdempotencyKey),
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
