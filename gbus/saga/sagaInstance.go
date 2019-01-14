package saga

import (
	"log"
	"reflect"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rs/xid"
)

type SagaInstance struct {
	ID                 string
	underlyingInstance interface{}
	msgToMethodMap     map[string]string
}

func (si *SagaInstance) invoke(invocation gbus.Invocation, message *gbus.BusMessage) {

	methodName := si.getSagaMethodNameToInvoke(message)

	if methodName == "" {
		log.Printf("Saga instance called with message but no message handlers were found for message type\nSaga type:%v\nMessage type",
			reflect.TypeOf(si.underlyingInstance).Name(),
			reflect.TypeOf(message.Payload).Name())
		return
	}

	valueOfMessage := reflect.ValueOf(message)
	sginv := &sagaInvocation{
		invocation.Bus(),
		invocation,
		message,
		si.ID}
	reflectedVal := reflect.ValueOf(si.underlyingInstance)
	params := make([]reflect.Value, 0)
	params = append(params, reflect.ValueOf(sginv), valueOfMessage)
	method := reflectedVal.MethodByName(methodName)

	log.Printf("calling saga instance:%v", si.ID)
	method.Call(params)
}
func (si *SagaInstance) getSagaMethodNameToInvoke(message *gbus.BusMessage) string {
	fqn := gbus.GetFqn(message.Payload)
	methodName := si.msgToMethodMap[fqn]
	return methodName
}
func (si *SagaInstance) isComplete() bool {
	saga := si.underlyingInstance.(gbus.Saga)
	return saga.IsComplete()
}
func newSagaInstance(sagaType reflect.Type, msgToMethodMap map[string]string) *SagaInstance {
	newSagaPtr := reflect.New(sagaType).Elem().Interface()
	newInstance := &SagaInstance{
		ID:                 xid.New().String(),
		underlyingInstance: newSagaPtr,
		msgToMethodMap:     msgToMethodMap}
	return newInstance
}
