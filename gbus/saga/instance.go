package saga

import (
	"log"
	"reflect"
	"time"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rs/xid"
)

//Instance represent a living instance of a saga of a particular definition
type Instance struct {
	ID                 string
	underlyingInstance interface{}
	msgToMethodMap     map[string]string
}

func (si *Instance) invoke(invocation gbus.Invocation, message *gbus.BusMessage) {

	methodName := si.getSagaMethodNameToInvoke(message)

	if methodName == "" {
		log.Printf("Saga instance called with message but no message handlers were found for message type\nSaga type:%v\nMessage type:%v\n",
			si.String(),
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
	log.Printf(" invoking method %v on saga instance %v", methodName, si.ID)
	method.Call(params)
	log.Printf(" saga instance %v invoked", si.ID)
}
func (si *Instance) getSagaMethodNameToInvoke(message *gbus.BusMessage) string {
	fqn := gbus.GetFqn(message.Payload)
	methodName := si.msgToMethodMap[fqn]
	return methodName
}
func (si *Instance) isComplete() bool {
	saga := si.underlyingInstance.(gbus.Saga)
	return saga.IsComplete()
}

func (si *Instance) requestsTimeout() (bool, time.Duration) {

	timeoutDuration := -1 * time.Millisecond
	timeoutData, canTimeout := si.underlyingInstance.(gbus.RequestSagaTimeout)
	if canTimeout {
		timeoutDuration = timeoutData.TimeoutDuration()
	}
	return canTimeout, timeoutDuration
}
func newInstance(sagaType reflect.Type, msgToMethodMap map[string]string) *Instance {
	var newSagaPtr interface{}
	if sagaType.Kind() == reflect.Ptr {
		newSagaPtr = reflect.New(sagaType).Elem().Interface()
	} else {
		newSagaPtr = reflect.New(sagaType).Elem()
	}

	saga := newSagaPtr.(gbus.Saga)

	//newSagaPtr := reflect.New(sagaType).Elem()
	newInstance := &Instance{
		ID:                 xid.New().String(),
		underlyingInstance: saga.New(),
		msgToMethodMap:     msgToMethodMap}
	return newInstance
}

func (si *Instance) String() string {
	return gbus.GetFqn(si.underlyingInstance)
}
