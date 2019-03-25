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
	ConcurrencyCtrl    int
	UnderlyingInstance gbus.Saga
	MsgToMethodMap     map[string]string
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
		decoratedBus:        invocation.Bus(),
		decoratedInvocation: invocation,
		inboundMsg:          message,
		sagaID:              si.ID,
		ctx:                 invocation.Ctx(),
	}
	reflectedVal := reflect.ValueOf(si.UnderlyingInstance)

	params := make([]reflect.Value, 0)
	params = append(params, reflect.ValueOf(sginv), valueOfMessage)
	method := reflectedVal.MethodByName(methodName)
	log.Printf(" invoking method %v on saga instance %v", methodName, si.ID)
	method.Call(params)
	log.Printf(" saga instance %v invoked", si.ID)
}
func (si *Instance) getSagaMethodNameToInvoke(message *gbus.BusMessage) string {
	fqn := message.PayloadFQN
	methodName := si.MsgToMethodMap[fqn]
	return methodName
}
func (si *Instance) isComplete() bool {
	saga := si.UnderlyingInstance.(gbus.Saga)
	return saga.IsComplete()
}

func (si *Instance) requestsTimeout() (bool, time.Duration) {

	timeoutDuration := -1 * time.Millisecond
	timeoutData, canTimeout := si.UnderlyingInstance.(gbus.RequestSagaTimeout)
	if canTimeout {
		timeoutDuration = timeoutData.TimeoutDuration()
	}
	return canTimeout, timeoutDuration
}

//NewInstance create a new instance of a Saga
func NewInstance(sagaType reflect.Type, msgToMethodMap map[string]string, confFns ...gbus.SagaConfFn) *Instance {
	var newSagaPtr interface{}
	if sagaType.Kind() == reflect.Ptr {
		newSagaPtr = reflect.New(sagaType).Elem().Interface()
	} else {
		newSagaPtr = reflect.New(sagaType).Elem()
	}

	saga := newSagaPtr.(gbus.Saga)

	newSaga := saga.New()
	for _, conf := range confFns {
		newSaga = conf(newSaga)
	}
	//newSagaPtr := reflect.New(sagaType).Elem()
	newInstance := &Instance{
		ID:                 xid.New().String(),
		UnderlyingInstance: newSaga,
		MsgToMethodMap:     msgToMethodMap}
	return newInstance
}

func (si *Instance) String() string {
	return gbus.GetFqn(si.UnderlyingInstance)
}
