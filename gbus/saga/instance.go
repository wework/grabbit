package saga

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/rs/xid"
	"github.com/wework/grabbit/gbus"
)

//Instance represent a living instance of a saga of a particular definition
type Instance struct {
	ID                 string
	ConcurrencyCtrl    int
	UnderlyingInstance gbus.Saga
	MsgToMethodMap     []*MsgToFuncPair
}

func (si *Instance) invoke(exchange, routingKey string, invocation gbus.Invocation, message *gbus.BusMessage) error {

	methodsToInvoke := si.getSagaMethodNameToInvoke(exchange, routingKey, message)

	if len(methodsToInvoke) == 0 {
		err := fmt.Errorf("saga instance called with message but no message handlers were found for message type\nSaga type:%v\nMessage type:%v",
			si.String(),
			reflect.TypeOf(message.Payload).Name())
		return err
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

	for _, methodName := range methodsToInvoke {
		params := make([]reflect.Value, 0)
		params = append(params, reflect.ValueOf(sginv), valueOfMessage)
		method := reflectedVal.MethodByName(methodName)
		log.Printf(" invoking method %v on saga instance %v", methodName, si.ID)
		returns := method.Call(params)

		val := returns[0]
		if val.IsNil() == false {
			return val.Interface().(error)
		}

		log.Printf(" saga instance %v invoked", si.ID)

	}

	return nil
}
func (si *Instance) getSagaMethodNameToInvoke(exchange, routingKey string, message *gbus.BusMessage) []string {

	methods := make([]string, 0)

	for _, pair := range si.MsgToMethodMap {
		if pair.Filter.Matches(exchange, routingKey, message.PayloadFQN) {
			methods = append(methods, pair.SagaFuncName)
		}
	}
	return methods
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

func NewInstance(sagaType reflect.Type, msgToMethodMap []*MsgToFuncPair, confFns ...gbus.SagaConfFn) *Instance {

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
