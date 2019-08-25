package saga

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus/metrics"

	"github.com/rs/xid"
	"github.com/wework/grabbit/gbus"
)

//Instance represent a living instance of a saga of a particular definition
type Instance struct {
	ID                 string
	ConcurrencyCtrl    int
	UnderlyingInstance gbus.Saga
	MsgToMethodMap     []*MsgToFuncPair
	Log                logrus.FieldLogger
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

	reflectedVal := reflect.ValueOf(si.UnderlyingInstance)

	for _, methodName := range methodsToInvoke {
		params := make([]reflect.Value, 0)
		params = append(params, reflect.ValueOf(invocation), valueOfMessage)
		method := reflectedVal.MethodByName(methodName)
		if invocation.Log() == nil {
			panic("here")
		}
		invocation.Log().WithFields(logrus.Fields{
			"method_name": methodName, "saga_id": si.ID,
		}).Info("invoking method on saga")

		err := metrics.RunHandlerWithMetric(func() error {
			returns := method.Call(params)

			val := returns[0]
			if !val.IsNil() {
				return val.Interface().(error)
			}
			return nil
		}, methodName, message.PayloadFQN, invocation.Log())

		if err != nil {
			return err
		}

		invocation.Log().WithFields(logrus.Fields{
			"method_name": methodName, "saga_id": si.ID,
		}).Info("saga instance invoked")
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

func (si *Instance) timeout(tx *sql.Tx, bus gbus.Messaging) error {

	saga, canTimeout := si.UnderlyingInstance.(gbus.RequestSagaTimeout)
	if !canTimeout {
		return fmt.Errorf("saga instance does not support timeouts")

	}
	return saga.Timeout(tx, bus)
}

//NewInstance creates a new saga instance
func NewInstance(sagaType reflect.Type, msgToMethodMap []*MsgToFuncPair) *Instance {

	var newSagaPtr interface{}
	if sagaType.Kind() == reflect.Ptr {
		newSagaPtr = reflect.New(sagaType).Elem().Interface()
	} else {
		newSagaPtr = reflect.New(sagaType).Elem()
	}

	saga := newSagaPtr.(gbus.Saga)

	newSaga := saga.New()

	//newSagaPtr := reflect.New(sagaType).Elem()
	newInstance := &Instance{
		ID:                 xid.New().String(),
		UnderlyingInstance: newSaga,
		MsgToMethodMap:     msgToMethodMap,
	}
	return newInstance
}

func (si *Instance) String() string {
	return gbus.GetFqn(si.UnderlyingInstance)
}
