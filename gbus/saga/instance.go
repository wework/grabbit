package saga

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/metrics"
)

//Instance represent a living instance of a saga of a particular definition
type Instance struct {
	ID                 string
	ConcurrencyCtrl    int
	UnderlyingInstance gbus.Saga
	MsgToMethodMap     []*MsgToFuncPair
	logger             logrus.FieldLogger
	/*
		Will hold the service name that sent the command or event that started the saga
	*/
	StartedBy string
	/*
		If this saga has been started by a message originating from another saga instance
		this field will hold the saga_id of that instance
	*/
	StartedBySaga string

	//StartedByMessageID the message-id of the message that created the saga
	StartedByMessageID string
	//StartedByRPCID the rpc id of the message that created the saga
	StartedByRPCID string
}

func (si *Instance) log() logrus.FieldLogger {
	if si.logger == nil {
		return logrus.WithField("id", si.ID)
	}

	return si.logger
}

func (si *Instance) invoke(exchange, routingKey string, invocation *sagaInvocation, message *gbus.BusMessage) error {

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

		//replace the handler_name entry with the current inoked saga method that will be invoked
		chainedLogger := invocation.Log().WithField("handler_name", methodName)
		invocation.SetLogger(chainedLogger)

		invocation.Log().Info("invoking saga instance")

		span, sctx := opentracing.StartSpanFromContext(invocation.Ctx(), methodName)
		// replace the original context with the conext built around the span so we ca
		// trace the saga handler that is invoked
		invocation.ctx = sctx

		defer span.Finish()

		err := metrics.RunHandlerWithMetric(func() error {
			returns := method.Call(params)

			val := returns[0]
			if !val.IsNil() {
				return val.Interface().(error)
			}
			return nil
<<<<<<< HEAD
		}, methodName, message.PayloadFQN, si.log())
=======
		}, methodName, message.PayloadFQN, invocation.Log())
>>>>>>> added generic handler metrics with message type as the label (#144)

		if err != nil {
			return err
		}

		invocation.Log().Info("saga instance invoked")
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
