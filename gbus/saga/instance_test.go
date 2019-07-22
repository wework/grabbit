package saga

import (
	"errors"
	"reflect"
	"testing"

	"github.com/wework/grabbit/gbus"
)

func TestInstanceInvocationReturnsErrors(t *testing.T) {

	/*
	   This tests that the instance calls the correct method and returns the the proper error value
	*/

	s := &TestSaga{}
	m1 := TestMsg1{}
	m2 := TestMsg2{}

	exchange, routingKey := "", "kong"
	invocationStub := &sagaInvocation{}

	failName := gbus.MessageHandler(s.Fail).Name()
	failFilter := gbus.NewMessageFilter(exchange, routingKey, m1)

	passName := gbus.MessageHandler(s.Pass).Name()
	passFilter := gbus.NewMessageFilter(exchange, routingKey, m2)

	//map the filter to correct saga function name
	funcPairs := make([]*MsgToFuncPair, 0)
	funcPairs = append(funcPairs, &MsgToFuncPair{failFilter, failName}, &MsgToFuncPair{passFilter, passName})

	instance := NewInstance(reflect.TypeOf(s), funcPairs)

	hasError := instance.invoke(exchange, routingKey, invocationStub, gbus.NewBusMessage(m1))
	if hasError == nil {
		t.Errorf("expected invocation to retrun an error but returned nil")
	}

	noError := instance.invoke(exchange, routingKey, invocationStub, gbus.NewBusMessage(m2))

	if noError != nil {
		t.Errorf("expected invocation not to retrun an error but instead returned a non nil error value")
	}

}

type TestMsg1 struct{}

func (TestMsg1) SchemaName() string {
	return "testmsg1"
}

type TestMsg2 struct{}

func (TestMsg2) SchemaName() string {
	return "testmsg2"
}

type TestSaga struct {
}

func (*TestSaga) StartedBy() []gbus.Message {
	fake := make([]gbus.Message, 0)
	return fake
}

func (*TestSaga) RegisterAllHandlers(register gbus.HandlerRegister) {}

func (*TestSaga) New() gbus.Saga {
	return &TestSaga{}
}

func (s *TestSaga) IsComplete() bool {
	return false
}

func (s *TestSaga) Fail(inocation gbus.Invocation, message *gbus.BusMessage) error {
	return errors.New("TestSaga.Fail invocation failed")
}

func (s *TestSaga) Pass(inocation gbus.Invocation, message *gbus.BusMessage) error {
	return nil
}
