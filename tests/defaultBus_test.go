package tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/rhinof/grabbit/gbus"
)

func TestSendCommand(t *testing.T) {
	cmd := Command1{
		Data: "Command1",
	}
	proceed := make(chan bool)
	b := createBusForTest()

	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {

		_, ok := message.Payload.(*Command1)
		if !ok {
			t.Errorf("handler invoced with wrong message type\r\nexpeted:%v\r\nactual:%v", reflect.TypeOf(Command1{}), reflect.TypeOf(message.Payload))
		}
		proceed <- true

		return nil
	}

	err := b.HandleMessage(cmd, handler)
	if err != nil {
		t.Errorf("Registering handler returned false, expected true with error: %s", err.Error())
	}

	err = b.Start()
	if err != nil {
		t.Errorf("could not start bus for test error: %s", err.Error())
	}

	err = b.Send(noopTraceContext(), testSvc1, gbus.NewBusMessage(cmd))
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}

	<-proceed
	b.Shutdown()

}

func TestReply(t *testing.T) {
	cmd := Command1{}
	reply := Reply1{}
	cmdBusMsg := gbus.NewBusMessage(cmd)

	svc1 := createNamedBusForTest(testSvc1)
	svc2 := createNamedBusForTest(testSvc2)

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		err := invocation.Reply(noopTraceContext(), gbus.NewBusMessage(reply))
		if err != nil {
			t.Errorf("could not send reply with error: %s", err.Error())
			return err
		}
		return nil

	}

	replyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		_, ok := message.Payload.(*Reply1)
		if !ok {
			t.Errorf("message handler for reply message invoced with wrong message type\r\n%v", message)
		}

		proceed <- true
		return nil
	}

	svc2.HandleMessage(cmd, cmdHandler)
	svc1.HandleMessage(reply, replyHandler)

	svc1.Start()
	defer svc1.Shutdown()

	svc2.Start()
	defer svc2.Shutdown()

	svc1.Send(noopTraceContext(), testSvc2, cmdBusMsg)
	<-proceed
}

func TestPubSub(t *testing.T) {
	event := Event1{}
	b := createBusForTest()

	proceed := make(chan bool)
	eventHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		proceed <- true
		return nil
	}
	b.HandleEvent("test_exchange", "test_topic", event, eventHandler)

	b.Start()
	defer b.Shutdown()
	err := b.Publish(noopTraceContext(), "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	<-proceed

}

func TestHandlerRetry(t *testing.T) {

	c1 := Command1{}
	r1 := Reply1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(r1)

	bus := createBusForTest()

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		return invocation.Reply(noopTraceContext(), reply)
	}

	attempts := 0
	replyHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		if attempts == 0 {
			attempts++
			return fmt.Errorf("expecting retry on errors")
		} else if attempts == 1 {
			attempts++
			panic("expecting retry on panics")
		} else {
			proceed <- true
		}
		return nil
	}

	bus.HandleMessage(c1, cmdHandler)
	bus.HandleMessage(r1, replyHandler)

	bus.Start()
	defer bus.Shutdown()

	bus.Send(noopTraceContext(), testSvc1, cmd)
	<-proceed

}

func TestRPC(t *testing.T) {

	c1 := Command1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(Reply1{})

	handler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {

		return invocation.Reply(noopTraceContext(), reply)
	}

	svc1 := createNamedBusForTest(testSvc1)
	svc1.HandleMessage(c1, handler)
	svc1.Start()
	defer svc1.Shutdown()
	svc2 := createNamedBusForTest(testSvc2)
	svc2.Start()
	defer svc2.Shutdown()
	t.Log("Sending RPC")
	reply, _ = svc2.RPC(noopTraceContext(), testSvc1, cmd, reply, 5*time.Second)
	t.Log("Tested RPC")
	if reply == nil {
		t.Fail()
	}

}

func noopTraceContext() context.Context {
	return context.Background()
	// tracer := opentracing.NoopTracer{}
	// span := tracer.StartSpan("test")
	// ctx := opentracing.ContextWithSpan(context.Background(), span)
	// return ctx
}
