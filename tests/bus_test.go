package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/wework/grabbit/gbus/metrics"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/wework/grabbit/gbus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true})
}

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
	cmdHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
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
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
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

func TestSubscribingOnTopic(t *testing.T) {
	event := Event1{}
	b := createBusForTest()

	proceed := make(chan bool)
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}
	b.HandleEvent("test_exchange", "a.*.c", nil, eventHandler)

	b.Start()
	defer b.Shutdown()
	err := b.Publish(noopTraceContext(), "test_exchange", "a.b.c", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	<-proceed
}

var (
	handlerRetryProceed = make(chan bool)
	attempts            = 0
)

func TestHandlerRetry(t *testing.T) {

	c1 := Command1{}
	r1 := Reply1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(r1)

	bus := createBusForTest()

	cmdHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		return invocation.Reply(noopTraceContext(), reply)
	}

	bus.HandleMessage(c1, cmdHandler)
	bus.HandleMessage(r1, handleRetry)

	bus.Start()
	defer bus.Shutdown()

	bus.Send(noopTraceContext(), testSvc1, cmd)
	<-handlerRetryProceed

	hm := metrics.GetHandlerMetrics("handleRetry")
	if hm == nil {
		t.Error("Metrics for handleRetry should be initiated")
	}
	f, _ := hm.GetFailureCount()
	s, _ := hm.GetSuccessCount()

	if f != 2 {
		t.Errorf("Failure count should be 2 but was %f", f)
	}
	if s != 1 {
		t.Errorf("Success count should be 1 but was %f", s)
	}
}

func handleRetry(invocation gbus.Invocation, message *gbus.BusMessage) error {
	if attempts == 0 {
		attempts++
		return fmt.Errorf("expecting retry on errors")
	} else if attempts == 1 {
		attempts++
		panic("expecting retry on panics")
	} else {
		handlerRetryProceed <- true
	}
	return nil
}

func TestRPC(t *testing.T) {

	c1 := Command1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(Reply1{})

	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {

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

func TestDeadlettering(t *testing.T) {

	var waitgroup sync.WaitGroup
	waitgroup.Add(2)
	poison := gbus.NewBusMessage(PoisonMessage{})
	service1 := createNamedBusForTest(testSvc1)
	deadletterSvc := createNamedBusForTest("deadletterSvc")

	deadMessageHandler := func(tx *sql.Tx, poison amqp.Delivery) error {
		waitgroup.Done()
		return nil
	}

	faultyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		return errors.New("fail")
	}

	deadletterSvc.HandleDeadletter(deadMessageHandler)
	service1.HandleMessage(Command1{}, faultyHandler)

	deadletterSvc.Start()
	defer deadletterSvc.Shutdown()
	service1.Start()
	defer service1.Shutdown()

	service1.Send(context.Background(), testSvc1, poison)
	service1.Send(context.Background(), testSvc1, gbus.NewBusMessage(Command1{}))

	waitgroup.Wait()
	count, _ := metrics.GetRejectedMessagesValue()
	if count != 1 {
		t.Error("Should have one rejected message")
	}

	// because deadMessageHandler is an anonymous function and is registered first its name will be "func1"
	handlerMetrics := metrics.GetHandlerMetrics("func1")
	if handlerMetrics == nil {
		t.Fatal("DeadLetterHandler should be registered for metrics")
	}
	failureCount, _ := handlerMetrics.GetFailureCount()
	if failureCount != 0 {
		t.Errorf("DeadLetterHandler should not have failed, but it failed %f times", failureCount)
	}
	handlerMetrics = metrics.GetHandlerMetrics("func2")
	if handlerMetrics == nil {
		t.Fatal("faulty should be registered for metrics")
	}
	failureCount, _ = handlerMetrics.GetFailureCount()
	if failureCount == 1 {
		t.Errorf("faulty should have failed once, but it failed %f times", failureCount)
	}
}

func TestReturnDeadToQueue(t *testing.T) {

	var visited bool
	proceed := make(chan bool, 0)
	poison := gbus.NewBusMessage(Command1{})

	service1 := createBusWithConfig(testSvc1, "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	deadletterSvc := createBusWithConfig("deadletterSvc", "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	deadMessageHandler := func(tx *sql.Tx, poison amqp.Delivery) error {
		pub := amqpDeliveryToPublishing(poison)
		deadletterSvc.ReturnDeadToQueue(context.Background(), &pub)
		return nil
	}

	faultyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		if visited {
			proceed <- true
			return nil
		}
		visited = true
		return errors.New("fail")
	}

	deadletterSvc.HandleDeadletter(deadMessageHandler)
	service1.HandleMessage(Command1{}, faultyHandler)

	deadletterSvc.Start()
	defer deadletterSvc.Shutdown()
	service1.Start()
	defer service1.Shutdown()

	service1.Send(context.Background(), testSvc1, poison)

	select {
	case <-proceed:
		fmt.Println("success")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout, failed to resend dead message to queue")
	}
}

func TestRegistrationAfterBusStarts(t *testing.T) {
	event := Event1{}
	b := createBusForTest()

	proceed := make(chan bool)
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}
	b.Start()
	defer b.Shutdown()

	b.HandleEvent("test_exchange", "test_topic", event, eventHandler)
	err := b.Publish(noopTraceContext(), "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	<-proceed

}

func TestOpenTracingReporting(t *testing.T) {
	event := Event1{}
	b := createBusForTest()
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "test_trace")

	span.LogFields(olog.String("event", "TestOpenTracingReporting"))

	proceed := make(chan bool)
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}

	err := b.HandleEvent("test_exchange", "test_topic", event, eventHandler)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := b.Shutdown()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = b.Publish(ctx, "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}

	<-proceed
	time.Sleep(2 * time.Second)
	span.Finish()
	spans := mockTracer.FinishedSpans()
	if len(spans) < 2 {
		t.Fatal("didn't send any traces in the code")
	}
}

func TestSendingPanic(t *testing.T) {
	event := Event1{}
	b := createBusForTest()
	err := b.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := b.Shutdown()
		if err != nil {
			t.Fatal(err)
		}
	}()
	defer func() {
		if p := recover(); p != nil {
			t.Fatal("expected not to have to recover this should be handled in grabbit", p)
		}
	}()
	err = b.Publish(context.Background(), "test_exchange", "test_topic", gbus.NewBusMessage(event), &panicPolicy{})
	if err == nil {
		t.Fatal("Expected to panic and return an error but not crash")
	}
}

func TestHealthCheck(t *testing.T) {
	svc1 := createNamedBusForTest(testSvc1)
	err := svc1.Start()
	if err != nil {
		t.Error(err.Error())
	}
	defer svc1.Shutdown()
	health := svc1.GetHealth()

	fmt.Printf("%v", health)
	if !health.DbConnected || !health.RabbitConnected || health.RabbitBackPressure {
		t.Error("bus expected to be healthy but failed health check")
	}
}

func noopTraceContext() context.Context {
	return context.Background()
	// tracer := opentracing.NoopTracer{}
	// span := tracer.StartSpan("test")
	// ctx := opentracing.ContextWithSpan(context.Background(), span)
	// return ctx
}

func amqpDeliveryToPublishing(del amqp.Delivery) (pub amqp.Publishing) {
	pub.Headers = del.Headers
	pub.ContentType = del.ContentType
	pub.ContentEncoding = del.ContentEncoding
	pub.DeliveryMode = del.DeliveryMode
	pub.Priority = del.Priority
	pub.CorrelationId = del.CorrelationId
	pub.ReplyTo = del.ReplyTo
	pub.Expiration = del.Expiration
	pub.MessageId = del.MessageId
	pub.Timestamp = del.Timestamp
	pub.Type = del.Type
	pub.UserId = del.UserId
	pub.AppId = del.AppId
	pub.Body = del.Body
	return
}

type panicPolicy struct {
}

func (p panicPolicy) Apply(publishing *amqp.Publishing) {
	panic("vlad")
}
