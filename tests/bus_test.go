package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wework/grabbit/gbus/serialization"

	"github.com/wework/grabbit/gbus/metrics"
	"github.com/wework/grabbit/gbus/policy"

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
	defer assertBusShutdown(b, t)

	err = b.Send(context.Background(), testSvc1, gbus.NewBusMessage(cmd))
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	proceedOrTimeout(2, proceed, nil, t)
}

func TestReply(t *testing.T) {
	cmd := Command1{}
	reply := Reply1{}
	cmdBusMsg := gbus.NewBusMessage(cmd)

	svc1 := createNamedBusForTest(testSvc1)
	svc2 := createNamedBusForTest(testSvc2)

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		err := invocation.Reply(context.Background(), gbus.NewBusMessage(reply))
		if err != nil {
			t.Errorf("could not send reply with error: %s", err.Error())
			return err
		}
		return nil

	}

	replyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		_, ok := message.Payload.(*Reply1)
		if !ok {
			t.Errorf("message handler for reply message invoked with wrong message type\r\n%v", message)
		}

		if message.CorrelationID != cmdBusMsg.ID {
			t.Errorf("CorrelationID didn't match expected %s but was %s", cmdBusMsg.ID, message.CorrelationID)
		}

		proceed <- true
		return nil
	}

	svc2.HandleMessage(cmd, cmdHandler)
	svc1.HandleMessage(reply, replyHandler)

	svc1.Start()
	defer assertBusShutdown(svc1, t)

	svc2.Start()
	defer assertBusShutdown(svc2, t)

	svc1.Send(context.Background(), testSvc2, cmdBusMsg)
	proceedOrTimeout(2, proceed, nil, t)
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
	defer assertBusShutdown(b, t)
	err := b.Publish(context.Background(), "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	proceedOrTimeout(2, proceed, nil, t)
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
	defer assertBusShutdown(b, t)
	err := b.Publish(context.Background(), "test_exchange", "a.b.c", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	proceedOrTimeout(2, proceed, nil, t)
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
		return invocation.Reply(context.Background(), reply)
	}

	bus.HandleMessage(c1, cmdHandler)
	bus.HandleMessage(r1, handleRetry)

	bus.Start()
	defer assertBusShutdown(bus, t)

	bus.Send(context.Background(), testSvc1, cmd)
	<-handlerRetryProceed

	hm := metrics.GetHandlerMetrics("handleRetry")
	if hm == nil {
		t.Error("Metrics for handleRetry should be initiated")
	}
	f, _ := hm.GetFailureCount()
	mtf, _ := metrics.GetFailureCountByMessageTypeAndHandlerName(reply.PayloadFQN, "handleRetry")
	s, _ := hm.GetSuccessCount()
	mts, _ := metrics.GetSuccessCountByMessageTypeAndHandlerName(reply.PayloadFQN, "handleRetry")

	if f != 2 {
		t.Errorf("Failure count should be 2 but was %f", f)
	}
	if mtf != 2 {
		t.Errorf("Failure count should be 2 but was %f", mtf)
	}
	if s != 1 {
		t.Errorf("Success count should be 1 but was %f", s)
	}
	if mts != 1 {
		t.Errorf("Success count should be 1 but was %f", mts)
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

		return invocation.Reply(context.Background(), reply)
	}

	svc1 := createNamedBusForTest(testSvc1)
	svc1.HandleMessage(c1, handler)
	svc1.Start()
	defer assertBusShutdown(svc1, t)
	svc2 := createNamedBusForTest(testSvc2)
	svc2.Start()
	defer assertBusShutdown(svc2, t)
	t.Log("Sending RPC")
	reply, _ = svc2.RPC(context.Background(), testSvc1, cmd, reply, 5*time.Second)
	t.Log("Tested RPC")
	if reply == nil {
		t.Fail()
	}

}

func TestDeadlettering(t *testing.T) {
	metrics.ResetRejectedMessagesCounter()

	proceed := make(chan bool)
	poison := gbus.NewBusMessage(PoisonMessage{})
	service1 := createNamedBusForTest(testSvc1)
	deadletterSvc := createNamedBusForTest("deadletterSvc")

	deadMessageHandler := func(tx *sql.Tx, poison *amqp.Delivery) error {
		proceed <- true
		return nil
	}

	faultyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		return errors.New("TestDeadlettering simulating a failed handler")
	}

	deadletterSvc.HandleDeadletter(deadMessageHandler)
	service1.HandleMessage(Command1{}, faultyHandler)

	deadletterSvc.Start()
	defer assertBusShutdown(deadletterSvc, t)
	service1.Start()
	defer assertBusShutdown(service1, t)

	cmd := gbus.NewBusMessage(Command1{})
	service1.Send(context.Background(), testSvc1, poison)
	service1.Send(context.Background(), testSvc1, cmd)

	proceedOrTimeout(2, proceed, nil, t)

	count, _ := metrics.GetRejectedMessagesValue()
	if count != 1 {
		t.Error("Should have one rejected message")
	}

	//because deadMessageHandler is an anonymous function and is registered first its name will be "func1"
	handlerMetrics := metrics.GetHandlerMetrics("func1")
	if handlerMetrics == nil {
		t.Fatal("DeadLetterHandler should be registered for metrics")
	}
	failureCount, _ := handlerMetrics.GetFailureCount()
	if failureCount != 0 {
		t.Errorf("DeadLetterHandler should not have failed, but it failed %f times", failureCount)
	}
	poisonF, _ := metrics.GetFailureCountByMessageTypeAndHandlerName(poison.PayloadFQN, "func1")
	if poisonF != 0 {
		t.Errorf("DeadLetterHandler should not have failed, but it failed %f times", poisonF)
	}
	handlerMetrics = metrics.GetHandlerMetrics("func2")
	if handlerMetrics == nil {
		t.Fatal("faulty should be registered for metrics")
	}
	failureCount, _ = handlerMetrics.GetFailureCount()
	if failureCount == 1 {
		t.Errorf("faulty should have failed once, but it failed %f times", failureCount)
	}
	cmdF, _ := metrics.GetFailureCountByMessageTypeAndHandlerName(cmd.PayloadFQN, "func2")
	if cmdF == 1 {
		t.Errorf("faulty should have failed once, but it failed %f times", cmdF)
	}
}

func TestRawMessageHandling(t *testing.T) {

	proceed := make(chan bool)
	handler := func(tx *sql.Tx, delivery *amqp.Delivery) error {
		proceed <- true
		return nil
	}
	svc1 := createNamedBusForTest(testSvc1)
	svc1.SetGlobalRawMessageHandler(handler)
	_ = svc1.Start()
	defer assertBusShutdown(svc1, t)

	cmd1 := gbus.NewBusMessage(Command1{})
	_ = svc1.Send(context.Background(), testSvc1, cmd1)

	proceedOrTimeout(2, proceed, nil, t)
}

func TestGlobalRawMessageHandlingErr(t *testing.T) {
	metrics.ResetRejectedMessagesCounter()
	/*
		tests issues:
		https://github.com/wework/grabbit/issues/187
		https://github.com/wework/grabbit/issues/188
	*/
	var otherHandlerCalled bool

	handler := func(tx *sql.Tx, delivery *amqp.Delivery) error {

		return errors.New("other handlers should not be called")
	}

	//this handler should not be invoked by the bus, if it does the test should fail
	otherHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {

		otherHandlerCalled = true
		return nil
	}
	svc1 := createNamedBusForTest(testSvc1)
	svc1.SetGlobalRawMessageHandler(handler)
	svc1.HandleMessage(Command1{}, otherHandler)
	_ = svc1.Start()
	defer assertBusShutdown(svc1, t)

	cmd1 := gbus.NewBusMessage(Command1{})
	_ = svc1.Send(context.Background(), testSvc1, cmd1)

	if otherHandlerCalled == true {
		t.Fail()
	}

	//delay test execution so to make sure the second handler is not called
	time.Sleep(1500 * time.Millisecond)
	rejected, _ := metrics.GetRejectedMessagesValue()
	if rejected != 1 {
		t.Errorf("rejected messages metric was expected to be 1 but was %f", rejected)
	}

	if otherHandlerCalled {
		t.Errorf("other handler that was not expected to be called was called ")
	}
}

func TestReturnDeadToQueue(t *testing.T) {

	var visitedMessageHandler, visitedEventHandler bool
	messageProceed, eventProceed := make(chan bool), make(chan bool)

	poison := gbus.NewBusMessage(Command1{})

	service1 := createBusWithConfig(testSvc1, "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	deadletterSvc := createBusWithConfig("deadletterSvc", "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	deadMessageHandler := func(tx *sql.Tx, poison *amqp.Delivery) error {
		pub := amqpDeliveryToPublishing(poison)
		err := deadletterSvc.ReturnDeadToQueue(context.Background(), &pub)
		if err != nil {
			t.Fatalf("failed returning dead to queue with error: %s", err.Error())
		}
		return nil
	}

	faultyMessageHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		if visitedMessageHandler {
			messageProceed <- true
			return nil
		}
		visitedMessageHandler = true
		return errors.New("fail")
	}

	faultyEventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		if visitedEventHandler {
			eventProceed <- true
			return nil
		}
		visitedEventHandler = true
		return errors.New("fail")
	}

	deadletterSvc.HandleDeadletter(deadMessageHandler)
	service1.HandleMessage(Command1{}, faultyMessageHandler)
	service1.HandleEvent("exchange", "topic", Command1{}, faultyEventHandler)

	deadletterSvc.Start()
	defer assertBusShutdown(deadletterSvc, t)
	service1.Start()
	defer assertBusShutdown(service1, t)

	service1.Send(context.Background(), testSvc1, poison)
	proceedOrTimeout(2, messageProceed, nil, t)

	service1.Publish(context.Background(), "exchange", "topic", poison)
	proceedOrTimeout(2, eventProceed, nil, t)
}

func TestDeadLetterHandlerPanic(t *testing.T) {
	proceed := make(chan bool)
	metrics.ResetRejectedMessagesCounter()
	poison := gbus.NewBusMessage(Command1{})
	service1 := createBusWithConfig(testSvc1, "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	deadletterSvc := createBusWithConfig("deadletterSvc", "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})
	visited := false
	deadMessageHandler := func(tx *sql.Tx, poison *amqp.Delivery) error {
		/*
			this handler will be called more than once since when grabbit rejects
			a message from a deadletter queue to rejects it with the requeu option set to
			true and that is why this will be called more than once even though the retry count
			is set to 0

		*/

		if !visited {
			visited = true
			panic("PANIC DEAD HANDLER aaahhh!!!!!!")
		}
		proceed <- true
		return nil
	}

	faultyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		return errors.New("fail")
	}

	deadletterSvc.HandleDeadletter(deadMessageHandler)
	err := service1.HandleMessage(Command1{}, faultyHandler)
	if err != nil {
		t.Error("failed to register faultyhandler")
	}

	deadletterSvc.Start()
	defer assertBusShutdown(deadletterSvc, t)
	service1.Start()
	defer assertBusShutdown(service1, t)

	service1.Send(context.Background(), testSvc1, poison)
	proceedOrTimeout(2, proceed, func() {
		count, _ := metrics.GetRejectedMessagesValue()
		//we expect only 1 rejcted meessage from the counter since rejected messages that get
		//requeued are not reported to the metric so the counter won't be increment when the message
		//in the dlq gets rejected as it is rejected with the requeue option set to true
		if count != 1 {
			t.Errorf("Should have 1 rejected messages but was %v", count)
		}
	}, t)
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
	defer assertBusShutdown(b, t)

	b.HandleEvent("test_exchange", "test_topic", event, eventHandler)
	err := b.Publish(context.Background(), "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	proceedOrTimeout(2, proceed, nil, t)

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
	defer assertBusShutdown(b, t)
	err = b.Publish(ctx, "test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}

	proceedOrTimeout(2, proceed, func() {
		time.Sleep(2 * time.Second)
		span.Finish()
		spans := mockTracer.FinishedSpans()
		if len(spans) < 2 {
			t.Fatal("didn't send any traces in the code")
		}
	}, t)
}

func TestSendingPanic(t *testing.T) {
	event := Event1{}
	b := createBusForTest()
	err := b.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer assertBusShutdown(b, t)
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

func TestEmptyBody(t *testing.T) {
	b := createNamedBusForTest(testSvc1)
	proceed := make(chan bool)
	b.SetGlobalRawMessageHandler(func(tx *sql.Tx, delivery *amqp.Delivery) error {
		proceed <- true
		return nil
	})

	err := b.Start()
	if err != nil {
		t.Errorf("could not start bus for test error: %s", err.Error())
	}
	defer assertBusShutdown(b, t)
	conn, err := amqp.Dial(connStr)
	if err != nil {
		t.Error("couldnt connect to rabbitmq")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Error("couldnt open rabbitmq channel for publishing")
	}
	defer ch.Close()

	cmd := amqp.Publishing{}
	err = ch.Publish("", testSvc1, true, false, cmd)
	if err != nil {
		t.Error("couldnt send message on rabbitmq channel")
	}
	proceedOrTimeout(2, proceed, nil, t)
}

func TestEmptyMessageInvokesDeadHanlder(t *testing.T) {
	/*
		test call for dead letter handler when a message with nil or len 0 body is consumed. the handler
		should handle the message successfully.
	*/

	b := createBusWithConfig(testSvc1, "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	proceed := make(chan bool)
	b.HandleDeadletter(func(tx *sql.Tx, delivery *amqp.Delivery) error {
		proceed <- true
		return nil
	})

	err := b.Start()
	if err != nil {
		t.Errorf("could not start bus for test error: %s", err.Error())
	}
	defer assertBusShutdown(b, t)

	conn, err := amqp.Dial(connStr)
	if err != nil {
		t.Error("couldnt connect to rabbitmq")
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Error("couldnt open rabbitmq channel for publishing")
	}
	defer ch.Close()

	headersMap := make(map[string]interface{})
	headersMap["x-death"] = make([]interface{}, 0)
	cmd := amqp.Publishing{Headers: headersMap}
	err = ch.Publish("", testSvc1, true, false, cmd)
	if err != nil {
		t.Error("couldnt send message on rabbitmq channel")
	}
	proceedOrTimeout(2, proceed, nil, t)
}

func TestOnlyRawMessageHandlersInvoked(t *testing.T) {
	/*
		The global and dead letter handlers can consume message with nil body but
		"normal" handlers cannot.
		If a "normal" handler is registered for this type of message, the bus must reject this message.
	*/
	metrics.ResetRejectedMessagesCounter()
	b := createBusWithConfig(testSvc1, "grabbit-dead1", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0})

	proceed := make(chan bool)
	b.HandleDeadletter(func(tx *sql.Tx, delivery *amqp.Delivery) error {
		proceed <- true
		return nil
	})
	err := b.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		t.Error("handler invoked for non-grabbit message")
		return nil
	})
	if err != nil {
		t.Errorf("could not register handler for bus %s", err.Error())
	}

	err = b.Start()
	if err != nil {
		t.Errorf("could not start bus for test error: %s", err.Error())
	}
	defer assertBusShutdown(b, t)

	conn, err := amqp.Dial(connStr)
	if err != nil {
		t.Error("couldnt connect to rabbitmq")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Error("couldnt open rabbitmq channel for publishing")
	}
	defer ch.Close()

	headersMap := make(map[string]interface{})
	headersMap["x-msg-name"] = Command1{}.SchemaName()
	cmd := amqp.Publishing{Headers: headersMap}
	err = ch.Publish("", testSvc1, true, false, cmd)
	if err != nil {
		t.Error("couldnt send message on rabbitmq channel")
	}

	proceedOrTimeout(2, proceed, func() {
		count, _ := metrics.GetRejectedMessagesValue()
		if count != 1 {
			t.Error("Should have one rejected message")
		}
	}, t)
}

func TestTypeAndContentTypeHeadersSet(t *testing.T) {
	cmd := Command1{}

	bus := createNamedBusForTest(testSvc1)

	policy := &policy.Generic{
		Funk: func(publishing *amqp.Publishing) {
			if publishing.Type != cmd.SchemaName() {
				t.Errorf("publishing.Type != cmd.SchemaName()")
			}
			dfb := bus.(*gbus.DefaultBus)
			if publishing.ContentType != dfb.Serializer.Name() {
				t.Errorf("expected %s as content-type but actual value was %s", dfb.Serializer.Name(), publishing.ContentType)
			}
		}}

	bus.Start()
	defer bus.Shutdown()
	bus.Send(context.Background(), testSvc1, gbus.NewBusMessage(cmd), policy)
}

func TestSendEmptyBody(t *testing.T) {
	/*
			test sending of message with len(payload) == 0 .
		    for example, the body of proto message with 1 "false" field is len 0.
	*/

	logger := log.WithField("test", "empty_body")
	serializer := serialization.NewProtoSerializer(logger)
	msg := EmptyProtoCommand{}
	cmd := gbus.NewBusMessage(&msg)
	proceed := make(chan bool)

	cfgSerializer := func(builder gbus.Builder) {
		builder.WithSerializer(serializer)
	}
	b := createBusWithConfig(testSvc1, "grabbit-dead", true, true,
		gbus.BusConfiguration{MaxRetryCount: 0, BaseRetryDuration: 0}, cfgSerializer)

	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}

	err := b.HandleMessage(&EmptyProtoCommand{}, handler)
	if err != nil {
		t.Errorf("Registering handler returned false, expected true with error: %s", err.Error())
	}

	err = b.Start()
	if err != nil {
		t.Errorf("could not start bus for test error: %s", err.Error())
	}
	defer assertBusShutdown(b, t)

	err = b.Send(context.Background(), testSvc1, cmd)
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	proceedOrTimeout(2, proceed, nil, t)
}

func TestHealthCheck(t *testing.T) {
	svc1 := createNamedBusForTest(testSvc1)
	err := svc1.Start()
	if err != nil {
		t.Error(err.Error())
	}
	defer assertBusShutdown(svc1, t)
	health := svc1.GetHealth()

	fmt.Printf("%v", health)
	if !health.DbConnected || !health.RabbitConnected || health.RabbitBackPressure {
		t.Error("bus expected to be healthy but failed health check")
	}
}

func TestSanitizingSvcName(t *testing.T) {
	svc4 := createNamedBusForTest(testSvc4)
	err := svc4.Start()
	if err != nil {
		t.Error(err.Error())
	}
	defer assertBusShutdown(svc4, t)

	fmt.Println("succeeded sanitizing service name")
}

func TestIdempotencyKeyHeaders(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	keys := make([]string, 0)
	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		keys = append(keys, message.IdempotencyKey)
		wg.Done()
		return nil
	}

	bus := createNamedBusForTest(testSvc1)

	bus.HandleMessage(Command1{}, handler)
	bus.Start()
	defer bus.Shutdown()

	cmd1 := gbus.NewBusMessage(Command1{})
	cmd1.SetIdempotencyKey("some-unique-key")

	cmd2 := gbus.NewBusMessage(Command1{})
	cmd2.SetIdempotencyKey("some-unique-key")

	//send two commands with the same IdempotencyKey to test that the same IdempotencyKey is propogated
	bus.Send(context.Background(), testSvc1, cmd1)
	bus.Send(context.Background(), testSvc1, cmd2)

	wg.Wait()

	if keys[0] != keys[1] && keys[0] != "" {
		t.Errorf("expected same IdempotencyKey. actual key1:%s, key2%s", keys[0], keys[1])
	}

}

func amqpDeliveryToPublishing(del *amqp.Delivery) (pub amqp.Publishing) {
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

func assertBusShutdown(bus gbus.Bus, t *testing.T) {
	err := bus.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
}

func proceedOrTimeout(timeout time.Duration, p chan bool, onProceed func(), t *testing.T) {
	select {
	case <-p:
		if onProceed != nil {
			onProceed()
		}
	case <-time.After(timeout * time.Second * 5):
		t.Fatal("timeout")
	}
}

func TestDeduplicationReject(t *testing.T) {
	metrics.ResetDuplicateMessagesCounters()
	cmd := Command1{
		Data: "Command1",
	}
	proceed := make(chan bool)
	b := createBusWithConfig(testSvc1, "dead-grabbit", true, true, gbus.BusConfiguration{MaxRetryCount: 4, BaseRetryDuration: 15}, func(builder gbus.Builder) {
		builder.WithDeduplicationPolicy(gbus.DeduplicationPolicyReject, 1*time.Hour)
	})

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
	defer assertBusShutdown(b, t)

	bm := gbus.NewBusMessage(cmd)
	err = b.Send(context.Background(), testSvc1, bm)
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	proceedOrTimeout(2, proceed, nil, t)
	err = b.Send(context.Background(), testSvc1, bm)
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	done := make(chan bool)
	ticker := time.NewTicker(500 * time.Millisecond)
	rejected := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				rejectedValue, err := metrics.GetDuplicateMessageRejectValue()
				if err != nil {
					return
				}
				if rejectedValue == 1 {
					rejected <- true
				}
			}
		}

	}()
	proceedOrTimeout(2, rejected, nil, t)
	done <- true
}

func TestDeduplicationAcked(t *testing.T) {
	metrics.ResetDuplicateMessagesCounters()
	cmd := Command1{
		Data: "Command1",
	}
	proceed := make(chan bool)
	b := createBusWithConfig(testSvc1, "dead-grabbit", true, true, gbus.BusConfiguration{MaxRetryCount: 4, BaseRetryDuration: 15}, func(builder gbus.Builder) {
		builder.WithDeduplicationPolicy(gbus.DeduplicationPolicyAck, 1*time.Hour)
	})

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
	defer assertBusShutdown(b, t)

	bm := gbus.NewBusMessage(cmd)
	err = b.Send(context.Background(), testSvc1, bm)
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	proceedOrTimeout(2, proceed, nil, t)
	err = b.Send(context.Background(), testSvc1, bm)
	if err != nil {
		t.Errorf("could not send message error: %s", err.Error())
		return
	}
	done := make(chan bool)
	ticker := time.NewTicker(500 * time.Millisecond)
	acked := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				ackedValue, err := metrics.GetDuplicateMessageAckValue()
				if err != nil {
					return
				}
				if ackedValue == 1 {
					acked <- true
				}
			}
		}

	}()
	proceedOrTimeout(2, acked, nil, t)
	done <- true
}
