package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"
	slog "github.com/opentracing/opentracing-go/log"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type worker struct {
	*Safety
	channel           *amqp.Channel
	messages          <-chan amqp.Delivery
	rpcMessages       <-chan amqp.Delivery
	q                 amqp.Queue
	rpcq              amqp.Queue
	consumerTag       string
	svcName           string
	rpcLock           *sync.Mutex
	handlersLock      *sync.Mutex
	registrations     []*Registration
	rpcHandlers       map[string]MessageHandler
	deadletterHandler func(tx *sql.Tx, poision amqp.Delivery) error
	isTxnl            bool
	b                 *DefaultBus
	serializer        Serializer
	txProvider        TxProvider
	amqpErrors        chan *amqp.Error
	stop              chan bool
	span              opentracing.Span
}

func (worker *worker) Start() error {

	worker.log().Info("starting worker")
	worker.channel.NotifyClose(worker.amqpErrors)

	var (
		messages, rpcmsgs <-chan amqp.Delivery
		err               error
	)
	if messages, err = worker.createMessagesChannel(worker.q, worker.consumerTag); err != nil {
		return err
	}

	if rpcmsgs, err = worker.createMessagesChannel(worker.rpcq, worker.consumerTag+"_rpc"); err != nil {
		return err
	}
	worker.messages = messages
	worker.rpcMessages = rpcmsgs
	worker.stop = make(chan bool)
	go worker.consumeMessages()

	return nil
}

func (worker *worker) Stop() error {
	worker.log().Info("stopping worker")
	close(worker.stop) // worker.stop <- true
	return nil
}

func (worker *worker) createMessagesChannel(q amqp.Queue, consumerTag string) (<-chan amqp.Delivery, error) {
	msgs, e := worker.channel.Consume(q.Name, /*queue*/
		consumerTag, /*consumer*/
		false,       /*autoAck*/
		false,       /*exclusive*/
		false,       /*noLocal*/
		false,       /*noWait*/
		nil /*args* amqp.Table*/)
	if e != nil {
		return nil, e
	}

	return msgs, nil
}

func (worker *worker) consumeMessages() {

	//TODO:Handle panics due to tx errors so the consumption of messages will continue
	for {

		var isRPCreply bool
		var delivery amqp.Delivery
		var shouldProceed bool

		select {

		case <-worker.stop:
			worker.log().Info("stopped consuming messages")
			return
		case msgDelivery, ok := <-worker.messages:
			if ok {
				shouldProceed = true
			}
			delivery = msgDelivery
			isRPCreply = false
		case rpcDelivery, ok := <-worker.rpcMessages:
			if ok {
				shouldProceed = true
			}
			delivery = rpcDelivery
			isRPCreply = true
		}

		/*
			as the bus shuts down and amqp connection is killed the messages channel (b.msgs) gets closed
			and delivery is a zero value so in order not to panic down the road we return if bus is shutdown
		*/
		if shouldProceed {

			worker.processMessage(delivery, isRPCreply)
		} else {
			worker.log().WithField("message_id", delivery.MessageId).Warn("no proceed")
		}

	}

}

func (worker *worker) extractBusMessage(delivery amqp.Delivery) (*BusMessage, error) {
	bm := NewFromAMQPHeaders(delivery.Headers)
	bm.ID = delivery.MessageId
	bm.CorrelationID = delivery.CorrelationId
	if delivery.Exchange != "" {
		bm.Semantics = EVT
	} else {
		bm.Semantics = CMD
	}
	if bm.PayloadFQN == "" || bm.Semantics == "" {
		//TODO: Log poision pill message
		worker.log().WithFields(log.Fields{"fqn": bm.PayloadFQN, "semantics": bm.Semantics}).Warn("message received but no headers found...rejecting message")

		return nil, errors.New("missing critical headers")
	}

	var decErr error
	bm.Payload, decErr = worker.serializer.Decode(delivery.Body, bm.PayloadFQN)
	if decErr != nil {
		worker.log().WithError(decErr).WithField("message", delivery).Error("failed to decode message. rejected as poison")
		return nil, decErr
	}
	return bm, nil
}

func (worker *worker) resolveHandlers(isRPCreply bool, bm *BusMessage, delivery amqp.Delivery) []MessageHandler {
	handlers := make([]MessageHandler, 0)
	if isRPCreply {
		rpcID, rpcHeaderFound := delivery.Headers[RpcHeaderName].(string)
		if !rpcHeaderFound {
			worker.log().Warn("rpc message received but no rpc header found...rejecting message")
			return handlers
		}
		worker.rpcLock.Lock()
		rpcHandler := worker.rpcHandlers[rpcID]
		worker.rpcLock.Unlock()

		if rpcHandler == nil {
			worker.log().Warn("rpc message received but no rpc header found...rejecting message")
			return handlers
		}

		handlers = append(handlers, rpcHandler)

	} else {
		worker.handlersLock.Lock()
		defer worker.handlersLock.Unlock()
		worker.log().WithFields(log.Fields{"number_of_handlers": len(worker.registrations)}).Info("found message handlers")
		for _, registration := range worker.registrations {
			if registration.Matches(delivery.Exchange, delivery.RoutingKey, bm.PayloadFQN) {
				handlers = append(handlers, registration.Handler)
			}
		}
	}

	return handlers
}

func (worker *worker) ack(delivery amqp.Delivery) error {
	ack := func() error { return delivery.Ack(false /*multiple*/) }
	err := worker.SafeWithRetries(ack, MaxRetryCount)
	if err != nil {
		worker.log().WithError(err).Error("could not ack the message")
		worker.span.LogFields(slog.Error(err))
	}
	return err
}

func (worker *worker) reject(requeue bool, delivery amqp.Delivery) error {
	reject := func() error { return delivery.Reject(requeue /*multiple*/) }
	err := worker.SafeWithRetries(reject, MaxRetryCount)
	if err != nil {
		worker.log().WithError(err).Error("could not reject the message")
		worker.span.LogFields(slog.Error(err))
	}
	return err
}

func (worker *worker) isDead(delivery amqp.Delivery) bool {

	if xDeath := delivery.Headers["x-death"]; xDeath != nil {
		return true
	}
	return false
}

func (worker *worker) invokeDeadletterHandler(delivery amqp.Delivery) {
	tx, txCreateErr := worker.txProvider.New()
	if txCreateErr != nil {
		worker.log().WithError(txCreateErr).Error("failed creating new tx")
		worker.span.LogFields(slog.Error(txCreateErr))
		_ = worker.ack(delivery)
		return
	}
	var fn func() error
	err := worker.deadletterHandler(tx, delivery)
	if err != nil {
		worker.log().WithError(err).Error("failed handling deadletter")
		worker.span.LogFields(slog.Error(err))
		fn = tx.Rollback
	} else {
		fn = tx.Commit
	}
	err = worker.SafeWithRetries(fn, MaxRetryCount)
	if err != nil {
		worker.log().WithError(err).Error("Rollback/Commit deadletter handler message")
		worker.span.LogFields(slog.Error(err))
	}
}

func (worker *worker) processMessage(delivery amqp.Delivery, isRPCreply bool) {
	var ctx context.Context
	var spanOptions []opentracing.StartSpanOption

	spCtx, err := amqptracer.Extract(delivery.Headers)
	if err != nil {
		worker.log().WithError(err).Warn("could not extract SpanContext from headers")
	} else {
		spanOptions = append(spanOptions, opentracing.FollowsFrom(spCtx))
	}
	worker.span, ctx = opentracing.StartSpanFromContext(context.Background(), "processMessage", spanOptions...)

	//catch all error handling so goroutine will not crash
	defer func() {
		if r := recover(); r != nil {
			logEntry := worker.log().WithField("worker", worker.consumerTag)
			if err, ok := r.(error); ok {
				worker.span.LogFields(slog.Error(err))
				logEntry = logEntry.WithError(err)
			} else {
				logEntry = logEntry.WithField("panic", r)
			}
			worker.span.LogFields(slog.String("panic", "failed to process message"))
			logEntry.Error("failed to process message")
		}
		worker.span.Finish()
	}()

	worker.log().WithFields(log.Fields{"worker": worker.consumerTag, "message_id": delivery.MessageId}).Info("GOT MSG")

	//handle a message that originated from a deadletter exchange
	if worker.isDead(delivery) {
		worker.span.LogFields(slog.Error(errors.New("handling dead-letter delivery")))
		worker.log().Info("invoking deadletter handler")
		worker.invokeDeadletterHandler(delivery)
		return
	}

	bm, err := worker.extractBusMessage(delivery)
	if err != nil {
		worker.span.LogFields(slog.Error(err), slog.String("grabbit", "message is poison"))
		//reject poison message
		_ = worker.reject(false, delivery)
		return
	}
	worker.span.LogFields(bm.GetTraceLog()...)

	//TODO:Dedup message
	handlers := worker.resolveHandlers(isRPCreply, bm, delivery)
	if len(handlers) == 0 {
		worker.log().
			WithFields(
				log.Fields{"message-name": bm.PayloadFQN,
					"message-type": bm.Semantics}).
			Warn("Message received but no handlers found")
		worker.span.LogFields(slog.String("grabbit", "no handlers found"))
		// worker.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", bm.PayloadFQN, bm.Semantics)
		//remove the message by acking it and not rejecting it so it will not be routed to a deadletter queue
		_ = worker.ack(delivery)
		return
	}

	var tx *sql.Tx
	var txErr error
	if worker.isTxnl {
		tx, txErr = worker.txProvider.New()
		if txErr != nil {
			worker.log().WithError(txErr).Error("failed to create transaction")
			worker.span.LogFields(slog.Error(txErr))
			//reject the message but requeue it so it gets redelivered until we can create transactions
			_ = worker.reject(true, delivery)
			return
		}
	}
	err = worker.invokeHandlers(ctx, handlers, bm, &delivery, tx)

	// if all handlers executed with out errors then commit the transactional if the bus is transactional
	// if the tranaction committed successfully then ack the message.
	// if the bus is not transactional then just ack the message
	if err == nil {
		if worker.isTxnl {
			err = worker.SafeWithRetries(tx.Commit, MaxRetryCount)
			if err == nil {
				worker.log().Info("bus transaction committed successfully ")
				//ack the message
				_ = worker.ack(delivery)
			} else {
				worker.span.LogFields(slog.Error(err))
				worker.log().WithError(err).Error("failed committing transaction")
				//if the commit failed we will reject the message
				_ = worker.reject(false, delivery)
			}
		} else { /*if the bus in not transactional just try acking the message*/
			_ = worker.ack(delivery)
		}
		//else there was an error in the invokation then try rollingback the transaction and reject the message
	} else {
		worker.span.LogFields(slog.Error(err))
		worker.log().WithError(err).WithFields(log.Fields{"message_name": bm.PayloadFQN, "semantics": bm.Semantics}).Error("Failed to consume message due to failure of one or more handlers.\n Message rejected as poison")

		if worker.isTxnl {
			worker.log().Warn("rolling back transaction")
			err = worker.SafeWithRetries(tx.Rollback, MaxRetryCount)

			if err != nil {
				worker.span.LogFields(slog.Error(err))
				worker.log().WithError(err).Error("failed to rollback transaction")
			}
		}

		_ = worker.reject(false, delivery)
	}
}

func (worker *worker) invokeHandlers(sctx context.Context, handlers []MessageHandler, message *BusMessage, delivery *amqp.Delivery, tx *sql.Tx) (err error) {

	action := func(attempts uint) (actionErr error) {
		worker.span, sctx = opentracing.StartSpanFromContext(sctx, "invokeHandlers")
		worker.span.LogFields(slog.Uint64("attempt", uint64(attempts+1)))
		defer func() {
			if p := recover(); p != nil {
				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				worker.log().WithField("stack", pncMsg).Error("recovered from panic while invoking handler")
				actionErr = errors.New(pncMsg)
				worker.span.LogFields(slog.Error(actionErr))
			}
			worker.span.Finish()
		}()
		for _, handler := range handlers {
			hspan, hsctx := opentracing.StartSpanFromContext(sctx, runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name())
			hspan.Finish()

			ctx := &defaultInvocationContext{
				invocingSvc: delivery.ReplyTo,
				bus:         worker.b,
				inboundMsg:  message,
				tx:          tx,
				ctx:         hsctx,
				exchange:    delivery.Exchange,
				routingKey:  delivery.RoutingKey}
			e := handler(ctx, message)
			if e != nil {
				hspan.LogFields(slog.Error(e))
				return e
			}
		}
		return nil
	}

	//retry for MaxRetryCount, back off by a Fibonacci series 50, 50, 100, 150, 250 ms
	return retry.Retry(action,
		strategy.Limit(MaxRetryCount),
		strategy.Backoff(backoff.Fibonacci(50*time.Millisecond)))
}

func (worker *worker) log() *log.Entry {

	return log.WithFields(log.Fields{
		"_service": worker.svcName})
}

func (worker *worker) AddRegistration(registration *Registration) {
	worker.handlersLock.Lock()
	defer worker.handlersLock.Unlock()
	worker.registrations = append(worker.registrations, registration)
}
