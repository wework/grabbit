package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/wework/grabbit/gbus/metrics"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/jitter"
	"github.com/Rican7/retry/strategy"
	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"
	slog "github.com/opentracing/opentracing-go/log"
	"github.com/sirupsen/logrus"
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
	b                 *DefaultBus
	serializer        Serializer
	txProvider        TxProvider
	amqpErrors        chan *amqp.Error
	stop              chan bool
	span              opentracing.Span
}

func (worker *worker) Start() error {

	worker.log().Info("starting worker")
	worker.stop = make(chan bool)
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
		//TODO: Log poison pill message
		worker.log().WithFields(logrus.Fields{"fqn": bm.PayloadFQN, "semantics": bm.Semantics}).Warn("message received but no headers found...rejecting message")

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

		for _, registration := range worker.registrations {
			if registration.Matches(delivery.Exchange, delivery.RoutingKey, bm.PayloadFQN) {
				handlers = append(handlers, registration.Handler)
			}
		}
	}

	worker.log().WithFields(logrus.Fields{"number_of_handlers": len(handlers)}).Info("found message handlers")
	return handlers
}

func (worker *worker) ack(delivery amqp.Delivery) error {
	ack := func(attempts uint) error { return delivery.Ack(false /*multiple*/) }
	worker.log().WithField("message_id", delivery.MessageId).Debug("acking message")
	err := retry.Retry(ack,
		strategy.Wait(100*time.Millisecond))

	if err != nil {
		worker.log().WithError(err).Error("could not ack the message")
		worker.span.LogFields(slog.Error(err))
	} else {
		worker.log().WithField("message_id", delivery.MessageId).Debug("message acked")
	}

	return err
}

func (worker *worker) reject(requeue bool, delivery amqp.Delivery) error {
	reject := func(attempts uint) error { return delivery.Reject(requeue /*multiple*/) }
	worker.log().WithFields(logrus.Fields{"message_id": delivery.MessageId, "requeue": requeue}).Info("rejecting message")
	err := retry.Retry(reject,
		strategy.Wait(100*time.Millisecond))
	if err != nil {
		worker.log().WithError(err).Error("could not reject the message")
		worker.span.LogFields(slog.Error(err))
	}
	worker.log().WithFields(logrus.Fields{"message_id": delivery.MessageId, "requeue": requeue}).Info("message rejected")
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
		_ = worker.reject(true, delivery)
		return
	}
	err := worker.deadletterHandler(tx, delivery)
	var reject bool
	if err != nil {
		worker.log().WithError(err).Error("failed handling deadletter")
		worker.span.LogFields(slog.Error(err))
		err = worker.SafeWithRetries(tx.Rollback, MaxRetryCount)
		reject = true
	} else {
		err = worker.SafeWithRetries(tx.Commit, MaxRetryCount)
	}

	if err != nil {
		worker.log().WithError(err).Error("Rollback/Commit deadletter handler message")
		worker.span.LogFields(slog.Error(err))
		reject = true
	}

	if reject {
		_ = worker.reject(true, delivery)
	} else {
		_ = worker.ack(delivery)
	}
}

func (worker *worker) processMessage(delivery amqp.Delivery, isRPCreply bool) {
	var ctx context.Context
	var spanOptions []opentracing.StartSpanOption

	spCtx, err := amqptracer.Extract(delivery.Headers)
	if err != nil {
		worker.log().WithError(err).Debug("could not extract SpanContext from headers")
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

	worker.log().WithFields(logrus.Fields{"worker": worker.consumerTag, "message_id": delivery.MessageId}).Info("GOT MSG")

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
				logrus.Fields{"message-name": bm.PayloadFQN,
					"message-type": bm.Semantics}).
			Warn("Message received but no handlers found")
		worker.span.LogFields(slog.String("grabbit", "no handlers found"))
		// worker.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", bm.PayloadFQN, bm.Semantics)
		//remove the message by acking it and not rejecting it so it will not be routed to a deadletter queue
		_ = worker.ack(delivery)
		return
	}

	err = worker.invokeHandlers(ctx, handlers, bm, &delivery)
	if err == nil {
		_ = worker.ack(delivery)
	} else {
		_ = worker.reject(false, delivery)
		metrics.ReportRejectedMessage()
	}
}

func (worker *worker) invokeHandlers(sctx context.Context, handlers []MessageHandler, message *BusMessage, delivery *amqp.Delivery) (err error) {

	//this is the action that will get retried
	// each retry should run a new and separate transaction which should end with a commit or rollback

	action := func(attempt uint) (actionErr error) {
		
		tx, txCreateErr := worker.txProvider.New()
			if txCreateErr != nil {
				worker.log().WithError(txCreateErr).Error("failed creating new tx")
				worker.span.LogFields(slog.Error(txCreateErr))
				return txCreateErr
			}

		worker.span, sctx = opentracing.StartSpanFromContext(sctx, "invokeHandlers")
		worker.span.LogFields(slog.Uint64("attempt", uint64(attempt+1)))
		defer func() {
			if p := recover(); p != nil {
				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				worker.log().WithField("stack", pncMsg).Error("recovered from panic while invoking handler")
				actionErr = errors.New(pncMsg)
				rbkErr := tx.Rollback()
					if rbkErr != nil {
						worker.log().WithError(rbkErr).Error("failed rolling back transaction when recovering from handler panic")
					}
				worker.span.LogFields(slog.Error(actionErr))
			}
			worker.span.Finish()
		}()
		var handlerErr error
		var hspan opentracing.Span
		var hsctx context.Context
		for _, handler := range handlers {
			hspan, hsctx = opentracing.StartSpanFromContext(sctx, handler.Name())

			ctx := &defaultInvocationContext{
				invocingSvc: delivery.ReplyTo,
				bus:         worker.b,
				inboundMsg:  message,
				tx:          tx,
				ctx:         hsctx,
				exchange:    delivery.Exchange,
				routingKey:  delivery.RoutingKey,
				deliveryInfo: DeliveryInfo{
					Attempt:       attempt,
					MaxRetryCount: MaxRetryCount,
				},
			}
			ctx.SetLogger(worker.log().WithField("handler", handler.Name()))
			handlerErr = metrics.RunHandlerWithMetric(func() error {
				return handler(ctx, message)
			}, handler.Name(), worker.log())
			if handlerErr != nil {
				hspan.LogFields(slog.Error(handlerErr))
				break
			}
			hspan.Finish()
		}
		if handlerErr != nil {
			hspan.LogFields(slog.Error(handlerErr))
			rbkErr := tx.Rollback()
				if rbkErr != nil {
					worker.log().WithError(rbkErr).Error("failed rolling back transaction when recovering from handler error")
				}
			hspan.Finish()
			return handlerErr
		}
		cmtErr := tx.Commit()
			if cmtErr != nil {
				worker.log().WithError(cmtErr).Error("failed committing transaction after invoking handlers")
				return cmtErr
			}
		return nil
	}

	//retry for MaxRetryCount, back off by a jittered strategy
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	return retry.Retry(action,
		strategy.Limit(MaxRetryCount),
		strategy.BackoffWithJitter(
			backoff.BinaryExponential(BaseRetryDuration),
			jitter.Deviation(random, 0.5),
		))
}

func (worker *worker) log() logrus.FieldLogger {
	return worker.b.Log()
}

func (worker *worker) AddRegistration(registration *Registration) {
	worker.handlersLock.Lock()
	defer worker.handlersLock.Unlock()
	worker.registrations = append(worker.registrations, registration)
}
