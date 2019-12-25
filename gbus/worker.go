package gbus

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/wework/grabbit/gbus/deduplicator"
	"github.com/wework/grabbit/gbus/metrics"

	"emperror.dev/errors"
	logrushandler "emperror.dev/handler/logrus"
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
	deadletterHandler RawMessageHandler
	globalRawHandler  RawMessageHandler
	b                 *DefaultBus
	serializer        Serializer
	txProvider        TxProvider
	amqpErrors        chan *amqp.Error
	stop              chan bool
	span              opentracing.Span
	duplicateStore    deduplicator.Store
	delicatePolicy    DeduplicationPolicy
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
	go worker.consumeRPC()
	return nil
}

func (worker *worker) Stop() error {
	worker.log().Info("stopping worker")
	e1 := worker.channel.Cancel(worker.consumerTag, false)
	e2 := worker.channel.Cancel(worker.consumerTag+"_rpc", false)
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
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

	for msg := range worker.messages {
		worker.processMessage(msg, false)
	}
}

func (worker *worker) consumeRPC() {

	for msg := range worker.rpcMessages {
		worker.processMessage(msg, true)
	}
}

func (worker *worker) extractBusMessage(delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) (*BusMessage, error) {
	bm, err := NewFromDelivery(delivery)
	if err != nil {
		msgSpecificLogEntry.Warn("failed creating BusMessage from AMQP delivery")
		return nil, err
	}

	var decErr error
	bm.Payload, decErr = worker.serializer.Decode(delivery.Body, bm.PayloadFQN)
	if decErr != nil {
		msgSpecificLogEntry.WithError(decErr).Error("failed to decode message. rejected as poison")
		return nil, decErr
	}
	return bm, nil
}

func (worker *worker) resolveHandlers(isRPCreply bool, delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) []MessageHandler {
	handlers := make([]MessageHandler, 0)
	if isRPCreply {
		rpcID, rpcHeaderFound := delivery.Headers[RPCHeaderName].(string)
		if !rpcHeaderFound {
			msgSpecificLogEntry.Warn("rpc message received but no rpc header found...rejecting message")
			return handlers
		}
		worker.rpcLock.Lock()
		rpcHandler := worker.rpcHandlers[rpcID]
		worker.rpcLock.Unlock()

		if rpcHandler == nil {
			msgSpecificLogEntry.Warn("rpc message received but no rpc header found...rejecting message")
			return handlers
		}

		handlers = append(handlers, rpcHandler)

	} else {
		exchange, routingKey, err := getRoutingParamsFromDelivery(delivery)
		if err != nil {
			msgSpecificLogEntry.WithError(err).Warn("failed extracting exchange and routingKey from delivery...rejecting message")
			return handlers
		}

		worker.handlersLock.Lock()
		defer worker.handlersLock.Unlock()
		msgName := GetMessageName(delivery)
		for _, registration := range worker.registrations {
			if registration.Matches(exchange, routingKey, msgName) {
				handlers = append(handlers, registration.Handler)
			}
		}
	}

	return handlers
}

func getRoutingParamsFromDelivery(delivery amqp.Delivery) (exchange string, routingKey string, err error) {

	// In order to route "resurrected" events to the correct handler it is necessary to use the original exchange and routing-key which
	// were added by grabbit previously as headers, as opposed to the native values on the delivery
	// Issue reference:	https://github.com/wework/grabbit/issues/191
	if isResurrectedMessage(delivery) {
		exchange, ok := delivery.Headers["x-first-death-exchange"].(string)
		if !ok {
			return "", "", errors.New("failed extracting exchange from resurrected message, bad x-first-death-exchange")
		}
		routingKey, ok := delivery.Headers[FirstDeathRoutingKeyHeaderName].(string)
		if !ok {
			return "", "", errors.New("failed extracting routing-key from resurrected message, bad x-first-death-routing-key")
		}
		return exchange, routingKey, nil
	}

	return delivery.Exchange, delivery.RoutingKey, nil
}

func (worker *worker) ack(delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) error {
	ack := func(attempts uint) error { return delivery.Ack(false /*multiple*/) }
	msgSpecificLogEntry.Debug("acking message")
	err := retry.Retry(ack,
		strategy.Wait(100*time.Millisecond))

	if err != nil {
		msgSpecificLogEntry.WithError(err).Error("could not ack the message")
		worker.span.LogFields(slog.Error(err))
	} else {
		msgSpecificLogEntry.Debug("message acked")
	}

	return err
}

func (worker *worker) reject(requeue bool, delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) error {
	reject := func(attempts uint) error { return delivery.Reject(requeue /*multiple*/) }
	msgSpecificLogEntry.WithField("requeue", requeue).Info("rejecting message")
	err := retry.Retry(reject,
		strategy.Wait(100*time.Millisecond))
	if err != nil {
		worker.log().WithError(err).Error("could not reject the message")
		worker.span.LogFields(slog.Error(err))
	}
	if !requeue {
		metrics.ReportRejectedMessage()
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

func (worker *worker) invokeDeadletterHandler(delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) {
	txWrapper := func(tx *sql.Tx) error {
		handlerWrapper := func() error {
			return worker.deadletterHandler(tx, &delivery)
		}
		return metrics.RunHandlerWithMetric(handlerWrapper, worker.deadletterHandler.Name(), fmt.Sprintf("deadletter_%s", delivery.Type), worker.log())
	}

	err := worker.withTx(txWrapper, msgSpecificLogEntry)
	if err != nil {
		//we reject the deelivery but requeue it so the message will not be lost and recovered to the dlq
		_ = worker.reject(true, delivery, msgSpecificLogEntry)
	} else {
		_ = worker.ack(delivery, msgSpecificLogEntry)
	}
}

func (worker *worker) extractOpenTracingSpan(delivery amqp.Delivery, actionName string) (opentracing.Span, context.Context) {

	var spanOptions []opentracing.StartSpanOption

	spCtx, err := amqptracer.Extract(delivery.Headers)

	if err != nil {
		worker.log().WithError(err).Debug("could not extract SpanContext from headers")
	} else {
		spanOptions = append(spanOptions, opentracing.FollowsFrom(spCtx))
	}
	return opentracing.StartSpanFromContext(context.Background(), actionName, spanOptions...)

}

func (worker *worker) runGlobalHandler(delivery *amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) error {
	if worker.globalRawHandler != nil {
		handlerName := worker.globalRawHandler.Name()
		retryAction := func() error {
			metricsWrapper := func() error {
				txWrapper := func(tx *sql.Tx) error {
					return worker.globalRawHandler(tx, delivery)
				}
				//run the global handler inside a  transactions
				return worker.withTx(txWrapper, msgSpecificLogEntry)
			}
			//run the global handler with metrics
			return metrics.RunHandlerWithMetric(metricsWrapper, handlerName, delivery.Type, worker.log())
		}
		return worker.SafeWithRetries(retryAction, MaxRetryCount)
	}
	return nil
}

func (worker *worker) processMessage(delivery amqp.Delivery, isRPCreply bool) {
	span, ctx := worker.extractOpenTracingSpan(delivery, "ProcessMessage")
	worker.span = span
	defer worker.span.Finish()

	msgSpecificLogEntry := worker.log().WithFields(GetDeliveryLogEntries(delivery))

	//catch all error handling so goroutine will not crash
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				worker.span.LogFields(slog.Error(err))
				msgSpecificLogEntry = msgSpecificLogEntry.WithError(err)
			} else {
				msgSpecificLogEntry = msgSpecificLogEntry.WithField("panic", r)
			}
			worker.span.LogFields(slog.String("panic", "failed to process message"))
			msgSpecificLogEntry.Error("failed to process message")
			_ = worker.reject(false, delivery, msgSpecificLogEntry)
		}
	}()

	msgSpecificLogEntry.Info("GOT MSG")

	// handle a message that originated from a deadletter exchange
	if worker.isDead(delivery) && worker.deadletterHandler != nil {
		worker.span.LogFields(slog.Error(errors.New("handling dead-letter delivery")))
		msgSpecificLogEntry.Info("invoking deadletter handler")
		worker.invokeDeadletterHandler(delivery, msgSpecificLogEntry)
		return
	}

	if err := worker.runGlobalHandler(&delivery, msgSpecificLogEntry); err != nil {
		// when the global handler fails terminate executation and reject the message
		_ = worker.reject(false, delivery, msgSpecificLogEntry)
		return
	}

	handlers := worker.resolveHandlers(isRPCreply, delivery, msgSpecificLogEntry)
	if len(handlers) == 0 {
		msgSpecificLogEntry.Warn("Message received but no handlers found")
		worker.span.LogFields(slog.String("grabbit", "no handlers found"))
		// remove the message by acking it and not rejecting it so it will not be routed to a deadletter queue
		_ = worker.ack(delivery, msgSpecificLogEntry)
		return
	}
	msgSpecificLogEntry.WithFields(logrus.Fields{"number_of_handlers": len(handlers)}).Info("found message handlers")

	if delivery.Body == nil {
		msgSpecificLogEntry.Warn("body is missing for message. Cannot invoke handlers.")
		worker.span.LogFields(slog.String("grabbit", "no body found"))
		// if there are handlers registered for this type of message, it's a bug and the message must be rejected.
		_ = worker.reject(false, delivery, msgSpecificLogEntry)
		return
	}
	/*
		extract the bus message only after we are sure there are registered handlers since
		it includes deserializing the amqp payload which we want to avoid if no handlers are found
		(for instance if a reply message arrives but bo handler is registered for that type of message)
	*/
	bm, err := worker.extractBusMessage(delivery, msgSpecificLogEntry)
	if err != nil {
		worker.span.LogFields(slog.Error(err), slog.String("grabbit", "message is poison"))
		// reject poison message
		_ = worker.reject(false, delivery, msgSpecificLogEntry)
		return
	}
	isDuplicate, err := worker.handleDuplicates(bm, delivery, msgSpecificLogEntry)
	if err != nil {
		worker.span.LogFields(slog.Error(err))
		msgSpecificLogEntry.WithError(err).Info("failed getting information about message duplication")
		_ = worker.reject(true, delivery, msgSpecificLogEntry)
		return
	}
	if isDuplicate {
		msgSpecificLogEntry.Warn("message is a duplicate")
		worker.span.LogFields(slog.String("grabbit", "message is a duplicate"))
		return
	}
	err = worker.invokeHandlers(ctx, handlers, bm, &delivery, msgSpecificLogEntry)
	if err == nil {
		_ = worker.ack(delivery, msgSpecificLogEntry)
	} else {
		logErr := logrushandler.New(msgSpecificLogEntry)
		logErr.Handle(err)
		_ = worker.reject(false, delivery, msgSpecificLogEntry)
	}
}

func (worker *worker) withTx(handlerWrapper func(tx *sql.Tx) error, msgSpecificLogEntry logrus.FieldLogger) (actionErr error) {

	var tx *sql.Tx
	defer func() {
		if p := recover(); p != nil {
			pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
			msgSpecificLogEntry.WithField("stack", pncMsg).Error("recovered from panic while invoking handler")
			actionErr = errors.New(pncMsg)
			if tx != nil {
				rbkErr := tx.Rollback()
				if rbkErr != nil {
					msgSpecificLogEntry.WithError(rbkErr).Error("failed rolling back transaction when recovering from handler panic")
				}
			}
			worker.span.LogFields(slog.Error(actionErr))
		}
	}()
	tx, txCreateErr := worker.txProvider.New()
	if txCreateErr != nil {
		msgSpecificLogEntry.WithError(txCreateErr).Error("failed creating new tx")
		worker.span.LogFields(slog.Error(txCreateErr))
		return txCreateErr
	}
	//execute the wrapper that eventually calls the handler
	handlerErr := handlerWrapper(tx)
	if handlerErr != nil {
		rbkErr := tx.Rollback()
		if rbkErr != nil {
			msgSpecificLogEntry.WithError(rbkErr).Error("failed rolling back transaction when recovering from handler error")
			return rbkErr
		}
		return handlerErr
	}
	cmtErr := tx.Commit()
	if cmtErr != nil {
		msgSpecificLogEntry.WithError(cmtErr).Error("failed committing transaction after invoking handlers")
		return cmtErr
	}
	return nil
}

func (worker *worker) createInvocation(ctx context.Context, delivery *amqp.Delivery, tx *sql.Tx, attempt uint, message *BusMessage, handlerName string, msgSpecificLogEntry logrus.FieldLogger) *defaultInvocationContext {
	invocation := &defaultInvocationContext{
		Glogged:     &Glogged{},
		invokingSvc: delivery.ReplyTo,
		bus:         worker.b,
		inboundMsg:  message,
		tx:          tx,
		ctx:         ctx,
		exchange:    delivery.Exchange,
		routingKey:  delivery.RoutingKey,
		deliveryInfo: DeliveryInfo{
			Attempt:       attempt,
			MaxRetryCount: MaxRetryCount,
		},
	}
	invocationLogger := msgSpecificLogEntry.
		WithField("handler_name", handlerName)
	invocation.SetLogger(invocationLogger)
	return invocation
}

func (worker *worker) invokeHandlers(sctx context.Context, handlers []MessageHandler, message *BusMessage, delivery *amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) (err error) {

	//this is the action that will get retried
	// each retry should run a new and separate transaction which should end with a commit or rollback
	retryAction := func(attempt uint) (actionErr error) {
		attemptSpan, sctx := opentracing.StartSpanFromContext(sctx, "InvokeHandler")
		defer attemptSpan.Finish()

		attemptSpan.LogFields(slog.Uint64("attempt", uint64(attempt+1)))

		for _, handler := range handlers {
			//this function accepets the scoped transaction and executes the handler with metrics
			handlerWrapper := func(tx *sql.Tx) error {

				pinedHandler := handler //https://github.com/kyoh86/scopelint
				handlerName := pinedHandler.Name()
				hspan, hsctx := opentracing.StartSpanFromContext(sctx, handlerName)
				invocation := worker.createInvocation(hsctx, delivery, tx, attempt, message, handlerName, msgSpecificLogEntry)
				//execute the handler with metrics
				handlerErr := metrics.RunHandlerWithMetric(func() error {
					return pinedHandler(invocation, message)
				}, handlerName, message.PayloadFQN, worker.log())

				if handlerErr != nil {
					hspan.LogFields(slog.Error(handlerErr))
				}
				hspan.Finish()
				return handlerErr
			}

			err := worker.withTx(worker.withDeduplicator(handlerWrapper, message, msgSpecificLogEntry))
			if err != nil {
				return err
			}
		}

		return nil
	}

	//retry for MaxRetryCount, back off by a jittered strategy
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	return retry.Retry(retryAction,
		strategy.Limit(MaxRetryCount),
		strategy.BackoffWithJitter(
			backoff.BinaryExponential(BaseRetryDuration),
			jitter.Deviation(random, 0.5),
		))
}

func (worker *worker) handleDuplicates(message *BusMessage, delivery amqp.Delivery, msgSpecificLogEntry *logrus.Entry) (bool, error) {
	if worker.delicatePolicy == DeduplicationPolicyNone {
		return false, nil
	}
	duplicate, err := worker.duplicateStore.MessageExists(message.IdempotencyKey)
	if err != nil {
		worker.span.LogFields(slog.String("grabbit", "failed processing duplicate"))
		worker.log().WithError(err).Error("failed checking for existing message")
		return true, err
	}
	if duplicate {
		msgSpecificLogEntry.Error("message is a duplicate")
		err = worker.duplicatePolicyApply(false, delivery, msgSpecificLogEntry)
		if err != nil {
			msgSpecificLogEntry.WithError(err).Error("failed handling duplicate delivery")
			worker.span.LogFields(slog.Error(errors.New("failed handling duplicate delivery")))
		}
		return true, err
	}
	return false, nil
}

func (worker *worker) duplicatePolicyApply(requeue bool, delivery amqp.Delivery, msgSpecificLogEntry logrus.FieldLogger) error {
	switch worker.delicatePolicy {
	case DeduplicationPolicyReject:
		msgSpecificLogEntry.Info("rejecting message on duplicate")
		worker.span.LogFields(slog.String("grabbit", "rejecting duplicate"))
		metrics.ReportDuplicateMessageReject()
		return worker.reject(false, delivery, msgSpecificLogEntry)
	case DeduplicationPolicyAck:
		msgSpecificLogEntry.Info("acknowledging duplicate")
		worker.span.LogFields(slog.String("grabbit", "acknowledging duplicate"))
		metrics.ReportDuplicateMessageAck()
		return worker.ack(delivery, msgSpecificLogEntry)
	default:
		return errors.NewWithDetails("invalid deduplication policy", "policy", worker.b.DeduplicationPolicy)
	}
}

func (worker *worker) withDeduplicator(txWrapper func(tx *sql.Tx) error, message *BusMessage, logger logrus.FieldLogger) (func(tx *sql.Tx) error, logrus.FieldLogger) {
	return func(tx *sql.Tx) error {
		if worker.delicatePolicy == DeduplicationPolicyNone {
			return txWrapper(tx)
		}
		err := worker.duplicateStore.StoreMessageID(tx, message.IdempotencyKey)
		if err != nil {
			return err
		}
		return txWrapper(tx)
	}, logger
}

func (worker *worker) log() logrus.FieldLogger {
	return worker.b.Log().WithField("worker", worker.consumerTag)
}

func (worker *worker) AddRegistration(registration *Registration) {
	worker.handlersLock.Lock()
	defer worker.handlersLock.Unlock()
	worker.registrations = append(worker.registrations, registration)
}
