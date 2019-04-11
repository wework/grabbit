package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"runtime/debug"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
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
			worker.log().WithField("message id", delivery.MessageId).Warn("no proceed")
		}

	}

}

func (worker *worker) extractBusMessage(delivery amqp.Delivery) (*BusMessage, error) {
	bm := NewFromAMQPHeaders(delivery.Headers)
	bm.ID = delivery.MessageId
	bm.CorrelationID = delivery.CorrelationId
	if delivery.Exchange != "" {
		bm.Semantics = "evt"
	} else {
		bm.Semantics = "cmd"
	}
	worker.log().WithField("message name", bm.PayloadFQN).Info("")
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
		rpcID, rpcHeaderFound := delivery.Headers[rpcHeaderName].(string)
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
		worker.log().WithFields(log.Fields{"number of handlers": len(worker.registrations)}).Info("found message handlers")
		for _, registration := range worker.registrations {
			if registration.Matches(delivery.Exchange, delivery.RoutingKey, bm.PayloadFQN) {
				handlers = append(handlers, registration.Handler)
			}
		}
		worker.handlersLock.Unlock()
	}

	return handlers
}

func (worker *worker) Ack(delivery amqp.Delivery) error {
	ack := func() error { return delivery.Ack(false /*multiple*/) }
	return worker.SafeWithRetries(ack, MaxRetryCount)
}

func (worker *worker) Reject(requeue bool, delivery amqp.Delivery) error {
	reject := func() error { return delivery.Reject(requeue /*multiple*/) }
	return worker.SafeWithRetries(reject, MaxRetryCount)
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
		worker.Ack(delivery)
		return
	}
	deadErr := worker.deadletterHandler(tx, delivery)
	if deadErr != nil {
		worker.SafeWithRetries(tx.Rollback, MaxRetryCount)

	} else {
		worker.SafeWithRetries(tx.Commit, MaxRetryCount)
	}
}

func (worker *worker) processMessage(delivery amqp.Delivery, isRPCreply bool) {

	//catch all error handling so goroutine will not crash
	defer func() {
		if r := recover(); r != nil {
			logEntry := worker.log().WithField("Worker", worker.consumerTag)
			if err, ok := r.(error); ok {
				logEntry = logEntry.WithError(err)
			} else {
				logEntry = logEntry.WithField("Panic", r)
			}

			logEntry.Error("failed to process message")
		}
	}()

	worker.log().WithFields(log.Fields{"Worker": worker.consumerTag, "MessageId": delivery.MessageId}).Info("GOT MSG")
	// worker.log("%v GOT MSG - Worker %v - MessageId %v", worker.svcName, worker.consumerTag, delivery.MessageId)
	/*TODO:FIX Opentracing
	spCtx, _ := amqptracer.Extract(delivery.Headers)
	sp := opentracing.StartSpan(
		"processMessage",
		opentracing.FollowsFrom(spCtx),
	)
	if sp != nil {
		defer sp.Finish()
	}
	*/
	// Update the context with the span for the subsequent reference.

	//handle a message that originated from a deadletter exchange
	if worker.isDead(delivery) {
		worker.log().Info("invoking deadletter handler")
		worker.invokeDeadletterHandler(delivery)
		return
	}

	bm, err := worker.extractBusMessage(delivery)
	if err != nil {
		//reject poison message
		worker.Reject(false, delivery)
		return
	}
	//TODO:Dedup message
	handlers := worker.resolveHandlers(isRPCreply, bm, delivery)
	if len(handlers) == 0 {
		worker.log().
			WithFields(
				log.Fields{"Message Name": bm.PayloadFQN,
					"Message Type": bm.Semantics}).
			Warn("Message received but no handlers found")
		// worker.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", bm.PayloadFQN, bm.Semantics)
		//remove the message by acking it and not rejecting it so it will not be routed to a deadletter queue
		worker.Ack(delivery)
		return
	}

	var tx *sql.Tx
	var txErr error
	if worker.isTxnl {
		tx, txErr = worker.txProvider.New()
		if txErr != nil {
			worker.log().WithError(txErr).Error("failed to create transaction")
			//reject the message but requeue it so it gets redelivered until we can create transactions
			worker.Reject(true, delivery)
			return
		}
	}
	var ackErr, commitErr, rollbackErr, rejectErr error
	invkErr := worker.invokeHandlers(context.Background(), handlers, bm, &delivery, tx)

	// if all handlers executed with out errors then commit the transactional if the bus is transactional
	// if the tranaction committed successfully then ack the message.
	// if the bus is not transactional then just ack the message
	if invkErr == nil {

		if worker.isTxnl {
			commitErr = worker.SafeWithRetries(tx.Commit, MaxRetryCount)
			if commitErr == nil {
				worker.log().Info("bus transaction committed successfully ")
				//ack the message
				ackErr = worker.Ack(delivery)
				if ackErr != nil {
					// if this fails then the message will be eventually redeilvered by RabbitMQ
					//sp handlers should be idempotent.
					worker.log().Info("failed to send ack to the broker ")
				}

			} else {
				worker.log().WithError(commitErr).Error("failed committing transaction")
				//if the commit failed we will reject the message
				worker.Reject(false, delivery)
			}
		} else { /*if the bus in not transactional just try acking the message*/
			ackErr = worker.Ack(delivery)
			if ackErr != nil {
				worker.log().Info("failed to send ack to the broker ")
			}
		}
		//else there was an error in the invokation then try rollingback the transaction and reject the message
	} else {
		worker.log().WithError(invkErr).WithFields(log.Fields{"message name": bm.PayloadFQN, "semantics": bm.Semantics}).Error("Failed to consume message due to failure of one or more handlers.\n Message rejected as poison")

		if worker.isTxnl {
			worker.log().Warn("rolling back transaction")
			rollbackErr = worker.SafeWithRetries(tx.Rollback, MaxRetryCount)

			if rollbackErr != nil {
				worker.log().WithError(rollbackErr).Error("failed to rollback transaction")
			}
		}

		rejectErr = worker.Reject(false, delivery)
		if rejectErr != nil {
			worker.log().WithError(rejectErr).Error("failed to reject message")
		}
	}
}

func (worker *worker) invokeHandlers(sctx context.Context, handlers []MessageHandler, message *BusMessage, delivery *amqp.Delivery, tx *sql.Tx) (err error) {

	action := func(attempts uint) (actionErr error) {
		defer func() {
			if p := recover(); p != nil {

				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				worker.log().WithField("stack", pncMsg).Error("recovered from panic while invoking handler")
				actionErr = errors.New(pncMsg)
			}
		}()
		for _, handler := range handlers {
			ctx := &defaultInvocationContext{
				invocingSvc: delivery.ReplyTo,
				bus:         worker.b,
				inboundMsg:  message,
				tx:          tx,
				ctx:         sctx,
				exchange:    delivery.Exchange,
				routingKey:  delivery.RoutingKey}

			e := handler(ctx, message)
			if e != nil {
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
		"Service": worker.svcName})
}
