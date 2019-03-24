package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/streadway/amqp"
)

type worker struct {
	*Safety
	channel      *amqp.Channel
	messages     <-chan amqp.Delivery
	rpcMessages  <-chan amqp.Delivery
	q            amqp.Queue
	rpcq         amqp.Queue
	consumerTag  string
	svcName      string
	rpcLock      *sync.Mutex
	handlersLock *sync.Mutex
	msgHandlers  map[string][]MessageHandler
	rpcHandlers  map[string]MessageHandler
	isTxnl       bool
	b            *DefaultBus
	serializer   MessageEncoding
	txProvider   TxProvider
	amqpErrors   chan *amqp.Error
	stop         chan bool
}

func (worker *worker) Start() error {

	worker.log("starting worker")
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
	worker.log("stopping worker")
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
			worker.log("stopping to consume messages")
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
			worker.log("no proceed %v", delivery.MessageId)
		}

	}

}

func (worker *worker) processMessage(delivery amqp.Delivery, isRPCreply bool) {

	worker.log("%v GOT MSG - Worker %v - MessageId %v", worker.svcName, worker.consumerTag, delivery.MessageId)
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
	bm := NewFromAMQPHeaders(delivery.Headers)
	bm.ID = delivery.MessageId
	bm.CorrelationID = delivery.CorrelationId
	if delivery.Exchange != "" {
		bm.Semantics = "evt"
	} else {
		bm.Semantics = "cmd"
	}
	worker.log("Message fqn:%v", bm.PayloadFQN)
	if bm.PayloadFQN == "" || bm.Semantics == "" {
		//TODO: Log poision pill message
		worker.log("message received but no headers found...rejecting message")
		worker.log("received message fqn %v, received message semantics %v", bm.PayloadFQN, bm.Semantics)
		delivery.Reject(false /*requeue*/)
		return
	}

	//TODO:Dedup message
	handlers := make([]MessageHandler, 0)
	if isRPCreply {
		rpcID, rpcHeaderFound := delivery.Headers[rpcHeaderName].(string)
		if !rpcHeaderFound {
			worker.log("rpc message received but no rpc header found...rejecting message")
			delivery.Reject(false /*requeue*/)
			return
		}
		worker.rpcLock.Lock()
		rpcHandler := worker.rpcHandlers[rpcID]
		worker.rpcLock.Unlock()

		if rpcHandler == nil {
			worker.log("rpc message received but no rpc header found...rejecting message")
			delivery.Reject(false /*requeue*/)
			return
		}

		handlers = append(handlers, rpcHandler)

	} else {
		worker.handlersLock.Lock()
		handlers = worker.msgHandlers[bm.PayloadFQN]
		worker.handlersLock.Unlock()
	}
	if len(handlers) == 0 {
		worker.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", bm.PayloadFQN, bm.Semantics)

		delivery.Reject(false /*requeue*/)
		return
	}
	var decErr error
	bm.Payload, decErr = worker.serializer.Decode(delivery.Body)

	if decErr != nil {
		worker.log("failed to decode message. rejected as poison\nError:\n%v\nMessage:\n%v", decErr, delivery)

		delivery.Reject(false /*requeue*/)
		return
	}

	var tx *sql.Tx
	var txErr error
	if worker.isTxnl {
		//TODO:Add retries, and reject message with requeue=true
		tx, txErr = worker.txProvider.New()
		if txErr != nil {
			worker.log("failed to create transaction.\n%v", txErr)
			return
		}
	}
	var ackErr, commitErr, rollbackErr, rejectErr error
	invkErr := worker.invokeHandlers(context.Background(), handlers, bm, &delivery, tx)
	worker.log("invoked")
	if invkErr == nil {
		ack := func() error { return delivery.Ack(false /*multiple*/) }

		ackErr = worker.SafeWithRetries(ack, MAX_RETRY_COUNT)
		if worker.isTxnl && ackErr == nil {

			commitErr = worker.SafeWithRetries(tx.Commit, MAX_RETRY_COUNT)
			if commitErr == nil {
				worker.log("bus transaction comitted successfully ")

			} else {
				worker.log("failed to commit transaction\nerror:%v", commitErr)
			}

		} else if worker.isTxnl && ackErr != nil {
			worker.log("bus rolling back transaction", ackErr)
			//TODO:retry on error
			rollbackErr = worker.SafeWithRetries(tx.Rollback, MAX_RETRY_COUNT)
			if rollbackErr != nil {
				worker.log("failed to rollback transaction\nerror:%v", rollbackErr)
			}
		}
	} else {
		logMsg := "Failed to consume message due to failure of one or more handlers.\n Message rejected as poison. Message name: %v \n  Error: %s"
		worker.log(logMsg, bm.PayloadFQN, bm.Semantics, invkErr)
		if worker.isTxnl {
			worker.log("rolling back transaction")
			rollbackErr = worker.SafeWithRetries(tx.Rollback, MAX_RETRY_COUNT)

			if rollbackErr != nil {
				worker.log("failed to rollback transaction\nerror:%v", rollbackErr)
			}
		}
		// //add the error to the delivery so if it will be available in the deadletter
		// delivery.Headers["x-grabbit-last-error"] = invkErr.Error()
		// b.Publish(b.DLX, "", message)
		reject := func() error { return delivery.Reject(false /*requeue*/) }
		rejectErr = worker.SafeWithRetries(reject, MAX_RETRY_COUNT)
		if rejectErr != nil {

			worker.log("failed to reject message.\nerror:%v", rejectErr)
		}
	}
}

func (worker *worker) invokeHandlers(sctx context.Context, handlers []MessageHandler, message *BusMessage, delivery *amqp.Delivery, tx *sql.Tx) (err error) {

	action := func(attempts uint) (actionErr error) {
		defer func() {
			if p := recover(); p != nil {

				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				worker.log("recovered from panic while invoking handler.\n%v", pncMsg)
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
			}

			e := handler(ctx, message)
			if e != nil {
				return e
			}
		}
		return nil
	}

	//retry for MAX_RETRY_COUNT, back off by a Fibonacci series 50, 50, 100, 150, 250 ms
	return retry.Retry(action,
		strategy.Limit(MAX_RETRY_COUNT),
		strategy.Backoff(backoff.Fibonacci(50*time.Millisecond)))
}

func (worker *worker) log(format string, v ...interface{}) {

	log.Printf(worker.svcName+":"+format, v...)
}
