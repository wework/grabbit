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

	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"

	"github.com/rhinof/grabbit/gbus/tx"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

//DefaultBus implements the Bus interface
type DefaultBus struct {
	*Safety
	outgoing             *AMQPOutbox
	AmqpConnStr          string
	amqpConn             *amqp.Connection
	AMQPChannel          *amqp.Channel
	serviceQueue         amqp.Queue
	rpcQueue             amqp.Queue
	SvcName              string
	ConnErrors           chan *amqp.Error
	MsgHandlers          map[string][]MessageHandler
	RPCHandlers          map[string]MessageHandler
	msgs                 <-chan amqp.Delivery
	rpcMsgs              <-chan amqp.Delivery
	HandlersLock         *sync.Mutex
	RPCLock              *sync.Mutex
	RegisteredSchemas    map[string]bool
	DelayedSubscriptions [][]string
	PurgeOnStartup       bool
	started              bool
	Glue                 SagaRegister
	TxProvider           tx.Provider
	IsTxnl               bool
	WorkerNum            uint
	Serializer           MessageEncoding
	DLX                  string
	DefaultPolicies      []MessagePolicy
	Confirm              bool
}

var (
	//TODO: Replace constants with configuration
	MAX_RETRY_COUNT uint = 3
	rpcHeaderName        = "x-grabbit-rpc-id"
)

func (b *DefaultBus) createRPCQueue() (amqp.Queue, error) {
	/*
		the RPC queue is a queue per service instance (as opposed to the service queue which
		is shared between service instances to allow for round-robin load balancing) in order to
		support synchronous RPC style calls.amqpit is not durable and is auto-deleted once the service
		instance process terminates
	*/
	uid := xid.New().String()
	qName := b.SvcName + "_rpc_" + uid
	q, e := b.AMQPChannel.QueueDeclare(qName,
		false, /*durable*/
		true,  /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		nil /*args*/)
	b.rpcQueue = q
	return q, e
}

func (b *DefaultBus) createMessagesChannel(q amqp.Queue, consumerTag string) (<-chan amqp.Delivery, error) {
	msgs, e := b.AMQPChannel.Consume(q.Name, /*queue*/
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

func (b *DefaultBus) createServiceQueue() (amqp.Queue, error) {
	qName := b.SvcName
	var q amqp.Queue

	if b.PurgeOnStartup {
		msgsPurged, purgeError := b.AMQPChannel.QueueDelete(qName, false /*ifUnused*/, false /*ifEmpty*/, false /*noWait*/)
		if purgeError != nil {
			b.log("failed to purge queue: %v.\ndeleted number of messages:%v\nError:%v", b.SvcName, msgsPurged, purgeError)
			return q, purgeError
		}
	}

	args := amqp.Table{}
	if b.DLX != "" {
		args["x-dead-letter-exchange"] = b.DLX
	}
	q, e := b.AMQPChannel.QueueDeclare(qName,
		true,  /*durable*/
		false, /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		args /*args*/)
	if e != nil {
		b.log("failed to declare queue.\nerror:%v", e)
	}
	b.serviceQueue = q
	return q, e
}

func (b *DefaultBus) bindServiceQueue() {
	for _, subscription := range b.DelayedSubscriptions {
		topic := subscription[0]
		exchange := subscription[1]
		e := b.AMQPChannel.ExchangeDeclare(exchange, /*name*/
			"topic", /*kind*/
			true,    /*durable*/
			false,   /*autoDelete*/
			false,   /*internal*/
			false,   /*noWait*/
			nil /*args amqp.Table*/)
		if e != nil {
			b.log("failed to declare exchange %v\n%v", exchange, e)
		} else {
			e = b.bindQueue(topic, exchange)
			if e != nil {
				b.log("failed to bind to the following\n topic:%v\n exchange:%v\n%v", topic, exchange, e)
			}
		}
	}
}

func (b *DefaultBus) createAMQPChannel(conn *amqp.Connection) error {
	channel, e := conn.Channel()
	if e != nil {
		return e
	}
	b.AMQPChannel = channel
	return nil
}

//Start implements GBus.Start()
func (b *DefaultBus) Start() error {
	//create connection
	conn, e := b.connect(int(MAX_RETRY_COUNT))
	if e != nil {
		return e
	}
	conn.NotifyClose(b.ConnErrors)
	b.amqpConn = conn
	//create channel
	e = b.createAMQPChannel(conn)
	if e != nil {
		return e
	}
	//create the outbox that sends the messages to the amqp transport and handles publisher confirms
	if b.outgoing, e = NewAMQPOutbox(b.AMQPChannel, b.Confirm); e != nil {
		return e
	}
	//declare queue
	var q amqp.Queue
	if q, e = b.createServiceQueue(); e != nil {
		return e
	}

	//bind queue
	b.bindServiceQueue()
	//consume queue
	b.msgs, e = b.createMessagesChannel(q, b.SvcName)
	if e != nil {
		return e
	}

	//declare rpc queue
	var rpcQ amqp.Queue
	if rpcQ, e = b.createRPCQueue(); e != nil {
		return e
	}

	b.rpcMsgs, e = b.createMessagesChannel(rpcQ, b.SvcName+"_rpc")
	if e != nil {
		return e
	}

	b.started = true
	//start consuming messags from service queue
	for workers := uint(0); workers < b.WorkerNum; workers++ {
		go b.consumeMessages()
	}

	return nil
}

//Shutdown implements GBus.Start()
func (b *DefaultBus) Shutdown() {
	b.outgoing.shutdown()
	b.started = false

	b.amqpConn.Close()
	if b.IsTxnl {
		b.TxProvider.Dispose()
	}
}

//Send implements  GBus.Send(destination string, message interface{})
func (b *DefaultBus) Send(ctx context.Context, toService string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = "cmd"
	return b.sendImpl(ctx, toService, b.SvcName, "", "", message, policies...)
}

//RPC implements  GBus.RPC
func (b *DefaultBus) RPC(ctx context.Context, service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error) {

	if !b.started {
		return nil, errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}

	b.RPCLock.Lock()
	rpcID := xid.New().String()
	request.RPCID = rpcID
	replyChan := make(chan *BusMessage)
	handler := func(invocation Invocation, message *BusMessage) error {
		replyChan <- message
		return nil
	}

	b.RPCHandlers[rpcID] = handler
	//we do not defer this as we do not want b.RPCHandlers to be locked until a reply returns
	b.RPCLock.Unlock()
	request.Semantics = "cmd"
	rpc := rpcPolicy{
		rpcID: rpcID}

	b.sendImpl(ctx, service, b.rpcQueue.Name, "", "", request, rpc)

	//wait for reply or timeout
	select {
	case reply := <-replyChan:

		b.RPCLock.Lock()
		delete(b.RPCHandlers, rpcID)
		b.RPCLock.Unlock()
		return reply, nil
	case <-time.After(timeout):
		b.RPCLock.Lock()
		delete(b.RPCHandlers, rpcID)
		b.RPCLock.Unlock()
		return nil, errors.New("rpc call timed out")
	}
}

//Publish implements GBus.Publish(topic, message)
func (b *DefaultBus) Publish(ctx context.Context, exchange, topic string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = "evt"
	return b.sendImpl(ctx, "", b.SvcName, exchange, topic, message, policies...)
}

//HandleMessage implements GBus.HandleMessage
func (b *DefaultBus) HandleMessage(message Message, handler MessageHandler) error {

	return b.registerHandlerImpl(message, handler)
}

//HandleEvent implements GBus.HandleEvent
func (b *DefaultBus) HandleEvent(exchange, topic string, event Message, handler MessageHandler) error {

	/*
	 TODO: Need to remove the event instance from the signature. currently, it is used to map
	 an incoming message to a given handler according to the message type.
	 This is not needed when handling events as we can resolve handlers that need to be invoked
	 by the subscription topic that the message was published to (we can get that out of the amqp headers)
	 In addition, creating a mapping between messages and handlers according to message type prevents us
	 from easily creating polymorphic events handlers or "catch all" handlers that are needed for dead-lettering scenarios
	*/

	/*
		Since it is most likely that the registration for handling an event will be called
		prior to the call to GBus.Start() we will store the exchange and topic of the delayedSubscriptions
		and bind the queue after the bus has been started

	*/
	if !b.started {
		subscription := make([]string, 0)
		subscription = append(subscription, topic, exchange)
		b.DelayedSubscriptions = append(b.DelayedSubscriptions, subscription)
	} else {
		err := b.bindQueue(topic, exchange)

		if err != nil {
			return err
		}
	}
	return b.registerHandlerImpl(event, handler)
}

//RegisterSaga impements GBus.RegisterSaga
func (b *DefaultBus) RegisterSaga(saga Saga) error {
	if b.Glue == nil {
		return errors.New("must configure bus to work with Sagas")
	}
	return b.Glue.RegisterSaga(saga)

}

func (b *DefaultBus) connect(retryCount int) (*amqp.Connection, error) {

	connected := false
	attempts := uint(0)
	var lastErr error
	for !connected && attempts < MAX_RETRY_COUNT {
		conn, e := amqp.Dial(b.AmqpConnStr)
		if e == nil {
			return conn, e
		}
		lastErr = e
		attempts++
	}
	return nil, lastErr
}

func (b *DefaultBus) invokeHandlers(sctx context.Context, handlers []MessageHandler,
	message *BusMessage,
	delivery *amqp.Delivery,
	tx *sql.Tx) (err error) {

	action := func(attempts uint) (actionErr error) {
		defer func() {
			if p := recover(); p != nil {

				pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
				b.log("recovered from panic while invoking handler.\n%v", pncMsg)
				actionErr = errors.New(pncMsg)
			}
		}()
		for _, handler := range handlers {
			ctx := &defaultInvocationContext{
				invocingSvc: delivery.ReplyTo,
				bus:         b,
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

func (b *DefaultBus) consumeMessages() {
	//TODO:Handle panics due to tx errors so the consumption of messages will continue

	for {
		var isRPCreply bool
		var delivery, serviceDelivery, rpcDelivery amqp.Delivery
		select {
		case serviceDelivery = <-b.msgs:
			delivery = serviceDelivery
			isRPCreply = false
		case rpcDelivery = <-b.rpcMsgs:
			delivery = rpcDelivery
			isRPCreply = true
		}

		//	b.log("consumed message with deliver tag %v\n", delivery.ConsumerTag)
		/*
			as the bus shuts down and amqp connection is killed the messages channel (b.msgs) gets closed
			and delivery is a zero value so in order not to panic down the road we return if bus is shutdown
		*/
		b.processMessage(delivery, isRPCreply)

	}
}

func (b *DefaultBus) processMessage(delivery amqp.Delivery, isRPCreply bool) {
	if !b.started {
		return
	}
	b.log("GOT MSG")
	spCtx, _ := amqptracer.Extract(delivery.Headers)
	sp := opentracing.StartSpan(
		"processMessage",
		opentracing.FollowsFrom(spCtx),
	)
	defer sp.Finish()

	// Update the context with the span for the subsequent reference.
	bm := NewFromAMQPHeaders(delivery.Headers)

	//msgName := delivery.Headers["x-msg-name"].(string)
	msgType := castToSgtring(delivery.Headers["x-msg-type"])
	if bm.PayloadFQN == "" || msgType == "" {
		//TODO: Log poision pill message
		b.log("message received but no headers found...rejecting message")
		delivery.Reject(false /*requeue*/)
		return
	}

	//TODO:Dedup message
	handlers := make([]MessageHandler, 0)
	if isRPCreply {
		rpcID, rpcHeaderFound := delivery.Headers[rpcHeaderName].(string)
		if !rpcHeaderFound {
			b.log("rpc message received but no rpc header found...rejecting message")
			delivery.Reject(false /*requeue*/)
			return
		}
		b.RPCLock.Lock()
		rpcHandler := b.RPCHandlers[rpcID]
		b.RPCLock.Unlock()

		if rpcHandler == nil {
			b.log("rpc message received but no rpc header found...rejecting message")
			delivery.Reject(false /*requeue*/)
			return
		}

		handlers = append(handlers, rpcHandler)

	} else {
		b.HandlersLock.Lock()
		handlers = b.MsgHandlers[bm.PayloadFQN]
		b.HandlersLock.Unlock()
	}
	if len(handlers) == 0 {
		b.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", bm.PayloadFQN, msgType)
		delivery.Reject(false /*requeue*/)
		return
	}
	var decErr error
	bm.Payload, decErr = b.Serializer.Decode(delivery.Body)

	if decErr != nil {
		b.log("failed to decode message. rejected as poison\nError:\n%v\nMessage:\n%v", decErr, delivery)
		delivery.Reject(false /*requeue*/)
		return
	}

	var tx *sql.Tx
	var txErr error
	if b.IsTxnl {
		//TODO:Add retries, and reject message with requeue=true
		tx, txErr = b.TxProvider.New()
		if txErr != nil {
			b.log("failed to create transaction.\n%v", txErr)
			return
		}
	}
	var ackErr, commitErr, rollbackErr, rejectErr error
	invkErr := b.invokeHandlers(opentracing.ContextWithSpan(context.Background(), sp), handlers, bm, &delivery, tx)
	if invkErr == nil {
		ack := func() error { return delivery.Ack(false /*multiple*/) }

		ackErr = b.SafeWithRetries(ack, MAX_RETRY_COUNT)
		if b.IsTxnl && ackErr == nil {
			b.log("bus commiting transaction")
			commitErr = b.SafeWithRetries(tx.Commit, MAX_RETRY_COUNT)
			if commitErr != nil {
				b.log("failed to commit transaction\nerror:%v", commitErr)
			}
		} else if b.IsTxnl && ackErr != nil {
			b.log("bus rolling back transaction", ackErr)
			//TODO:retry on error
			rollbackErr = b.SafeWithRetries(tx.Rollback, MAX_RETRY_COUNT)
			if rollbackErr != nil {
				b.log("failed to rollback transaction\nerror:%v", rollbackErr)
			}
		}
	} else {
		logMsg := `Failed to consume message due to failure of one or more handlers.\n
				Message rejected as poison.
				Message name: %v\n
				Message type: %v\n
				Error:\n%v`
		b.log(logMsg, bm.PayloadFQN, msgType, invkErr)
		if b.IsTxnl {
			rollbackErr = b.SafeWithRetries(tx.Rollback, MAX_RETRY_COUNT)

			if rollbackErr != nil {
				b.log("failed to rollback transaction\nerror:%v", rollbackErr)
			}
		}
		// //add the error to the delivery so if it will be availble in the deadletter
		// delivery.Headers["x-grabbit-last-error"] = invkErr.Error()
		// b.Publish(b.DLX, "", message)
		reject := func() error { return delivery.Reject(false /*requeue*/) }
		rejectErr = b.SafeWithRetries(reject, MAX_RETRY_COUNT)
		if rejectErr != nil {

			b.log("failed to reject message.\nerror:%v", rejectErr)
		}
	}
}

func (b *DefaultBus) log(format string, v ...interface{}) {

	log.Printf(b.SvcName+":"+format, v...)
}
func (b *DefaultBus) handleConnErrors() {

	for !b.started {
		connErr := <-b.ConnErrors
		e := b.Start()
		if e != nil {
			b.ConnErrors <- connErr
		}
	}
}

func (b *DefaultBus) sendImpl(ctx context.Context, toService, replyTo, exchange, topic string, message *BusMessage, policies ...MessagePolicy) (er error) {

	defer func() {
		if err := recover(); err != nil {
			errMsg := fmt.Sprintf("panic recovered panicking err:\n%v\n%v", err, debug.Stack())
			er = errors.New(errMsg)
		}
	}()

	headers := message.GetAMQPHeaders()

	buffer, err := b.Serializer.Encode(message.Payload)
	if err != nil {
		b.log("failed to send message, encoding of message failed with the following error:\n%v\nmessage details:\n%v", err, message)
		return err
	}

	msg := amqp.Publishing{
		Body:            buffer,
		ReplyTo:         replyTo,
		MessageId:       message.ID,
		CorrelationId:   message.CorrelationID,
		ContentEncoding: b.Serializer.EncoderID(),
		Headers:         headers,
	}
	sp := opentracing.SpanFromContext(ctx)
	defer sp.Finish()
	// Inject the span context into the AMQP header.
	if err := amqptracer.Inject(sp, msg.Headers); err != nil {
		return err
	}

	for _, defaultPolicy := range b.DefaultPolicies {
		defaultPolicy.Apply(&msg)
	}

	for _, policy := range policies {
		policy.Apply(&msg)
	}

	key := ""

	if message.Semantics == "cmd" {
		key = toService
	} else {
		key = topic
	}

	publish := func() error {
		return b.outgoing.send(exchange, key, msg)
	}
	err = b.SafeWithRetries(publish, MAX_RETRY_COUNT)

	if err != nil {
		log.Printf("failed publishing message.\n error:%v", err)
		return err
	}
	return err
}

func (b *DefaultBus) registerHandlerImpl(msg Message, handler MessageHandler) error {

	b.HandlersLock.Lock()
	defer b.HandlersLock.Unlock()

	b.Serializer.Register(msg)
	fqn := msg.Name()

	handlers := b.MsgHandlers[fqn]
	if handlers == nil {
		handlers = make([]MessageHandler, 0)
	}
	handlers = append(handlers, handler)
	b.MsgHandlers[fqn] = handlers
	return nil
}

func (b *DefaultBus) bindQueue(topic, exchange string) error {
	return b.AMQPChannel.QueueBind(b.serviceQueue.Name, topic, exchange, false /*noWait*/, nil /*args*/)
}

type rpcPolicy struct {
	rpcID string
}

func (p rpcPolicy) Apply(publishing *amqp.Publishing) {
	publishing.Headers[rpcHeaderName] = p.rpcID
}
