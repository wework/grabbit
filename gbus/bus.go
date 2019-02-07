package gbus

import (
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
	"github.com/rhinof/grabbit/gbus/tx"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

type DefaultBus struct {
	AmqpConnStr          string
	amqpConn             *amqp.Connection
	amqpChannel          *amqp.Channel
	amqpQueue            amqp.Queue
	SvcName              string
	ConnErrors           chan *amqp.Error
	MsgHandlers          map[string][]MessageHandler
	msgs                 <-chan amqp.Delivery
	HandlersLock         *sync.Mutex
	RegisteredSchemas    map[string]bool
	DelayedSubscriptions [][]string
	PurgeOnStartup       bool
	started              bool
	Glue                 SagaRegister
	TxProvider           tx.Provider
	IsTxnl               bool
	Serializer           MessageEncoding
}

var (
	//TODO: Replace constants with configuration
	MAX_RETRY_COUNT          uint  = 3
	DELIVERY_MODE_PERSISTENT uint8 = 2
)

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
	channel, e := conn.Channel()
	if e != nil {
		return e
	}
	b.amqpChannel = channel

	//declare queue
	//TODO: Add dead-lettering
	q, e := b.amqpChannel.QueueDeclare(b.SvcName,
		true,  /*durable*/
		false, /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		nil /*args*/)

	if e != nil {
		return e
	}
	if b.PurgeOnStartup {
		msgsPurged, purgeError := b.amqpChannel.QueuePurge(q.Name, false /*noWait*/)
		if purgeError != nil {
			b.log("failed to purge queue: %v.\ndeleted number of messages:%v\nError:%v", q.Name, msgsPurged, e)
			return e
		}
	}
	b.amqpQueue = q

	//bind queue
	for _, subscription := range b.DelayedSubscriptions {
		topic := subscription[0]
		exchange := subscription[1]
		e = b.amqpChannel.ExchangeDeclare(exchange, /*name*/
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

	//consume queue
	msgs, e := b.amqpChannel.Consume(b.amqpQueue.Name, /*queue*/
		b.SvcName, /*consumer*/
		false,     /*autoAck*/
		false,     /*exclusive*/
		false,     /*noLocal*/
		false,     /*noWait*/
		nil /*args* amqp.Table*/)

	if e != nil {
		return e
	}
	b.msgs = msgs
	b.started = true
	//TODO:Implement worker go routines
	go b.consumeMessages()

	return nil
}

//Shutdown implements GBus.Start()
func (b *DefaultBus) Shutdown() {

	b.started = false
	b.amqpConn.Close()
	if b.IsTxnl {
		b.TxProvider.Dispose()
	}
}

//Send implements  GBus.Send(destination string, message interface{})
func (b *DefaultBus) Send(toService string, message BusMessage) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = "cmd"
	return b.sendImpl("cmd", toService, "", "", &message)
}

//Publish implements GBus.Publish(topic, message)
func (b *DefaultBus) Publish(exchange, topic string, event BusMessage) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	event.Semantics = "evt"
	return b.sendImpl("evt", "", exchange, topic, &event)
}

//HandleMessage implements GBus.HandleMessage
func (b *DefaultBus) HandleMessage(message interface{}, handler MessageHandler) error {

	return b.registerHandlerImpl(message, handler)
}

//HandleEvent implements GBus.HandleEvent
func (b *DefaultBus) HandleEvent(exchange, topic string, event interface{}, handler MessageHandler) error {

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

func (b *DefaultBus) invokeHandlers(handlers []MessageHandler,
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
				tx:          tx}

			handler(ctx, message)
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
		delivery := <-b.msgs
		//	b.log("consumed message with deliver tag %v\n", delivery.ConsumerTag)
		/*
			as the bus shuts down and amqp connection is killed the messages channel (b.msgs) gets closed
			and delivery is a zero value so in order not to panic down the road we return if bus is shutdown
		*/
		if !b.started {
			return
		}
		b.log("GOT MSG")
		msgName := delivery.Headers["x-msg-name"].(string)
		msgType := delivery.Headers["x-msg-type"].(string)
		if msgName == "" || msgType == "" {
			//TODO: Log poision pill message
			b.log("message received but no headers found...rejecting message")
			delivery.Reject(false /*requeue*/)
			continue
		}
		//TODO:Dedup message
		b.HandlersLock.Lock()
		handlers := b.MsgHandlers[msgName]
		b.HandlersLock.Unlock()
		if len(handlers) == 0 {
			b.log("Message received but no handlers found\nMessage name:%v\nMessage Type:%v\nRejecting message", msgName, msgType)
			delivery.Reject(false /*requeue*/)
			continue
		}
		tm, decErr := b.Serializer.Decode(delivery.Body)
		// reader := bytes.NewReader(delivery.Body)
		// dec := gob.NewDecoder(reader)
		// var tm BusMessage
		if decErr != nil {
			b.log("failed to decode message. rejected as poison\nError:\n%v\nMessage:\n%v", decErr, delivery)
			delivery.Reject(false /*requeue*/)
			continue
		}

		var tx *sql.Tx
		var txErr error
		if b.IsTxnl {
			//TODO:Add retries, and reject message with requeue=true
			tx, txErr = b.TxProvider.New()
			if txErr != nil {
				b.log("failed to create transaction.\n%v", txErr)
			}
		}
		invkErr := b.invokeHandlers(handlers, tm, &delivery, tx)
		if invkErr == nil {
			//TODO:retry akc if error
			ackErr := delivery.Ack(false /*multiple*/)
			if b.IsTxnl && ackErr == nil {
				b.log("bus commiting transaction")
				tx.Commit()
			} else if b.IsTxnl && ackErr != nil {
				b.log("bus rollingback transaction")
				tx.Rollback()
			}
		} else {
			logMsg := `Failed to consume message due to failure of one or more handlers.\n
				Message rejected as poison.
				Message name: %v\n
				Message type: %v\n
				Error:\n%v`
			b.log(logMsg, msgName, msgType, invkErr)
			if b.IsTxnl {
				tx.Rollback()
			}
			delivery.Reject(false /*requeue*/)
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

func (b *DefaultBus) sendImpl(semantics, toService, exchange, topic string, message *BusMessage) (er error) {

	fqn := GetFqn(message.Payload)
	defer func() {
		if err := recover(); err != nil {
			errMsg := fmt.Sprintf("panic recovered panicking err:\n%v\n%v", err, debug.Stack())
			er = errors.New(errMsg)
		}
	}()

	buffer, err := b.Serializer.Encode(message)
	if err != nil {
		b.log("failed to send message, encoding of message failed with the following error:\n%v\nmessage details:\n%v", err, message)
		return err
	}

	headers := amqp.Table{}
	headers["x-msg-name"] = fqn
	headers["x-msg-type"] = semantics
	//TODO: Add message TTL
	msg := amqp.Publishing{
		Body:         buffer,
		DeliveryMode: DELIVERY_MODE_PERSISTENT,
		ReplyTo:      b.SvcName,
		MessageId:    xid.New().String(),
		Headers:      headers}

	key := ""

	if semantics == "cmd" {
		key = toService
	} else {
		key = topic
	}

	err = b.amqpChannel.Publish(exchange, /*exchange*/
		key,   /*key*/
		false, /*mandatory*/
		false, /*immediate*/
		msg /*msg*/)

	if err != nil {
		return err
	}

	return err
}

func (b *DefaultBus) registerHandlerImpl(msg interface{}, handler MessageHandler) error {

	b.HandlersLock.Lock()
	defer b.HandlersLock.Unlock()

	b.Serializer.Register(msg)
	fqn := GetFqn(msg)

	handlers := b.MsgHandlers[fqn]
	if handlers == nil {
		handlers = make([]MessageHandler, 0)
	}
	handlers = append(handlers, handler)
	b.MsgHandlers[fqn] = handlers
	return nil
}

func (b *DefaultBus) bindQueue(topic, exchange string) error {
	return b.amqpChannel.QueueBind(b.amqpQueue.Name, topic, exchange, false /*noWait*/, nil /*args*/)
}
