package gbus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/wework/grabbit/gbus/metrics"

	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"
	slog "github.com/opentracing/opentracing-go/log"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var _ SagaRegister = &DefaultBus{}
var _ Bus = &DefaultBus{}

//DefaultBus implements the Bus interface
type DefaultBus struct {
	*Safety
	*Glogged
	Outbox         TxOutbox
	PrefetchCount  uint
	AmqpConnStr    string
	ingressConn    *amqp.Connection
	egressConn     *amqp.Connection
	workers        []*worker
	ingressChannel *amqp.Channel
	egressChannel  *amqp.Channel
	serviceQueue   amqp.Queue
	rpcQueue       amqp.Queue
	SvcName        string
	amqpErrors     chan *amqp.Error
	amqpBlocks     chan amqp.Blocking
	Registrations  []*Registration
	amqpOutbox     *AMQPOutbox

	RPCHandlers          map[string]MessageHandler
	deadletterHandler    RawMessageHandler
	globalRawHandler     RawMessageHandler
	HandlersLock         *sync.Mutex
	RPCLock              *sync.Mutex
	SenderLock           *sync.Mutex
	ConsumerLock         *sync.Mutex
	RegisteredSchemas    map[string]bool
	DelayedSubscriptions [][]string
	PurgeOnStartup       bool
	started              bool
	Glue                 SagaGlue
	TxProvider           TxProvider

	WorkerNum           uint
	Serializer          Serializer
	DLX                 string
	DeduplicationPolicy DeduplicationPolicy
	Deduplicator        Deduplicator
	DefaultPolicies     []MessagePolicy
	Confirm             bool
	healthChan          chan error
	backpressure        bool
	DbPingTimeout       time.Duration
	amqpConnected       bool
}

var (
	//MaxRetryCount defines the max times a retry can run.
	//Default is 3 but it is configurable
	MaxRetryCount uint = 3
	//BaseRetryDuration defines the basic milliseconds that the retry algorithm uses
	//for a random retry time. Default is 10 but it is configurable.
	BaseRetryDuration = 10 * time.Millisecond
	//RPCHeaderName used to define the header in grabbit for RPC
	RPCHeaderName                  = "x-grabbit-msg-rpc-id"
	ResurrectedHeaderName          = "x-resurrected-from-death"
	FirstDeathRoutingKeyHeaderName = "x-first-death-routing-key"
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
	q, e := b.ingressChannel.QueueDeclare(qName,
		false, /*durable*/
		true,  /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		nil /*args*/)
	return q, e
}

func (b *DefaultBus) createServiceQueue() (amqp.Queue, error) {
	qName := b.SvcName
	var q amqp.Queue

	if b.PurgeOnStartup {
		msgsPurged, purgeError := b.ingressChannel.QueueDelete(qName, false /*ifUnused*/, false /*ifEmpty*/, false /*noWait*/)
		if purgeError != nil {
			b.Log().WithError(purgeError).WithField("deleted_messages", msgsPurged).Error("failed to purge queue")
			return q, purgeError
		}
	}

	args := amqp.Table{}
	if b.DLX != "" {
		args["x-dead-letter-exchange"] = b.DLX
	}
	q, e := b.ingressChannel.QueueDeclare(qName,
		true,  /*durable*/
		false, /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		args /*args*/)
	if e != nil {
		b.Log().WithError(e).Error("failed to declare queue")
	}
	b.serviceQueue = q
	return q, e
}

func (b *DefaultBus) bindServiceQueue() error {

	if b.deadletterHandler != nil && b.DLX != "" {
		err := b.ingressChannel.ExchangeDeclare(b.DLX, /*name*/
			"fanout", /*kind*/
			true,     /*durable*/
			false,    /*autoDelete*/
			false,    /*internal*/
			false,    /*noWait*/
			nil /*args amqp.Table*/)
		if err != nil {
			b.Log().WithError(err).Error("could not declare exchange")
			return err
		}
		err = b.bindQueue("", b.DLX)
		if err != nil {
			b.Log().WithError(err).Error("could not bind exchange")
			return err
		}
	}
	for _, subscription := range b.DelayedSubscriptions {
		topic := subscription[0]
		exchange := subscription[1]
		e := b.ingressChannel.ExchangeDeclare(exchange, /*name*/
			"topic", /*kind*/
			true,    /*durable*/
			false,   /*autoDelete*/
			false,   /*internal*/
			false,   /*noWait*/
			nil /*args amqp.Table*/)
		if e != nil {
			b.Log().WithError(e).WithField("exchange", exchange).Error("failed to declare exchange")
			return e
		}
		e = b.bindQueue(topic, exchange)
		if e != nil {
			b.Log().WithError(e).WithFields(logrus.Fields{"topic": topic, "exchange": exchange}).Error("failed to bind topic to exchange")
			return e
		}

	}
	return nil
}

//Start implements GBus.Start()
func (b *DefaultBus) Start() error {

	var e error
	//create amqo connection and channel
	if b.ingressConn, e = b.connect(MaxRetryCount); e != nil {
		return e
	}
	if b.egressConn, e = b.connect(MaxRetryCount); e != nil {
		return e
	}

	if b.ingressChannel, e = b.ingressConn.Channel(); e != nil {
		return e
	}
	if b.egressChannel, e = b.egressConn.Channel(); e != nil {
		return e
	}

	//register on failure notifications
	b.amqpErrors = make(chan *amqp.Error)
	b.amqpBlocks = make(chan amqp.Blocking)
	b.ingressConn.NotifyClose(b.amqpErrors)
	b.ingressConn.NotifyBlocked(b.amqpBlocks)
	b.egressConn.NotifyClose(b.amqpErrors)
	b.egressConn.NotifyBlocked(b.amqpBlocks)
	b.egressChannel.NotifyClose(b.amqpErrors)

	/*
		start the transactional outbox, make sure calling b.TxOutgoing.Start() is done only after b.Outgoing.init is called
		TODO://the design is crap and needs to be refactored
	*/
	var amqpChan *amqp.Channel
	if amqpChan, e = b.egressConn.Channel(); e != nil {
		b.Log().WithError(e).Error("failed to create amqp channel for transactional outbox")
		return e
	}
	amqpChan.NotifyClose(b.amqpErrors)
	b.amqpOutbox = &AMQPOutbox{
		SvcName: b.SvcName,
	}
	err := b.amqpOutbox.init(amqpChan, b.Confirm, false)
	if err != nil {
		b.Log().WithError(err).Error("failed initializing amqpOutbox")
		return err
	}
	if startErr := b.Outbox.Start(b.amqpOutbox); startErr != nil {
		b.Log().WithError(startErr).Error("failed to start transactional outbox")
		return startErr
	}

	b.Deduplicator.Start(b.Log())

	//declare queue
	var q amqp.Queue
	if q, e = b.createServiceQueue(); e != nil {
		return e
	}
	b.serviceQueue = q

	//bind queue
	bindErr := b.bindServiceQueue()
	if bindErr != nil {
		b.Log().WithError(err).Error("could not bind service to queue")
		return bindErr
	}

	//declare rpc queue

	if b.rpcQueue, e = b.createRPCQueue(); e != nil {
		return e
	}

	b.Log().WithField("number_of_workers", b.WorkerNum).Info("initiating workers")
	workers, createWorkersErr := b.createBusWorkers(b.WorkerNum)
	if createWorkersErr != nil {

		b.Log().WithError(createWorkersErr).Error("error creating channel for worker")

		return createWorkersErr
	}

	if err := b.Glue.Start(); err != nil {
		return err
	}
	b.workers = workers
	b.started = true
	//start monitoring on amqp related errors
	go b.monitorAMQPErrors()
	//start consuming messags from service queue
	b.amqpConnected = true
	return nil
}

func (b *DefaultBus) createBusWorkers(workerNum uint) ([]*worker, error) {
	workers := make([]*worker, 0)
	for i := uint(0); i < workerNum; i++ {
		//create a channel per worker as we can't share channels across go routines
		amqpChan, createChanErr := b.ingressConn.Channel()
		if createChanErr != nil {
			return nil, createChanErr
		}

		qosErr := amqpChan.Qos(int(b.PrefetchCount), 0, false)
		if qosErr != nil {
			b.Log().Printf("failed to set worker qos\n %v", qosErr)
		}

		tag := fmt.Sprintf("%s_worker_%d", b.SvcName, i)

		w := &worker{
			consumerTag:         tag,
			channel:             amqpChan,
			q:                   b.serviceQueue,
			rpcq:                b.rpcQueue,
			svcName:             b.SvcName,
			txProvider:          b.TxProvider,
			rpcLock:             b.RPCLock,
			rpcHandlers:         b.RPCHandlers,
			deadletterHandler:   b.deadletterHandler,
			globalRawHandler:    b.globalRawHandler,
			handlersLock:        &sync.Mutex{},
			registrations:       b.Registrations,
			serializer:          b.Serializer,
			b:                   b,
			amqpErrors:          b.amqpErrors,
			deduplicationPolicy: b.DeduplicationPolicy,
			deduplicator:        b.Deduplicator,
		}

		err := w.Start()
		if err != nil {
			b.Log().WithError(err).Error("failed to start worker")
		}

		workers = append(workers, w)
	}
	return workers, nil
}

//Shutdown implements GBus.Start()
func (b *DefaultBus) Shutdown() (shutdwonErr error) {

	b.Log().Info("Bus shuting down")
	defer func() {
		if p := recover(); p != nil {
			pncMsg := fmt.Sprintf("%v\n%s", p, debug.Stack())
			shutdwonErr = errors.New(pncMsg)
			b.Log().WithError(shutdwonErr).Error("error when shutting down bus")
		}
	}()

	for _, worker := range b.workers {
		err := worker.Stop()
		if err != nil {
			b.Log().WithError(err).Error("could not stop worker")
			return err
		}
	}

	if err := b.Glue.Stop(); err != nil {
		return err
	}
	b.started = false
	err := b.Outbox.Stop()

	if err != nil {
		b.Log().WithError(err).Error("could not shutdown outbox")
		return err
	}
	b.amqpOutbox.Shutdown()
	b.TxProvider.Dispose()

	return nil
}

//NotifyHealth implements Health.NotifyHealth
func (b *DefaultBus) NotifyHealth(health chan error) {
	if health == nil {
		panic("can't pass nil as health channel")
	}
	b.healthChan = health
}

//GetHealth implements Health.GetHealth
func (b *DefaultBus) GetHealth() HealthCard {

	dbConnected := b.TxProvider.Ping(b.DbPingTimeout)

	return HealthCard{
		DbConnected:        dbConnected,
		RabbitBackPressure: b.backpressure,
		RabbitConnected:    b.amqpConnected,
	}
}

func (b *DefaultBus) withTx(action func(tx *sql.Tx) error, ambientTx *sql.Tx) error {
	var shouldCommitTx bool
	var activeTx *sql.Tx
	//create a new transaction only if there is no active one already passed in
	if ambientTx == nil {

		/*
			if the passed in ambient transaction is not nil it means that some caller has created the transaction
			and knows when should this transaction be committed or rolledback.
			In these cases we only invoke the passed in action with the passed in transaction
			and do not commit/rollback the transaction.action
			If no ambient transaction is passed in then we create a new transaction and commit or rollback after
			invoking the passed in action
		*/
		shouldCommitTx = true

		newTx, newTxErr := b.TxProvider.New()
		if newTxErr != nil {
			b.Log().WithError(newTxErr).Error("failed to create transaction when sending a transactional message")
			return newTxErr
		}
		activeTx = newTx

	} else {
		activeTx = ambientTx
	}
	retryAction := func() error {
		return action(activeTx)
	}
	actionErr := b.SafeWithRetries(retryAction, MaxRetryCount)

	if shouldCommitTx {
		if actionErr != nil {
			err := activeTx.Rollback()
			if err != nil {
				b.Log().WithError(err).Error("could not rollback transaction")
			}
		} else {
			commitErr := activeTx.Commit()
			if commitErr != nil {
				b.Log().WithError(commitErr).Error("could not commit transaction")
				return commitErr
			}
		}
	}
	return actionErr
}

//Send implements  GBus.Send(destination string, message interface{})
func (b *DefaultBus) Send(ctx context.Context, toService string, message *BusMessage, policies ...MessagePolicy) error {
	return b.sendWithTx(ctx, nil, toService, message, policies...)
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
	request.Semantics = CMD
	rpc := rpcPolicy{
		rpcID: rpcID}

	b.Serializer.Register(reply.Payload)

	sendRPC := func(tx *sql.Tx) error {
		return b.sendImpl(ctx, tx, service, b.rpcQueue.Name, "", "", request, rpc)
	}

	err := b.withTx(sendRPC, nil)
	if err != nil {
		b.Log().WithError(err).Error("could not send message")
		return nil, err
	}

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

func (b *DefaultBus) publishWithTx(ctx context.Context, ambientTx *sql.Tx, exchange, topic string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = EVT
	publish := func(tx *sql.Tx) error {
		return b.sendImpl(ctx, tx, "", b.SvcName, exchange, topic, message, policies...)
	}
	return b.withTx(publish, ambientTx)
}

func (b *DefaultBus) sendWithTx(ctx context.Context, ambientTx *sql.Tx, toService string, message *BusMessage, policies ...MessagePolicy) error {
	if !b.started {
		return errors.New("bus not strated or already shutdown, make sure you call bus.Start() before sending messages")
	}
	message.Semantics = CMD
	send := func(tx *sql.Tx) error {
		return b.sendImpl(ctx, tx, toService, b.SvcName, "", "", message, policies...)
	}
	return b.withTx(send, ambientTx)
}

func (b *DefaultBus) returnDeadToQueue(ctx context.Context, ambientTx *sql.Tx, publishing *amqp.Publishing) error {
	if !b.started {
		return errors.New("bus not started or already shutdown, make sure you call bus.Start() before sending messages")
	}

	targetQueue, ok := publishing.Headers["x-first-death-queue"].(string)
	if !ok {
		return fmt.Errorf("bad x-first-death-queue field - %v", publishing.Headers["x-first-death-queue"])
	}
	exchange, ok := publishing.Headers["x-first-death-exchange"].(string)
	if !ok {
		return fmt.Errorf("bad x-first-death-exchange field - %v", publishing.Headers["x-first-death-exchange"])
	}
	routingKey, err := extractFirstDeathRoutingKey(publishing.Headers)
	if err != nil {
		return err
	}

	publishing.Headers[FirstDeathRoutingKeyHeaderName] = routingKey // Set the original death routing key to be used later for replaying
	publishing.Headers[ResurrectedHeaderName] = true                // mark message as resurrected
	// publishing.Headers["x-first-death-exchange"] is not deleted and kept as is

	delete(publishing.Headers, "x-death")
	delete(publishing.Headers, "x-first-death-queue")
	delete(publishing.Headers, "x-first-death-reason")

	b.Log().
		WithField("message_id", publishing.MessageId).
		WithField("target_queue", targetQueue).
		WithField("first_death_routing_key", routingKey).
		WithField("first_death_exchange", exchange).
		Info("returning dead message to queue...")

	send := func(tx *sql.Tx) error {
		// Publishing a "resurrected" message is done directly to the target queue using the default exchange
		return b.publish(tx, "", targetQueue, publishing)
	}
	return b.withTx(send, ambientTx)
}

// Extracts the routing key of the first death of the message. "x-death" header contains a list of "deaths" that happened to this message, with
// the most recent death always being first in the list, so fhe first death is the last one. More information: https://www.rabbitmq.com/dlx.html
func extractFirstDeathRoutingKey(headers amqp.Table) (result string, err error) {
	xDeathList, ok := headers["x-death"].([]interface{})
	if !ok {
		return "", fmt.Errorf("failed extracting routing-key from headers, bad 'x-death' field - %v", headers["x-death"])
	}

	xDeath, ok := xDeathList[0].(amqp.Table)
	if !ok {
		return "", fmt.Errorf("failed extracting routing-key from headers, bad 'x-death' field - %v", headers["x-death"])
	}

	routingKeys, ok := xDeath["routing-keys"].([]interface{})
	if !ok {
		return "", fmt.Errorf("failed extracting routing-key from headers, bad 'routing-keys' field - %v", xDeath["routing-keys"])
	}

	routingKey, ok := routingKeys[len(routingKeys)-1].(string)
	if !ok {
		return "", fmt.Errorf("failed extracting routing-key from headers, bad 'routing-keys' field - %v", xDeath["routing-keys"])
	}

	return routingKey, nil
}

//Publish implements GBus.Publish(topic, message)
func (b *DefaultBus) Publish(ctx context.Context, exchange, topic string, message *BusMessage, policies ...MessagePolicy) error {
	return b.publishWithTx(ctx, nil, exchange, topic, message, policies...)
}

//HandleMessage implements GBus.HandleMessage
func (b *DefaultBus) HandleMessage(message Message, handler MessageHandler) error {

	return b.registerHandlerImpl("", b.SvcName, message, handler)
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
	return b.registerHandlerImpl(exchange, topic, event, handler)
}

//HandleDeadletter implements Deadlettering.HandleDeadletter
func (b *DefaultBus) HandleDeadletter(handler RawMessageHandler) {
	b.registerDeadLetterHandler(handler)
}

//SetGlobalRawMessageHandler implements RawMessageHandling.SetGlobalRawMessageHandler
func (b *DefaultBus) SetGlobalRawMessageHandler(handler RawMessageHandler) {
	metrics.AddHandlerMetrics(handler.Name())
	b.globalRawHandler = handler
}

//ReturnDeadToQueue returns a message to its original destination
func (b *DefaultBus) ReturnDeadToQueue(ctx context.Context, publishing *amqp.Publishing) error {
	return b.returnDeadToQueue(ctx, nil, publishing)
}

//RegisterSaga impements GBus.RegisterSaga
func (b *DefaultBus) RegisterSaga(saga Saga, conf ...SagaConfFn) error {
	if b.Glue == nil {
		return errors.New("must configure bus to work with Sagas")
	}
	return b.Glue.RegisterSaga(saga, conf...)

}

func (b *DefaultBus) connect(retryCount uint) (*amqp.Connection, error) {
	var conn *amqp.Connection
	err := b.SafeWithRetries(func() error {
		var err error
		conn, err = amqp.Dial(b.AmqpConnStr)
		return err
	}, retryCount)
	return conn, err

}

func (b *DefaultBus) monitorAMQPErrors() {

	for b.started {
		select {
		case blocked := <-b.amqpBlocks:
			if blocked.Active {
				b.Log().WithField("reason", blocked.Reason).Warn("amqp connection blocked")
			} else {
				b.Log().WithField("reason", blocked.Reason).Info("amqp connection unblocked")
			}
			b.backpressure = blocked.Active
		case amqpErr := <-b.amqpErrors:
			b.amqpConnected = false
			b.Log().WithField("amqp_error", amqpErr).Error("amqp error")
			if b.healthChan != nil {
				b.healthChan <- amqpErr
			}
		}
	}
}

func (b *DefaultBus) publish(tx *sql.Tx, exchange, routingKey string, msg *amqp.Publishing) error {
	publish := func() error {

		b.Log().WithField("message_id", msg.MessageId).Debug("sending message to outbox")
		saveErr := b.Outbox.Save(tx, exchange, routingKey, *msg)
		if saveErr != nil {
			b.Log().WithError(saveErr).Error("failed to save to transactional outbox")
		}
		return saveErr
	}
	//currently only one thread can publish at a time
	//TODO:add a publishing workers

	err := b.SafeWithRetries(publish, MaxRetryCount)

	if err != nil {
		b.Log().WithError(err).Error("failed publishing message")
		return err
	}
	return err
}

func (b *DefaultBus) sendImpl(sctx context.Context, tx *sql.Tx, toService, replyTo, exchange, topic string, message *BusMessage, policies ...MessagePolicy) (er error) {
	b.SenderLock.Lock()
	defer b.SenderLock.Unlock()
	span, _ := opentracing.StartSpanFromContext(sctx, "SendMessage")

	defer func() {
		if err := recover(); err != nil {
			errMsg := fmt.Sprintf("panic recovered panicking err:\n%v\n%s", err, debug.Stack())
			er = errors.New(errMsg)
			span.LogFields(slog.Error(er))
		}
		span.Finish()
	}()

	headers := message.GetAMQPHeaders()
	err := amqptracer.Inject(span, headers)
	if err != nil {
		b.Log().WithError(err).Error("could not inject headers")
	}

	buffer, err := b.Serializer.Encode(message.Payload)
	if err != nil {
		b.Log().WithError(err).WithField("message", message).Error("failed to send message, encoding of message failed")
		return err
	}

	msg := amqp.Publishing{
		Type:          message.PayloadFQN,
		Body:          buffer,
		ReplyTo:       replyTo,
		MessageId:     message.ID,
		CorrelationId: message.CorrelationID,
		ContentType:   b.Serializer.Name(),
		Headers:       headers,
	}
	span.LogFields(message.GetTraceLog()...)

	for _, defaultPolicy := range b.DefaultPolicies {
		defaultPolicy.Apply(&msg)
	}

	for _, policy := range policies {
		policy.Apply(&msg)
	}

	key := ""

	if message.Semantics == CMD {
		key = toService
	} else {
		key = topic
	}

	return b.publish(tx, exchange, key, &msg)
}

func (b *DefaultBus) registerHandlerImpl(exchange, routingKey string, msg Message, handler MessageHandler) error {
	b.HandlersLock.Lock()
	defer b.HandlersLock.Unlock()

	if msg != nil {
		b.Serializer.Register(msg)
	}

	metrics.AddHandlerMetrics(handler.Name())
	registration := NewRegistration(exchange, routingKey, msg, handler)
	b.Registrations = append(b.Registrations, registration)
	for _, worker := range b.workers {
		worker.AddRegistration(registration)
	}
	return nil
}

func (b *DefaultBus) registerDeadLetterHandler(handler RawMessageHandler) {
	metrics.AddHandlerMetrics(handler.Name())
	b.deadletterHandler = handler
}

func (b *DefaultBus) bindQueue(topic, exchange string) error {
	return b.ingressChannel.QueueBind(b.serviceQueue.Name, topic, exchange, false /*noWait*/, nil /*args*/)
}

type rpcPolicy struct {
	rpcID string
}

func (p rpcPolicy) Apply(publishing *amqp.Publishing) {
	publishing.Headers[RPCHeaderName] = p.rpcID
}
