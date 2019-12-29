package rabbitmq

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"emperror.dev/emperror"
	"emperror.dev/errors"
	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/xid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/wework/grabbit/gbus"
)

var _ gbus.Transport = &transport{}

type queueBinding struct {
	topic, exchange string
}

type transport struct {
	*gbus.Glogged
	*gbus.Safety
	connString, svcName, deadLetterExchange  string
	prefetchCount, maxRetryCount, instanceId uint
	purgeOnStartup, withConfirms, started    bool

	ingressConn, egressConn       *amqp.Connection
	ingressChannel, egressChannel *amqp.Channel

	amqpErrors chan *amqp.Error
	amqpBlocks chan amqp.Blocking

	serviceQueue, rpcQueue amqp.Queue

	rawMessages, rawRPCMessages <-chan amqp.Delivery

	// The actual channels used within the gbus/worker
	rpcChannel, messageChannel chan *gbus.BusMessage

	// Used to allow registration of subscription before we start
	delayedSubscriptions []*queueBinding

	// Monitoring fields we should consider a different path
	backpressure          bool
	amqpConnected         bool
	healthChan, errorChan chan error
	inFlightMsgs          map[string]amqp.Delivery
	backPressureChannel   chan bool
}

func (t *transport) RPCChannel() <-chan *gbus.BusMessage {
	return t.rpcChannel
}

func (t *transport) MessageChannel() <-chan *gbus.BusMessage {
	return t.messageChannel
}

func (t *transport) Ack(message *gbus.BusMessage) error {
	delivery, err := t.popDelivery(message.ID)
	if err != nil {
		return err
	}
	return t.ack(delivery)
}

func (t *transport) Reject(message *gbus.BusMessage, requeue bool) error {
	delivery, err := t.popDelivery(message.ID)
	if err != nil {
		return err
	}
	return t.reject(requeue, delivery)
}

func (t *transport) Send(ctx context.Context, toService string, command *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	headers := getAMQPHeaders(command)
	span, _ := opentracing.StartSpanFromContext(ctx, "SendMessage")
	err := amqptracer.Inject(span, headers)
	if err != nil {
		t.Log().WithError(err).Error("failed injecting tracer span")
	}
	msg := amqp.Publishing{
		Type:          command.PayloadFQN,
		Body:          command.RawPayload,
		ReplyTo:       t.svcName,
		MessageId:     command.ID,
		CorrelationId: command.CorrelationID,
		ContentType:   command.ContentType,
		Headers:       headers,
	}

	span.LogFields(command.GetTraceLog()...)

	for _, policy := range policies {
		policy.Apply(&msg)
	}
	return t.egressChannel.Publish("",
		toService, /*key*/
		false,      /*mandatory*/
		false,      /*immediate*/
		msg /*msg*/)
}

func (t *transport) Publish(ctx context.Context, exchange, topic string, event *gbus.BusMessage, policies ...gbus.MessagePolicy) error {
	panic("implement me")
}

func (t *transport) RPC(ctx context.Context, service string, request, reply *gbus.BusMessage, timeout time.Duration) (*gbus.BusMessage, error) {
	panic("implement me")
}

func (t *transport) Start() error {
	var err error
	t.started = true

	// Open connections and channels
	if t.ingressConn, err = t.connect(); err != nil {
		return err
	}
	if t.egressConn, err = t.connect(); err != nil {
		return err
	}
	if t.ingressChannel, err = t.ingressConn.Channel(); err != nil {
		return err
	}

	if t.egressChannel, err = t.egressConn.Channel(); err != nil {
		return err
	}

	// register on failure notifications
	t.ingressConn.NotifyClose(t.amqpErrors)
	t.ingressConn.NotifyBlocked(t.amqpBlocks)
	t.egressConn.NotifyClose(t.amqpErrors)
	t.egressConn.NotifyBlocked(t.amqpBlocks)
	t.egressChannel.NotifyClose(t.amqpErrors)
	t.ingressChannel.NotifyClose(t.amqpErrors)

	// declare queue
	if err = t.createServiceQueue(); err != nil {
		t.Log().WithError(err).Error("failed creating service queue")
		return err
	}

	// bind queue
	if err = t.bindServiceQueue(); err != nil {
		t.Log().WithError(err).Error("failed binding service queue")
		return err
	}

	// declare rpc queue
	if t.rpcQueue, err = t.createRPCQueue(); err != nil {
		t.Log().WithError(err).Error("failed binding RPC queue")
		return err
	}

	// start monitoring on amqp related errors
	go t.monitorAMQPErrors()

	if t.rawMessages, err = t.createMessageChannel(t.serviceQueue, ""); err != nil {
		t.Log().WithError(err).Error("failed creating a message channel")
		return err
	}

	if t.rawRPCMessages, err = t.createMessageChannel(t.serviceQueue, "_rpc"); err != nil {
		t.Log().WithError(err).Error("failed creating a message channel")
		return err
	}

	go t.consumeMessages()
	go t.consumeRPC()
	return nil
}

func (t *transport) Stop() error {
	if !t.started {
		t.Log().Info("stopping a non started transport")
		return nil
	}
	builder := emperror.NewMultiErrorBuilder()

	builder.Add(t.ingressChannel.Cancel(t.consumerTag(""), false))
	builder.Add(t.ingressChannel.Cancel(t.consumerTag("_rpc"), false))
	builder.Add(t.ingressConn.Close())
	builder.Add(t.egressConn.Close())
	return builder.ErrOrNil()
}

func (t *transport) ErrorChan() <-chan error {
	return t.errorChan
}

func (t *transport) BackPressureChannel() <-chan bool {
	return t.backPressureChannel
}

func (t *transport) ListenOnEvent(exchange, topic string) error {

	subscription := &queueBinding{
		topic:    topic,
		exchange: exchange,
	}

	if t.started {
		return t.addSubscription(subscription)
	}
	t.delayedSubscriptions = append(t.delayedSubscriptions, subscription)
	return nil
}

func (t *transport) connect() (*amqp.Connection, error) {
	var conn *amqp.Connection
	err := t.SafeWithRetries(func() error {
		var err error
		conn, err = amqp.Dial(t.connString)
		return err
	}, t.maxRetryCount)
	return conn, err
}

func (t *transport) createServiceQueue() error {
	if t.purgeOnStartup {
		purgedMsgs, err := t.ingressChannel.QueueDelete(t.svcName, false, false, false)
		if err != nil {
			t.Log().WithError(err).WithField("purged_messages", purgedMsgs)
			return err
		}
	}

	args := amqp.Table{}
	if t.deadLetterExchange != "" {
		args["x-dead-letter-exchange"] = t.deadLetterExchange
	}
	var err error
	t.serviceQueue, err = t.ingressChannel.QueueDeclare(t.svcName,
		true,  /*durable*/
		false, /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		args /*args*/)

	if err != nil {
		t.Log().WithError(err).Error("failed to declare queue")
		return err
	}
	return nil
}

func (t *transport) bindServiceQueue() error {
	if t.deadLetterExchange != "" {
		err := t.ingressChannel.ExchangeDeclare(
			t.deadLetterExchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil)
		if err != nil {
			t.Log().WithError(err).
				WithField("dead_letter_exchange", t.deadLetterExchange).
				Error("failed declaring dead letter exchange")
			return err
		}

		err = t.bindQueue(&queueBinding{topic: "", exchange: t.deadLetterExchange})
		if err != nil {
			t.Log().WithError(err).
				WithField("dead_letter_exchange", t.deadLetterExchange).
				Error("failed binding to dead_letter_exchange queue")
			return err
		}
	}

	for _, subscription := range t.delayedSubscriptions {
		err := t.addSubscription(subscription)
		if err != nil {
			t.Log().WithError(err).Error("failed adding subscription")
			return err
		}
	}
	return nil

}

func (t *transport) addSubscription(subscription *queueBinding) error {
	err := t.ingressChannel.ExchangeDeclare(
		subscription.exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Log().WithError(err).
			WithField("exchange", subscription.exchange).
			WithField("topic", subscription.topic).
			Error("failed to declare the proper exchange")
		return err
	}
	err = t.bindQueue(subscription)
	if err != nil {
		t.Log().WithError(err).
			WithField("exchange", subscription.exchange).
			WithField("topic", subscription.topic).
			Error("failed to bind to queue")
		return err
	}
	return nil
}

func (t *transport) bindQueue(s *queueBinding) error {
	return t.ingressChannel.QueueBind(t.serviceQueue.Name, s.topic, s.exchange, false, nil)
}

func (t *transport) createRPCQueue() (amqp.Queue, error) {
	/*
		the RPC queue is a queue per service instance (as opposed to the service queue which
		is shared between service instances to allow for round-robin load balancing) in order to
		support synchronous RPC style calls.amqpit is not durable and is auto-deleted once the service
		instance process terminates
	*/
	uid := xid.New().String()
	qName := t.svcName + "_rpc_" + uid
	q, e := t.ingressChannel.QueueDeclare(qName,
		false, /*durable*/
		true,  /*autoDelete*/
		false, /*exclusive*/
		false, /*noWait*/
		nil /*args*/)
	return q, e

}

func (t *transport) monitorAMQPErrors() {
	// TODO(vlad): refactor this to make sure that this makes
	//  logic maybe implement https://github.com/AppsFlyer/go-sundheit ?
	for t.started {
		select {
		case blocked := <-t.amqpBlocks:
			if blocked.Active {
				t.Log().WithField("reason", blocked.Reason).Warn("amqp connection blocked")
			} else {
				t.Log().WithField("reason", blocked.Reason).Info("amqp connection unblocked")
			}
			t.backpressure = blocked.Active
			t.backPressureChannel <- t.backpressure
		case amqpErr := <-t.amqpErrors:
			t.amqpConnected = false
			t.Log().WithField("amqp_error", amqpErr).Error("amqp error")
			if t.healthChan != nil {
				t.healthChan <- amqpErr
			}
		}
	}
}

func (t *transport) NotifyHealth(health chan error) {
	if health == nil {
		panic("can't pass nil as health channel")
	}
	t.healthChan = health
}

func (t *transport) GetHealth() gbus.HealthCard {
	// TODO(vlad): refactor health messages in grabbit between components
	return gbus.HealthCard{
		RabbitBackPressure: t.backpressure,
		RabbitConnected:    t.amqpConnected,
	}
}

func (t *transport) createMessageChannel(queue amqp.Queue, suffix string) (<-chan amqp.Delivery, error) {
	consumerTag := t.consumerTag(suffix)
	msgs, err := t.ingressChannel.Consume(
		queue.Name,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Log().WithError(err).
			WithField("queue_name", queue.Name).
			WithField("consumer_tag", consumerTag).
			Error("failed to consume from queue")
		return nil, err
	}
	return msgs, nil
}

func (t *transport) consumerTag(suffix string) string {
	return fmt.Sprintf("%s_worker_%d%s", t.svcName, t.instanceId, suffix)
}

func (t *transport) consumeMessages() {
	t.consume(t.rawMessages)
}

func (t *transport) consumeRPC() {
	t.consume(t.rawRPCMessages)
}

func (t *transport) consume(c <-chan amqp.Delivery) {
	for msg := range c {
		bm, err := newFromDelivery(msg)
		if err != nil {
			t.errorChan <- err
			if e := t.reject(false, msg); e != nil {
				t.errorChan <- e
			}
		} else {
			t.inFlightMsgs[bm.ID] = msg
			t.rpcChannel <- bm
		}
	}
}

func (t *transport) reject(requeue bool, delivery amqp.Delivery) error {
	reject := func(attempts uint) error { return delivery.Reject(requeue) }
	l := t.Log().WithField("message_id", delivery.MessageId).
		WithField("requeue", requeue)
	err := retry.Retry(reject,
		strategy.Wait(100*time.Millisecond))
	if err != nil {
		l.WithError(err).
			Error("failed rejecting message")
		return err
	}
	l.Debug("successfully rejected message")
	return nil
}

func (t *transport) ack(delivery amqp.Delivery) error {
	ack := func(attempts uint) error { return delivery.Ack(false /*multiple*/) }
	l := t.Log().WithField("message_id", delivery.MessageId)
	err := retry.Retry(ack,
		strategy.Wait(100*time.Millisecond))
	if err != nil {
		l.WithError(err).
			Error("failed acking message")
		return err
	}
	l.Debug("successfully acked message")
	return nil
}

func (t *transport) popDelivery(id string) (amqp.Delivery, error) {
	delivery, ok := t.inFlightMsgs[id]
	if !ok {
		t.Log().Error("the message is not in our management we should make sure that we are not crossing channels and go routines somewhere")
		return delivery, errors.NewWithDetails("the message we are rejecting is not in our hands", "message_id", message.ID)
	}
	delete(t.inFlightMsgs, id)
	return delivery, nil
}

var _ gbus.NewTransport = NewTransport

// NewTransport creates a new AMQP transport
func NewTransport(svcName, connString, DLX string, prefetchCount, maxRetryCount uint, purgeOnStartup, withConfirms bool, logger logrus.FieldLogger) gbus.Transport {
	t := &transport{
		connString:           connString,
		svcName:              svcName,
		deadLetterExchange:   DLX,
		prefetchCount:        prefetchCount,
		purgeOnStartup:       purgeOnStartup,
		withConfirms:         withConfirms,
		maxRetryCount:        maxRetryCount,
		delayedSubscriptions: make([]*queueBinding, 0),
		instanceId:           uint(rand.Intn(100)),
		errorChan:            make(chan error),
		messageChannel:       make(chan *gbus.BusMessage),
		rpcChannel:           make(chan *gbus.BusMessage),
		inFlightMsgs:         make(map[string]amqp.Delivery),
		amqpBlocks:           make(chan amqp.Blocking),
		amqpErrors:           make(chan *amqp.Error),
		backPressureChannel:  make(chan bool),
	}
	t.SetLogger(logger)

	return t
}
