package gbus

import (
	"context"
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

//Semantics reopresents the semantics of a grabbit message
type Semantics string

const (
	//CMD represenst a messge with command semantics in grabbit
	CMD Semantics = "cmd"
	//EVT represenst a messge with event semantics in grabbit
	EVT Semantics = "evt"
	//REPLY represenst a messge with reply semantics in grabbit
	REPLY Semantics = "reply"
)

type DeduplicationPolicy int

const (
	DeduplicationPolicyNone DeduplicationPolicy = iota
	DeduplicationPolicyReject
	DeduplicationPolicyAck
)

//Bus interface provides the majority of functionality to Send, Reply and Publish messages to the Bus
type Bus interface {
	HandlerRegister
	Deadlettering
	RawMessageHandling
	BusSwitch
	Messaging
	SagaRegister
	Health
	Logged
}

//Message a common interface that passes to the serializers to allow decoding and encoding of content
type Message interface {
	SchemaName() string
}

//Messaging interface to send and publish messages to the bus
type Messaging interface {
	/*
		Send a command or a command response to a specific service
		one-to-one semantics
	*/
	Send(ctx context.Context, toService string, command *BusMessage, policies ...MessagePolicy) error

	/*
		Publish and event, one-to-many semantics
	*/
	Publish(ctx context.Context, exchange, topic string, event *BusMessage, policies ...MessagePolicy) error

	/*
		RPC calls the service passing him the request BusMessage and blocks until a reply is
		received or timeout experied.

	*/
	RPC(ctx context.Context, service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error)
}

//MessagePolicy defines a user policy for out going amqp messages User policies can control message ttl, durability etc..
type MessagePolicy interface {
	Apply(publishing *amqp.Publishing)
}

//Health reports om health issues in which the bus needs to be restarted
type Health interface {
	NotifyHealth(health chan error)
	GetHealth() HealthCard
}

//HealthCard that holds the health values of the bus
type HealthCard struct {
	DbConnected        bool
	RabbitConnected    bool
	RabbitBackPressure bool
}

//BusSwitch starts and shutdowns the bus
type BusSwitch interface {
	/*
		Start starts the bus, once the bus is started messages get consiumed from the queue
		and handlers get invoced.
		Register all handlers prior to calling GBus.Start()
	*/
	Start() error
	/*
		Shutdown the bus and close connection to the underlying broker
	*/
	Shutdown() error
}

//HandlerRegister registers message handlers to specific messages and events
type HandlerRegister interface {
	/*
		HandleMessage registers a handler to a specific message type
		Use this method to register handlers for commands and reply messages
		Use the HandleEvent method to subscribe on events and register a handler
	*/
	HandleMessage(message Message, handler MessageHandler) error
	/*
		HandleEvent registers a handler for a specific message type published
		to an exchange with a specific topic
	*/
	HandleEvent(exchange, topic string, event Message, handler MessageHandler) error
}

//Saga is the base interface for all Sagas.
type Saga interface {
	//StartedBy returns the messages that when received should create a new saga instance
	StartedBy() []Message
	/*
		RegisterAllHandlers passes in the HandlerRegister so that the saga can register
		the messages that it handles
	*/
	RegisterAllHandlers(register HandlerRegister)

	//IsComplete retruns if the saga is complete and can be discarded
	IsComplete() bool

	//New is a factory method used by the bus to crerate new instances of a saga
	New() Saga
}

//Deadlettering provides the ability to handle messages that were rejected as poision and arrive to the deadletter queue
type Deadlettering interface {
	/*
		HandleDeadletter is deprecated use RawMessageHandling.SetGlobalRawMessageHandler instead.
		This function will be removed in future grabbit releases
	*/
	HandleDeadletter(handler RawMessageHandler)
	ReturnDeadToQueue(ctx context.Context, publishing *amqp.Publishing) error
}

//RawMessageHandling provides the ability to consume and send raq amqp messages with the transactional guarantees that the bus provides
type RawMessageHandling interface {
	/*
				SetGlobalRawMessageHandler registers a handler that gets called for each amqp.Delivery that is delivered
		        to the service queue.
		        The handler will get called with a scoped transaction that is a different transaction than the ones that
		        regular message handlers are scoped by as we want the RawMessage handler to get executed even if the amqp.Delivery
		        can not be serialized by the bus to one of the registered schemas

		        In case a bus has both a raw message handler and regular ones the bus will first call the raw message handler
		        and afterward will call any registered message handlers.
		        if the global raw handler returns an error the message gets rejected and any additional
		        handlers will not be called.
		        You should not use the global raw message handler to drive business logic as it breaks the local transactivity
		        guarantees grabbit provides and should only be used in specialized cases.
		        If you do decide to use this feature try not shooting yourself in the foot.
	*/
	SetGlobalRawMessageHandler(handler RawMessageHandler)
}

//RequestSagaTimeout is the interface a saga needs to implement to get timeout servicess
type RequestSagaTimeout interface {
	TimeoutDuration() time.Duration
	Timeout(tx *sql.Tx, bus Messaging) error
}

//SagaConfFn is a function to allow configuration of a saga in the context of the gbus
type SagaConfFn func(Saga) Saga

//SagaRegister registers sagas to the bus
type SagaRegister interface {
	RegisterSaga(saga Saga, conf ...SagaConfFn) error
}

//SagaGlue glues together all the parts needed in order to orchistrate saga instances
type SagaGlue interface {
	SagaRegister
	Logged
	Start() error
	Stop() error
}

//Builder is the main interface that should be used to create an instance of a Bus
type Builder interface {
	PurgeOnStartUp() Builder
	WithDeadlettering(deadletterExchange string) Builder
	/*
		Txnl sets the bus to be transactional using a persisted saga store
		provider: mysql for mysql database
		connStr: connection string in the format of the passed in provider
	*/
	Txnl(provider, connStr string) Builder
	//WithSerializer provides the ability to plugin custom serializers
	WithSerializer(serializer Serializer) Builder
	/*
		 		WorkerNum sets the number of worker go routines consuming messages from the queue
				The default value if this option is not set is 1
	*/
	WorkerNum(workers uint, prefetchCount uint) Builder

	/*
	   WithConfirms enables publisher confirms
	*/
	WithConfirms() Builder

	//WithPolicies defines the default policies that are applied for evey outgoing amqp messge
	WithPolicies(policies ...MessagePolicy) Builder

	//ConfigureHealthCheck defines the default timeout in seconds for the db ping check
	ConfigureHealthCheck(timeoutInSeconds time.Duration) Builder

	//RetriesNum defines the number of retries upon error
	WithConfiguration(config BusConfiguration) Builder

	//Build the bus
	Build(svcName string) Bus

	//WithLogger set custom logger instance
	WithLogger(logger logrus.FieldLogger) Builder

	WithDeduplicationPolicy(method DeduplicationPolicy, age time.Duration) Builder
}

//Invocation context for a specific processed message
type Invocation interface {
	Logged
	Reply(ctx context.Context, message *BusMessage) error
	Bus() Messaging
	Tx() *sql.Tx
	Ctx() context.Context
	InvokingSvc() string
	Routing() (exchange, routingKey string)
	DeliveryInfo() DeliveryInfo
}

/*
	SagaInvocation allows saga instances to reply to their creator even when not in the conext of handling
	the message that starts the saga.
	A message handler that is attached to a saga instance can safly cast the passed in invocation to SagaInvocation
	and use the ReplyToInitiator function to send a message to the originating service that sent the message that started the saga
*/
type SagaInvocation interface {
	ReplyToInitiator(ctx context.Context, message *BusMessage) error
	//HostingSvc returns the svc name that is executing the service
	HostingSvc() string

	//SagaID returns the saga id of the currently invoked saga instance
	SagaID() string
}

//Serializer is the base interface for all message serializers
type Serializer interface {
	Name() string
	Encode(message Message) ([]byte, error)
	Decode(buffer []byte, schemaName string) (Message, error)
	Register(obj Message)
}

//TxProvider provides a new Tx from the configured driver to the bus
type TxProvider interface {
	New() (*sql.Tx, error)
	Dispose()
	Ping(timeoutInSeconds time.Duration) bool
}

//TxOutbox abstracts the transactional outgoing channel type
type TxOutbox interface {
	Logged
	Save(tx *sql.Tx, exchange, routingKey string, amqpMessage amqp.Publishing) error
	Start(amqpOut *AMQPOutbox) error
	Stop() error
}

//TimeoutManager abstracts the implementation of determining when a saga should be timed out
type TimeoutManager interface {
	//RegisterTimeout requests the TimeoutManager to register a timeout for a specific saga instance
	RegisterTimeout(tx *sql.Tx, sagaID string, duration time.Duration) error
	//ClearTimeout clears a timeout for a specific saga
	ClearTimeout(tx *sql.Tx, sagaID string) error
	//SetTimeoutFunction accepts the function that the TimeoutManager should invoke once a timeout expires
	SetTimeoutFunction(func(tx *sql.Tx, sagaID string) error)
	//Start starts the timeout manager
	Start() error
	//Stop shuts the timeout manager down
	Stop() error
}

//Logged represents a grabbit component that can be logged
type Logged interface {
	SetLogger(entry logrus.FieldLogger)
	Log() logrus.FieldLogger
}

// Deduplicator abstracts the way to manages the duplications
type Deduplicator interface {
	// StoreMessageID stores the message id in the storage
	StoreMessageID(logger logrus.FieldLogger, tx *sql.Tx, id string) error
	// MessageIDExists checks if message exists in storage
	MessageIDExists(logger logrus.FieldLogger, id string) (bool, error)
	// Deletes all data from the storage of the duplicator
	Purge(logger logrus.FieldLogger) error
	// Starts the background process which cleans the storage of the duplicator
	Start(logger logrus.FieldLogger)
	// Stops the background process of cleaning
	Stop(logger logrus.FieldLogger)
}
