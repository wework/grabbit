package gbus

import (
	"context"
	"database/sql"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/streadway/amqp"
)

type Semantics string

const (
	CMD Semantics = "cmd"
	EVT Semantics = "evt"
)

//BusConfiguration provides configuration passed to the bus builder
type BusConfiguration struct {
	MaxRetryCount     uint
	BaseRetryDuration int
}

//Bus interface provides the majority of functionality to Send, Reply and Publish messages to the Bus
type Bus interface {
	HandlerRegister
	RegisterDeadletterHandler
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

//RegisterDeadletterHandler provides the ability to handle messages that were rejected as poision and arrive to the deadletter queue
type RegisterDeadletterHandler interface {
	HandleDeadletter(handler func(tx *sql.Tx, poision amqp.Delivery) error)
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
}

//Invocation context for a specific processed message
type Invocation interface {
	Logged
	Reply(ctx context.Context, message *BusMessage) error
	Bus() Messaging
	Tx() *sql.Tx
	Ctx() context.Context
	Routing() (exchange, routingKey string)
	DeliveryInfo() DeliveryInfo
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
	GetDb() *sql.DB
}

//TxOutbox abstracts the transactional outgoing channel type
type TxOutbox interface {
	Save(tx *sql.Tx, exchange, routingKey string, amqpMessage amqp.Publishing) error
	Start(amqpOut *AMQPOutbox) error
	Stop() error
}

type Logged interface {
	SetLogger(entry logrus.FieldLogger)
	Log() logrus.FieldLogger
}
