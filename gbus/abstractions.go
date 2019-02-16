package gbus

import (
	"database/sql"
	"time"
)

//Bus interface provides the majority of functionality to Send, Reply and Publish messages to the Bus
type Bus interface {
	HandlerRegister
	BusSwitch
	Messaging
	SagaRegister
}

type Message interface {
	FQN() string
}

//Messaging interface to send and publish messages to the bus
type Messaging interface {
	/*
		Send a command or a command response to a specific service
		one-to-one semantics
	*/
	Send(toService string, command *BusMessage) error

	/*
		Publish and event, one-to-many semantics
	*/
	Publish(exchange, topic string, event *BusMessage) error

	/*
		RPC calls the service passing him the request BusMessage and blocks until a reply is
		recived or timeout experied.

	*/
	RPC(service string, request, reply *BusMessage, timeout time.Duration) (*BusMessage, error)
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
	Shutdown()
}

//HandlerRegister registers message handlers to specific messages and events
type HandlerRegister interface {
	/*
		HandleMessage registers a handler to a specific message type
		Use this methof to register handlers for commands and reply messages
		Use the HandleEvent method to subscribe on events and registr a handler
	*/
	HandleMessage(message Message, handler MessageHandler) error
	/*
		HandleEvent registers a handler for a specific message type published
		to an exchange with a specific topic
	*/
	HandleEvent(exchange, topic string, event Message, handler MessageHandler) error
}

//MessageHandler signature for all command handlers
type MessageHandler func(invocation Invocation, message *BusMessage) error

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

//RequestSagaTimeout is the interface a saga needs to implement to get timeout servicess
type RequestSagaTimeout interface {
	TimeoutDuration() time.Duration
	Timeout(invocation Invocation, message *BusMessage) error
}

//SagaTimeoutMessage is the timeout message for Saga's
type SagaTimeoutMessage struct {
	SagaID string
}

//FQN implements gbus.Message
func (SagaTimeoutMessage) FQN() string {
	return "grabbit.timeout"
}

//SagaRegister registers sagas to the bus
type SagaRegister interface {
	RegisterSaga(saga Saga) error
}

//Builder is the main interface that should be used to create an instance of a Bus
type Builder interface {
	PurgeOnStartUp() Builder
	WithDeadlettering(deadletterExchange string) Builder
	/*
		Txnl sets the bus to be transactional using a persisted saga store
		provider: pg for PostgreSQL
		connStr: connection string in the format of the passed in provider
	*/
	Txnl(provider, connStr string) Builder
	WithSerializer(serializer MessageEncoding) Builder
	/*
		 		WorkerNum sets the number of worker go routines consuming messages from the queue
				The default value if this option is not set is 1
	*/
	WorkerNum(workers uint) Builder
	Build(svcName string) Bus
}

//Invocation context for a specific processed message
type Invocation interface {
	Reply(message *BusMessage)
	Bus() Messaging
	Tx() *sql.Tx
}

//MessageEncoding is the base interface for all message serializers
type MessageEncoding interface {
	EncoderID() string
	Encode(message Message) ([]byte, error)
	Decode(buffer []byte) (Message, error)
	Register(obj Message)
}
