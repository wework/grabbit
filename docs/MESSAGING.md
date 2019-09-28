# Messaging

grabbit is all about asynchronous messaging and supports different kinds of interaction patterns.
essentially to work with grabbit you will be registering on a grabbit bus instance handlers for specific types of messages or topics, allowing you to perform your business logic once a message is consumed by grabbit.
Once invoked handlers can reply to incoming messages send or publish messages to other services via the bus.

At its core grabbit distinguishes between messages that target a specific service (commands) and
messages that may target many services (events).

See [README.md](https://github.com/wework/grabbit/blob/master/README.md) or have a look at grabbit's [test suite](https://github.com/wework/grabbit/blob/master/tests/bus_test.go) to learn basic usage

### One Way

All messages in grabbit are essentially one-way messages sent to a RabbitMQ queue or exchange.
More sophisticated complex interactions are all built on one way messaging.
see the [following test](https://github.com/wework/grabbit/blob/master/tests/bus_test.go#L29) as an example.

### Async Command/Reply

When a handlers replies to a command grabbit automatically adds the id of the inbound message to the outbound reply so services can correlate outbound messages with incoming ones.
see the [following test](https://github.com/wework/grabbit/blob/master/tests/bus_test.go#L91) as an example.

### Publish/Subscriber

grabbit allows publishing events to RabbitMQ exchange and topic hierarchies and handlers to subscribe to those events.
Events do not get correlated and can not be replied to.

see the [following test](https://github.com/wework/grabbit/blob/master/tests/bus_test.go#L112) as an example.

### Blocking Command/Reply (RPC)

it is sometimes beneficial to simulate blocking semantics over an async command/reply message exchange.
In particular, it might come in handly when front end web applications need to call backend queued services.
grabbit allows this scenario by providing the RPC API over the bus interface.

see the [following test](https://github.com/wework/grabbit/blob/master/tests/bus_test.go#L215) as an example.

