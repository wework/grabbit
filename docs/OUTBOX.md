# Reliable Messaging


grabbit ensures the reliable delivery of messages by implementing the [transactional outbox pattern](https://microservices.io/patterns/data/application-events.html).
By storing all outbound messages in a transactional resource under the same transactional context
that execute event handlers and business logic the service using grabbit achieves an "at least once" delivery guarantee
while maintaining local service transactivity ensuring that the state of the service always is in sync with the discharge of messages (commands, replies and events).

In order to use the outbox capabilities, grabbit needs to be configured as a transactional bus.
Currently, only MySQL 8 and above is supported

In addition, grabbit provide two more options that enhance the reliability of the message delivery
  1. grabbit can be configured to listen to [publisher confirms](https://www.rabbitmq.com/confirms.html) issued from the broker and retry a message delivery in case a negative acknowledgment is returned from the amqp broker.
  2. messages can be delivered as [durable messages](https://www.rabbitmq.com/persistence-conf.html) to the broker

Combining these three options provide for a reliable way of producing and sending messages to downstream consumers.
The following is an example of how to configure grabbit to make use of all of the above

```go

  import (
  	"github.com/rhinof/wework/grabbit/gbus"
  	"github.com/rhinof/wework/grabbit/gbus/builder"
  	"github.com/rhinof/wework/grabbit/gbus/policy"
  )


  func main(){


  gb := builder.
    New().
    Bus("connection string to rabbitmq").
    WithPolicies(&policy.Durable{}).
    WithConfirms().
    Txnl("mysql", "connection string to mysql").
    Build("your_service_name")

  someEvent := gbus.NewBusMessage(SomeEvent{})
  //this event will be sent via the transactional outbox
  gb.Publish(context.Background(), "some_exchange", "some.topic" someEvent)
}

```
