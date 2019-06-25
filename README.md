
[![CircleCI](https://circleci.com/gh/wework/grabbit.svg?style=svg)](https://circleci.com/gh/wework/grabbit)
[![Go Report Card](https://goreportcard.com/badge/github.com/wework/grabbit)](https://goreportcard.com/report/github.com/wework/grabbit)
[![Coverage Status](https://coveralls.io/repos/github/wework/grabbit/badge.svg?branch=master)](https://coveralls.io/github/wework/grabbit?branch=master)
![GitHub release](https://img.shields.io/github/release/wework/grabbit.svg)



# grabbit

A lightweight transactional message bus on top of RabbitMQ supporting:

1) Supported messaging semantics
    * One Way
    * Duplex
    * Publish/Subscribe
    * Request/Reply (RPC)
2) Long running processes via the [Saga](https://github.com/wework/grabbit/blob/master/docs/SAGA.md) pattern
3) Retry and backoffs
4) Publisher confirms
5) [Reliable messaging](https://github.com/wework/grabbit/blob/master/docs/OUTBOX.md) and local service transactivity via Transaction Outbox pattern
6) Deadlettering

Planned:

1) Deduplication of inbound messages

## Stable release
the v1.x branch contains the latest stable releases of grabbit and one should track that branch to get point and minor release updates. 

## Supported transactional resources
1) MySql > 8.0 (InnoDB)
## Supported serializers
1) gob
2) Avro
3) Protobuf

## Instrumentation

1) Opentracing

## Usage

The following outlines the basic usage of grabbit.
For a complete view of how you would use grabbit including how to write saga's and handle deadlettering refer to grabbit/tests package


```Go
import (
  "github.com/wework/grabbit/gbus"
  "github.com/wework/grabbit/gbus/builder"
)

```
Define a message

```Go
type SomeMessage struct {}

func(SomeMessage) SchemaName() string{
   return "some.unique.namespace.somemessage"
}

```

Creating a transactional GBus instance
```Go
gb := builder.
        New().
    Bus("connection string to RabbitMQ").
    Txnl("mysql", "connection string to mysql").
    WithConfirms().
    Build("name of your service")

```
Register a command handler

```Go


handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error
    cmd, ok := message.Payload.(SomeCommand)
    if ok {
      fmt.Printf("handler invoked with  message %v", cmd)
            return nil
    }

        return fmt.Errorf("failed to handle message")
  }

gb.HandleMessage(SomeCommand{}, handler)
```
Register an event handler

```Go


eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) {
    evt, ok := message.Payload.(SomeEvent)
    if ok {
      fmt.Printf("handler invoked with event %v", evt)
            return nil
    }

        return fmt.Errorf("failed to handle event")
  }

gb.HandleEvent("name of exchange", "name of topic", SomeEvent{}, eventHandler)

```

Start the bus
```Go
gb.Start()
defer gb.Shutdown()
```

Send a command
```Go
gb.Send(context.Background(), "name of service you are sending the command to", gbus.NewBusMessage(SomeCommand{}))
```
Publish an event
```Go
gb.Publish(context.Background(), "name of exchange", "name of topic", gbus.NewBusMessage(SomeEvent{}))
```

RPC style call
```Go


request := gbus.NewBusMessage(SomeRPCRequest{})
reply := gbus.NewBusMessage(SomeRPCReply{})
timeOut := 2 * time.Second

reply, e := gb.RPC(context.Background(), "name of service you are sending the request to", request, reply, timeOut)

if e != nil{
  fmt.Printf("rpc call failed with error %v", e)
} else{
  fmt.Printf("rpc call returned with reply %v", reply)
}

```

## Testing

0) ensure that you have the dependencies installed: `go get -v -t -d ./...`
1) make sure to first: `docker-compose up -V -d`
2) then to run the tests: `go test ./...`
