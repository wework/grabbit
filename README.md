
[![CircleCI](https://circleci.com/gh/wework/grabbit.svg?style=svg)](https://circleci.com/gh/wework/grabbit)
[![Go Report Card](https://goreportcard.com/badge/github.com/wework/grabbit)](https://goreportcard.com/report/github.com/wework/grabbit)
[![Coverage Status](https://coveralls.io/repos/github/wework/grabbit/badge.svg?branch=master)](https://coveralls.io/github/wework/grabbit?branch=master)
![GitHub release](https://img.shields.io/github/release/wework/grabbit.svg)



# grabbit

A lightweight transactional message bus on top of RabbitMQ supporting:

1) Supported [Messaging Styles](https://github.com/wework/grabbit/blob/master/docs/MESSAGING.md) 
    - One Way (Fire and forget)
    - Publish/Subscribe
    - Aync Command/Reply
    - Blocking Command/Reply (RPC)
2) [Transactional](https://github.com/wework/grabbit/blob/master/docs/TX.md) message processing
3) Message Orchestration via the [Saga](https://github.com/wework/grabbit/blob/master/docs/SAGA.md) pattern
4) At least once reliable messaging via [Transaction Outbox](https://github.com/wework/grabbit/blob/master/docs/OUTBOX.md) and [Publisher Confirms](https://github.com/wework/grabbit/blob/master/docs/OUTBOX.md)
5) [Retry and backoffs](https://github.com/wework/grabbit/blob/master/docs/RETRY.md)
6) [Structured logging](https://github.com/wework/grabbit/blob/master/docs/LOGGING.md)
7) Reporting [Metrics](https://github.com/wework/grabbit/blob/master/docs/METRICS.md) via Prometheus
8) Distributed [Tracing](https://github.com/wework/grabbit/blob/master/docs/TRACING.md) via OpenTracing
9) [Extensible serialization](https://github.com/wework/grabbit/blob/master/docs/SERIALIZATION.md) with
default support for gob, protobuf and avro

## Stable release
the v1.x branch contains the latest stable releases of grabbit and one should track that branch to get point and minor release updates.

## Supported transactional resources
1) MySql > 8.0 (InnoDB)

## Basic Usage

- For a complete sample application see the vacation booking [sample app](https://github.com/wework/grabbit/blob/master/examples/vacation_app) in the examples directory

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


handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error{
    cmd, ok := message.Payload.(*SomeCommand)
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
    evt, ok := message.Payload.(*SomeEvent)
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
