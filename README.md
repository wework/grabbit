
# grabbit

A lightweight message bus on top of RabbitMQ supporting:

1) Async Request/Reply
2) Pub/Sub
3) Saga pattern
4) Retry and backoffs

Planned:

1) Transactional Outbox
2) Deduplication of inbound messages


## Supported transactional resources
1) PostgreSQL
## Supported serializers
1) gob
2) Avro

## Usage

```Go
import (
	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/builder"
)

```


Creating a transactional GBus instance
```Go
gb := builder.
    		New().
		Bus("connection string to RabbitMQ").
		Txnl("pg", "connection string to PostgreSQL").
		Build("name of your service")

```
Register a command handler

```Go
type SomeCommand struct {}

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
type SomeEvent struct{}

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
defer gb.Shutsown()
```

Send a command
```Go
gb.Send("name of service you are sending the command to", gbus.NewBusMessage(SomeCommand{}))
```
Publish an event
```Go
gb.Publish("name of exchange", "name of topic", gbus.NewBusMessage(SomeEvent))
```

## Testing

1) make sure to first: `docker-compose up -d`
2) then to run the tests: `go test ./...`
