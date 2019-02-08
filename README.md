
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
// register command handler<br/>

```Go
type SomeCommand struct {}

handler := func(invocation gbus.Invocation, message *gbus.BusMessage)
		cmd, ok := message.Payload.(Command1)
		if ok {
			fmt.Printf("handler invoked with  message %v", cmd)
		}
	}

cmd := SomeCommand{}
gb.HandleMessage(cmd, handler)
```

//send the command 
```Go
msg := gbus.NewBusMessage(SomeCommand{})
gb.Send("name of service you are sending the command to", msg)
```


## Testing

1) make sure to first: `docker-compose up -d`
2) then to run the tests: `go test ./...`
