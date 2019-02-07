
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

import (<br/>
	"github.com/rhinof/grabbit/gbus" <br/>
	"github.com/rhinof/grabbit/gbus/builder" <br/>
)<br/>
  <br/>

type SomeCommand struct {} <br/>

// Creating a transactional GBus instance <br/>
gb := builder.<br/>
    New().<br/>
		Bus("connection string to RabbitMQ").<br/>
		Txnl("pg", "connection string to PostgreSQL").<br/>
		Build("name of your service")<br/>

// register command handler<br/>

  handler := func(invocation gbus.Invocation, message *gbus.BusMessage) {<br/>
		cmd, ok := message.Payload.(Command1)<br/>
		if ok {<br/>
			fmt.Printf("handler invoked with  message %v", cmd)<br/>
		}<br/>
	}<br/>
<br/>
cmd := SomeCommand{}<br/>
gb.HandleMessage(cmd, handler) <br/>

//send the command <br />
msg := gbus.NewBusMessage(SomeCommand{})<br/>
gb.Send("name of service you are sending the command to", msg)<br/>



## Testing

1) make sure to first: `docker-compose up -d`
2) then to run the tests: `go test ./...`
