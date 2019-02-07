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

import (
	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/builder"
)
// Creating a basic GBus instance (not for production)
gbus := builder.
  New().
  Bus("connection string to RabbitMQ").
  Build("name of your service")

// Creating a transactional GBus instance


gbus := builder.
		New().
		Bus("connection string to RabbitMQ").
		Txnl("pg", "connection string to PostgreSQL").
		Build("name of your service")

## Testing

1) make sure to first: `docker-compose up -d`
2) then to run the tests: `go test ./...`
