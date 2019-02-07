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

## Testing

1) make sure to first: `docker-compose up -d`
2) then to run the tests: `go test ./...`
