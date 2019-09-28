# Serialization

grabbit supports out of the box three serializers to format messages over the wire

- gob (defualt)
- protobuf
- avro (experimental)

To configure a bus to work with a different serializer than the default one you must call the WithSerializer function call on the builder interface passing it an instance of a gbus.Serializer.

The following example configures the bus to work with the protobuf serializer

```go
package main

import (
	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
	"github.com/wework/grabbit/gbus/serialization"
)

logger := logrus.New()
	bus := builder.
		New().
		Bus("rabbitmq connection string").
		WithLogger(logger).
		WithSerializer(serialization.NewProtoSerializer(logger)).
		WorkerNum(3, 1).
		WithConfirms().
        Txnl("mysql", "database connection string").
        Build("your service name")

```

