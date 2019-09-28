# Retry and Backoffs

When the invocation of a grabbit handler fails grabbit will retry the handler and perform a jittered [binary exponential backoff](https://en.wikipedia.org/wiki/Exponential_backoff#Binary_exponential_backoff_algorithm).

You can configure the number of retries and control the backoff time factor by passing in a gbus.BusConfiguration instance to the builder interface.

MaxRetryCount configures the maximum number of retries grabbit will try executing the handler before rejecting the message.

BaseRetryDuration is the base duration in milliseconds inputted into the backoff algorithm.
With a binary exponential backoff algorithm, the time between each retry attempt is calculated as 2^[retry attempt] * BaseRetryDuration + [random jitter (a few nanoseconds) ]

the default MaxRetryCount is 3 and BaseRetryDuration is 10ms
Given the above configuration, grabbit will try retrying according to the following

first retry after ~20ms
second retry after ~40ms
third retry after ~80ms

```go

package main

import (
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
)

bus := builder.
		New().
		Bus("rabbitmq connection string").
		WithLogger(logger).
		WorkerNum(3, 1).
        WithConfirms().
        WithConfiguration(gbus.BusConfiguration{
			MaxRetryCount: 5,
			BaseRetryDuration: 15,
		}).
        Txnl("mysql", "database connection string").
        Build("your service name")

```

