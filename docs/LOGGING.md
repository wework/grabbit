# Logging

### Logging within a handler

grabbit supports structured logging via the [logrus](https://github.com/sirupsen/logrus) logging package.
The logger is accessible to message handlers via the past in invocation instance.
When logging via the logger exposed on the passed invocation each entry you log will be
annotated with the following contextual data (added as logrus fields to the log entry)
allowing for a better debugging experience.

- _service: the service name
<<<<<<< HEAD
- correlation_id: the correlation id set for the message
- exchange: the exchange the message was published to
- handler_name: the name of the handler being invoked
- idempotency_key: the idempotency key set for the message
=======
- handler_name: the name of the handler being invoked
>>>>>>> Fix logging and added logging documentation (#176)
- message_id: the id of the processed message
- message_name: the type of the message that is being processed
- routing_key: the routing_key of the message
- saga_id: the id of the saga instance being invoked
<<<<<<< HEAD
- saga_def: the type of the saga that is being invoked
- worker: the worker identifier that is processing the message
=======
- saga_def: the type of the saga that is being invoked 
>>>>>>> Fix logging and added logging documentation (#176)

```go

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    invocation.Log().WithField("name", "rhinof").Info("performing some business logic")
    return nil
  }

```
### Logging contextual data when a handler errors

In cases a message handler errors it is common to log custom contextual data allowing 
service developers to diagnose the root cause of the error.

```go
package my_handlers

import (
   "gitub.com/wework/grabbit/gbus"
)

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    invocation.Log().WithField("name", "rhinof").Info("performing some business logic")
    PlaceOrderCommand := message.Payload.(*PlaceOrderCommand)
    e := placeOrder(PlaceOrderCommand.CustomerID, PlaceOrderCommand.LineItems)
    if e != nil{
      invocation.Log().
        WithField("customer_id", PlaceOrderCommand.CustomerID).
        Error("failed placing order for customer")
      return e
    }
    return nil
  }
```
grabbit makes it easier handling these cases and reduce the repetitive task of logging
<<<<<<< HEAD
these custom contextual attributes in cases of errors by integrating the [emperror errors package](https://github.com/emperror/errors).
=======
these custom contextual attributes in cases of errors by integrating the [emperror errors package] (https://github.com/emperror/errors).
>>>>>>> Fix logging and added logging documentation (#176)
emperror drop-in replacement for the default errors package allows developers to add the needed contextual data on the error instance and have graabit log the error with all contextual attribute.

```go
package my_handlers

import (
   "emperror.dev/errors"
   "gitub.com/wework/grabbit/gbus"
)

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    invocation.Log().WithField("name", "rhinof").Info("performing some business logic")
    PlaceOrderCommand := message.Payload.(*PlaceOrderCommand)
    return placeOrder(PlaceOrderCommand.CustomerID, PlaceOrderCommand.LineItems)
  }

func placeOrder(customerID string, lineItems LineItems[]) error{

  if(someErrorCondition()){
    return errors.WithDetails("failed placing order for customer", "customer_id", customerID)
  }

  return nil
}

```



### Setting a custom logger instance

grabbit will create a default instance of logrus FieldLogger if no such logger is set when the bus is created.
To set a custom logger when creating the bus you need to call the Builder.WithLogger method passing it
a logrus.FieldLogger instance.

```go
logger := logrus.New().WithField("my_custom_key", "my_custom_value")
bus := builder.
		New().
		Bus("rabbitmq connection string").
		WithLogger(logger).
		WorkerNum(3, 1).
		WithConfirms().
    Txnl("mysql", "").
    Build("your service name")

```
