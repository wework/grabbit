# Logging

grabbit supports structured logging via the [logrus](https://github.com/sirupsen/logrus) logging package.
The logger is accessible to message handlers via the past in invocation instance.

```go

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    invocation.Log().WithField("name", "rhinof").Info("handler invoked")
    return nil
  }

```

grabbit will create a default instance of logrus FieldLogger if no such logger is set when the bus is created.
In order to set a custom logger when creating the bus you need to call the Builder.WithLogger method passing it
a logrus.FieldLogger instance.

```go


```
