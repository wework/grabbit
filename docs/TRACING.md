# Tracing

grabbit supports reporting standard [OpenTracing](https://opentracing.io/) tracing spans to a compatable OpenTracing backend (such as [Jaeger](https://www.jaegertracing.io/)).

NOTE: In your hosting process you will need to set up a global tracer to collect and forward the traces reported by grabbit. See Jaeger go client for an [example](https://github.com/jaegertracing/jaeger-client-go)

Once the global tracer is set up you will need to make sure that in your message handlers you carry over the passed in context to successive messages sent by the handler.

```go

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    reply := gbus.NewBusMessage(MyReply{})
    cmd := gbus.NewBusMessage(MyCommand{})
    ctx := invocation.Ctx()

    if err := invocation.Send(ctx, "another-service", cmd); err != nil{
        return err
    }
    if err := invocation.Reply(ctx, reply); err != nil{
        return err
    }
    return nil
  }

```