# Transactional Support

grabbit executes handlers within the scope of an active transaction.
When handlers use the passed in transaction instance to persist their business objects
grabbit guarantees local transactivity by bounding business objects persistence, outgoing command, reply and event messages the handler issues in a single transaction and routing messages to the [Transaction Outbox](https://github.com/wework/grabbit/blob/master/docs/OUTBOX.md).

The following demonstrates how to access the active transaction from within a handler

In this example, the updating of the orders table, publishing of the OrderCanceledEvent event and sending the OrderCanceledReply reply message are all bound to the same transaction.



```go

func SomeHandler(invocation gbus.Invocation, message *gbus.BusMessage) error{
    cancelOrderCommand := message.Payload.(CancelOrderCommand)
    if e := invocation.Tx().Exec("UPDATE orders SET status = 'Canceled' WHERE order_id=?", cancelOrderCommand.orderID); e != nil{
        return e
    }
    orderCanceledEvent := gbus.NewBusMessage(OrderCanceledEvent{
        OrderID: cancelOrderCommand.orderID,
    })
    if e := invocation.Bus().Publish(invocation.Ctx(), "some_exchange", "order.canceled", orderCanceledEvent); e := nil{
        return e
    }

    orderCanceledReply := gbus.NewBusMessage(OrderCanceledReply{})
    return invocation.Reply(invocation.Ctx(), orderCanceledReply)
  }

```