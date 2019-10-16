package tests

import (
	"github.com/wework/grabbit/gbus"
)

/*** messages that the saga handles */
type InitiateOrderCommand struct {
	OrderID string
}

func (InitiateOrderCommand) SchemaName() string {
	return "InitiateOrderCommand"
}

type InitiateOrderResponse struct {
	OrderID string
}

func (InitiateOrderResponse) SchemaName() string {
	return "InitiateOrderResponse"
}

type AddToOrderCommand struct {
	OrderID string
}

func (AddToOrderCommand) SchemaName() string {
	return "AddToOrderCommand"
}

type AddToOrderResponse struct {
	OrderID string
}

func (AddToOrderResponse) SchemaName() string {
	return "AddToOrderResponse"
}

type CompleteOrderCommand struct {
	OrderID string
}

func (CompleteOrderCommand) SchemaName() string {
	return "CompleteOrderCommand"
}

type CompleteOrderResponse struct {
	OrderID string
}

func (CompleteOrderResponse) SchemaName() string {
	return "CompleteOrderResponse"
}

/*******************************************************/

var _ gbus.CustomeSagaCorrelator = &CustomSagaLocatorSaga{}

type CustomSagaLocatorSaga struct {
	OrderID   string
	Completed bool
}

func (*CustomSagaLocatorSaga) StartedBy() []gbus.Message {
	starters := make([]gbus.Message, 0)
	return append(starters, InitiateOrderCommand{})
}

func (c *CustomSagaLocatorSaga) IsComplete() bool {
	return c.Completed
}

func (c *CustomSagaLocatorSaga) New() gbus.Saga {
	return &CustomSagaLocatorSaga{}
}

func (c *CustomSagaLocatorSaga) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(InitiateOrderCommand{}, c.HandleInitiateOrderCommand)
	register.HandleMessage(AddToOrderCommand{}, c.HandleAddToOrderCommand)
	register.HandleMessage(CompleteOrderCommand{}, c.HandleCompleteOrderCommand)
}

func (c *CustomSagaLocatorSaga) HandleInitiateOrderCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	c.OrderID = message.Payload.(*InitiateOrderCommand).OrderID
	return invocation.Reply(invocation.Ctx(), gbus.NewBusMessage(InitiateOrderResponse{}))
}

func (c *CustomSagaLocatorSaga) HandleAddToOrderCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	return invocation.Reply(invocation.Ctx(), gbus.NewBusMessage(AddToOrderResponse{}))
}

func (c *CustomSagaLocatorSaga) HandleCompleteOrderCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	c.Completed = true
	return invocation.Reply(invocation.Ctx(), gbus.NewBusMessage(CompleteOrderResponse{}))
}

func (c *CustomSagaLocatorSaga) GenCustomCorrelationID() string {
	return c.OrderID
}

func (c *CustomSagaLocatorSaga) CorrelationID() func(message *gbus.BusMessage) string {

	return func(message *gbus.BusMessage) string {
		switch message.Payload.(type) {
		case *AddToOrderCommand:
			return message.Payload.(*AddToOrderCommand).OrderID
		case *CompleteOrderCommand:
			return message.Payload.(*CompleteOrderCommand).OrderID
		default:
			return ""
		}
	}
}
