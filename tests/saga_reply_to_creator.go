package tests

import "github.com/wework/grabbit/gbus"

type ReplyToInitiatorSaga1 struct {
	DelegateToSvc string
}

func (*ReplyToInitiatorSaga1) StartedBy() []gbus.Message {
	starters := make([]gbus.Message, 0)
	return append(starters, Command1{})
}

func (s *ReplyToInitiatorSaga1) IsComplete() bool {
	return false
}

func (s *ReplyToInitiatorSaga1) New() gbus.Saga {
	return &ReplyToInitiatorSaga1{}
}

func (s *ReplyToInitiatorSaga1) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(Command1{}, s.HandleCommand1)
	register.HandleMessage(Reply2{}, s.HandleRepl2)
}

func (s *ReplyToInitiatorSaga1) HandleCommand1(invocation gbus.Invocation, message *gbus.BusMessage) error {

	cmd2 := gbus.NewBusMessage(Command2{})
	return invocation.Bus().Send(invocation.Ctx(), s.DelegateToSvc, cmd2)
}

func (s *ReplyToInitiatorSaga1) HandleRepl2(invocation gbus.Invocation, message *gbus.BusMessage) error {
	reply := gbus.NewBusMessage(Reply1{})
	sagaInvocation := invocation.(gbus.SagaInvocation)
	return sagaInvocation.ReplyToInitiator(invocation.Ctx(), reply)
}

type ReplyToCreatorSaga2 struct {
}

func (*ReplyToCreatorSaga2) StartedBy() []gbus.Message {
	starters := make([]gbus.Message, 0)
	return append(starters, Command2{})
}

func (s *ReplyToCreatorSaga2) IsComplete() bool {
	return false
}

func (s *ReplyToCreatorSaga2) New() gbus.Saga {
	return &ReplyToCreatorSaga2{}
}

func (s *ReplyToCreatorSaga2) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(Command2{}, s.HandleCommand2)
	register.HandleMessage(Command3{}, s.HandleCommand3)
}

func (s *ReplyToCreatorSaga2) HandleCommand2(invocation gbus.Invocation, message *gbus.BusMessage) error {
	sagaInvocation := invocation.(gbus.SagaInvocation)
	cmd2 := gbus.NewBusMessage(Command3{})
	return invocation.Bus().Send(invocation.Ctx(), sagaInvocation.HostingSvc(), cmd2)
}

func (s *ReplyToCreatorSaga2) HandleCommand3(invocation gbus.Invocation, message *gbus.BusMessage) error {
	reply := gbus.NewBusMessage(Reply2{})
	sagaInvocation := invocation.(gbus.SagaInvocation)
	return sagaInvocation.ReplyToInitiator(invocation.Ctx(), reply)
}
