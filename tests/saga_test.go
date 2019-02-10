package tests

import (
	"log"
	"testing"
	"time"

	"github.com/rhinof/grabbit/gbus"
)

/*
	TestSagaStartUps test that the saga mechanism creats different saga instances
	The test sends three commands and counts the number of reply messages
	and makes sure all reply messages have unique SagaCorrelationID values
*/
func TestSagaStartUps(t *testing.T) {

	proceed := make(chan bool)
	sagaIDs := make([]string, 0)

	cmdReplyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		t.Logf("SAGA - %v", message.SagaID)
		sagaIDs = append(sagaIDs, message.SagaID)
		if len(sagaIDs) == 3 {
			proceed <- true
		}
		return nil
	}

	svc1 := createNamedBusForTest(testSvc1)
	svc1.HandleMessage(Reply1{}, cmdReplyHandler)
	svc1.Start()
	defer svc1.Shutdown()
	svc2 := createNamedBusForTest(testSvc2)
	svc2.RegisterSaga(&SagaA{})
	//this saga should not be created
	svc2.RegisterSaga(&SagaB{})
	svc2.Start()
	defer svc2.Shutdown()

	//send three commands
	cmd1 := gbus.NewBusMessage(Command1{})
	cmd2 := gbus.NewBusMessage(Command1{})
	cmd3 := gbus.NewBusMessage(Command1{})
	svc1.Send(testSvc2, cmd1)
	svc1.Send(testSvc2, cmd2)
	svc1.Send(testSvc2, cmd3)

	<-proceed
	//make sure all sagaIDs are unique
	duplicatesFound := false
	for i := 0; i < len(sagaIDs); i++ {
		sid1 := sagaIDs[i]
		for j := 0; j < len(sagaIDs); j++ {
			if j != i {
				sid2 := sagaIDs[j]
				if sid1 == sid2 {
					t.Logf("duplicate saga ids: %v - %v", sid1, sid2)
					duplicatesFound = true

				}
			}
		}
	}

	if duplicatesFound {
		t.Errorf("none unique saga ids found")
	}
}

func TestSagaToServiceConversation(t *testing.T) {

	proceed := make(chan bool)
	svc1 := createNamedBusForTest(testSvc1)
	svc2 := createNamedBusForTest(testSvc2)

	reply1Handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {

		invocation.Reply(gbus.NewBusMessage(Command2{}))
		return nil
	}

	reply2Handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}

	svc1.HandleMessage(Reply1{}, reply1Handler)
	svc1.HandleMessage(Reply2{}, reply2Handler)
	svc2.RegisterSaga(&SagaA{})

	svc1.Start()
	svc2.Start()
	defer svc1.Shutdown()
	defer svc2.Shutdown()

	cmd1 := gbus.NewBusMessage(Command1{})
	svc1.Send(testSvc2, cmd1)

	<-proceed

}

func TestSagas(t *testing.T) {

	completed := make(chan bool)
	var firstSagaCorrelationID, secondSagaCorrelationID string

	cmdReplyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		firstSagaCorrelationID = message.SagaCorrelationID

		invocation.Bus().Publish("test_exchange", "some.topic.1", gbus.NewBusMessage(Event1{}))
		return nil
	}

	evtReplyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		secondSagaCorrelationID = message.SagaCorrelationID

		completed <- true
		return nil
	}

	svc1 := createNamedBusForTest(testSvc1)

	svc1.HandleMessage(Reply1{}, cmdReplyHandler)
	svc1.HandleMessage(Reply2{}, evtReplyHandler)

	svc2 := createNamedBusForTest(testSvc2)
	s := &SagaA{}
	svc2.RegisterSaga(s)

	svc1.Start()
	defer svc1.Shutdown()
	svc2.Start()
	defer svc2.Shutdown()

	cmd := gbus.NewBusMessage(Command1{})
	svc1.Send(testSvc2, cmd)

	<-completed
	if firstSagaCorrelationID != secondSagaCorrelationID {
		t.Errorf("Messages did not route to the same saga instance")
	}
}

func TestSagaTimeout(t *testing.T) {
	proceed := make(chan bool)
	svc1 := createNamedBusForTest(testSvc1)
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		proceed <- true
		return nil
	}
	svc1.HandleEvent("test_exchange", "some.topic.1", Event1{}, eventHandler)
	svc1.Start()
	defer svc1.Shutdown()

	svc2 := createNamedBusForTest(testSvc2)
	svc2.RegisterSaga(&TimingOutSaga{})
	svc2.Start()
	defer svc2.Shutdown()
	cmd2 := gbus.NewBusMessage(Command2{})
	svc1.Send(testSvc2, cmd2)

	<-proceed
}

/*Test Sagas*/

type SagaA struct {
	Field1 string
	Field2 int
}

func (*SagaA) StartedBy() []interface{} {
	starters := make([]interface{}, 0)
	return append(starters, Command1{})
}

func (s *SagaA) IsComplete() bool {
	return false
}

func (s *SagaA) New() gbus.Saga {
	return &SagaA{}
}

func (s *SagaA) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(Command1{}, s.HandleCommand1)
	register.HandleMessage(Command2{}, s.HandleCommand2)
	register.HandleEvent("test_exchange", "some.topic.1", Event1{}, s.HandleEvent1)
	register.HandleEvent("test_exchange", "some.topic.2", Event2{}, s.HandleEvent1)
}

func (s *SagaA) HandleCommand1(invocation gbus.Invocation, message *gbus.BusMessage) error {

	reply := gbus.NewBusMessage(Reply1{})
	invocation.Reply(reply)
	return nil
}

func (s *SagaA) HandleCommand2(invocation gbus.Invocation, message *gbus.BusMessage) error {
	log.Println("command2 received")
	reply := gbus.NewBusMessage(Reply2{})
	invocation.Reply(reply)
	return nil
}

func (s *SagaA) HandleEvent1(invocation gbus.Invocation, message *gbus.BusMessage) error {
	reply := gbus.NewBusMessage(Reply2{})
	invocation.Reply(reply)
	log.Println("event1 received")
	return nil
}

func (s *SagaA) HandleEvent2(inocation gbus.Invocation, message *gbus.BusMessage) error {
	log.Println("event2 received")
	return nil
}

type SagaB struct {
}

func (*SagaB) StartedBy() []interface{} {
	starters := make([]interface{}, 0)
	return append(starters, Command2{})
}

func (s *SagaB) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(Command1{}, s.Startup)
	register.HandleEvent("test_exchange", "some.topic.1", Event1{}, s.HandleEvent1)
	register.HandleEvent("test_exchange", "some.topic.2", Event2{}, s.HandleEvent1)
}

func (s *SagaB) New() gbus.Saga {
	return &SagaB{}
}

func (s *SagaB) Startup(invocation gbus.Invocation, message *gbus.BusMessage) error {
	log.Println("command1 received")
	reply := gbus.NewBusMessage(Reply1{})
	invocation.Reply(reply)
	return nil
}

func (s *SagaB) HandleEvent1(invocation gbus.Invocation, message *gbus.BusMessage) error {
	reply := gbus.NewBusMessage(Reply2{})
	invocation.Reply(reply)
	log.Println("event1 on SagaB received")
	return nil
}

func (s *SagaB) IsComplete() bool {
	return false
}

func (s *SagaB) RequestTimeout() time.Duration {
	return time.Second * 1
}

func (s *SagaB) Timeout(invocation gbus.Invocation, message *gbus.BusMessage) error {
	invocation.Bus().Publish("test_exchange", "some.topic.1", gbus.NewBusMessage(Event1{}))
	return nil
}

type TimingOutSaga struct {
	TimedOut bool
}

func (*TimingOutSaga) StartedBy() []interface{} {
	starters := make([]interface{}, 0)
	return append(starters, Command2{})
}

func (s *TimingOutSaga) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(Command2{}, s.SagaStartup)
}

func (s *TimingOutSaga) SagaStartup(invocation gbus.Invocation, message *gbus.BusMessage) error {
	return nil
}

func (s *TimingOutSaga) IsComplete() bool {
	return s.TimedOut
}

func (s *TimingOutSaga) New() gbus.Saga {
	return &TimingOutSaga{}
}
func (s *TimingOutSaga) TimeoutDuration() time.Duration {
	return time.Second * 1
}

func (s *TimingOutSaga) Timeout(invocation gbus.Invocation, message *gbus.BusMessage) error {
	s.TimedOut = true
	invocation.Bus().Publish("test_exchange", "some.topic.1", gbus.NewBusMessage(Event1{}))
	return nil
}
