package tests

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/rhinof/grabbit/gbus"
)

func TestSendCommand(t *testing.T) {
	cmd := Command1{
		Data: "Command1",
	}
	proceed := make(chan bool)
	b := createBusForTest()

	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		_, ok := message.Payload.(Command1)
		if !ok {
			t.Errorf("handler invoced with wrong message type\r\n%v", cmd)
		}
		proceed <- true
		return nil
	}

	err := b.HandleMessage(cmd, handler)
	if err != nil {
		t.Fatalf("Registering handler returned false, expected true")
	}

	err = b.Start()
	if err != nil {
		t.Errorf("could not start bus for test")
	}
	defer b.Shutdown()

	err = b.Send(testSvc1, gbus.NewBusMessage(cmd))
	if err != nil {
		t.Errorf("could not send message")
	}
	<-proceed

}

func TestReply(t *testing.T) {
	cmd := Command1{}
	reply := Reply1{}
	cmdBusMsg := gbus.NewBusMessage(cmd)

	b := createBusForTest()

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		invocation.Reply(gbus.NewBusMessage(reply))
		return nil

	}

	replyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		_, ok := message.Payload.(Reply1)
		if !ok {
			t.Errorf("message handler for reply message invoced with wrong message type\r\n%v", message)
		}

		//if message.CorrelationID != cmdBusMsg.ID {
		//	t.Errorf("message handler inoced with message containing a wrong correlation id, expected %v as correlation id but received %v", cmdBusMsg.ID, message.CorrelationID)
		//}

		proceed <- true
		return nil
	}

	b.HandleMessage(cmd, cmdHandler)
	b.HandleMessage(reply, replyHandler)

	b.Start()
	defer b.Shutdown()

	b.Send(testSvc1, cmdBusMsg)
	<-proceed
}

func TestPubSub(t *testing.T) {
	event := Event1{}
	b := createBusForTest()

	proceed := make(chan bool)
	eventHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		proceed <- true
		return nil
	}
	b.HandleEvent("test_exchange", "test_topic", event, eventHandler)

	b.Start()
	defer b.Shutdown()
	err := b.Publish("test_exchange", "test_topic", gbus.NewBusMessage(event))
	if err != nil {
		t.Fatal(err)
	}
	<-proceed

}

func TestHandlerRetry(t *testing.T) {

	c1 := Command1{}
	r1 := Reply1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(r1)

	bus := createBusForTest()

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		invocation.Reply(reply)
		return nil
	}

	attempts := 0
	replyHandler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		if attempts == 0 {
			attempts++
			return fmt.Errorf("expecting retry on errors")
		} else if attempts == 1 {
			attempts++
			panic("expecting retry on panics")
		} else {
			proceed <- true
		}
		return nil
	}

	bus.HandleMessage(c1, cmdHandler)
	bus.HandleMessage(r1, replyHandler)

	bus.Start()
	defer bus.Shutdown()

	bus.Send(testSvc1, cmd)
	<-proceed

}

func TestRPC(t *testing.T) {

	c1 := Command1{}
	cmd := gbus.NewBusMessage(c1)
	reply := gbus.NewBusMessage(Reply1{})

	handler := func(invocation gbus.Invocation, _ *gbus.BusMessage) error {
		log.Println("on the moo !!!!")
		invocation.Reply(reply)
		return nil
	}

	svc1 := createNamedBusForTest(testSvc1)
	svc1.HandleMessage(c1, handler)
	svc1.Start()
	defer svc1.Shutdown()
	svc2 := createNamedBusForTest(testSvc2)
	svc2.Start()
	defer svc2.Shutdown()

	reply, _ = svc2.RPC(testSvc1, cmd, reply, 5*time.Second)

	if reply == nil {
		t.Fail()
	}

}
