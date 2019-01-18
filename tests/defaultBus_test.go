package tests

import (
	"testing"

	"github.com/rhinof/grabbit/gbus"
)

func TestSendCommand(t *testing.T) {
	cmd := Command1{}
	proceed := make(chan bool)
	b := createBusForTest()

	handler := func(invocation gbus.Invocation, message *gbus.BusMessage) {

		_, ok := message.Payload.(Command1)
		if !ok {
			t.Errorf("handler invoced with wrong message type\r\n%v", cmd)
		}
		proceed <- true
	}

	err := b.HandleMessage(cmd, handler)
	if err != nil {
		t.Fatalf("Registering handler returned false, expected true")
	}

	b.Start()
	defer b.Shutdown()

	b.Send(testSvc1, gbus.NewBusMessage(cmd))
	<-proceed

}

func TestReply(t *testing.T) {
	cmd := Command1{}
	reply := Reply1{}
	cmdBusMsg := gbus.NewBusMessage(cmd)

	b := createBusForTest()

	proceed := make(chan bool)
	cmdHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) {
		invocation.Reply(gbus.NewBusMessage(reply))

	}

	replyHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) {
		_, ok := message.Payload.(Reply1)
		if !ok {
			t.Errorf("message handler for reply message invoced with wrong message type\r\n%v", message)
		}

		if message.CorrelationID != cmdBusMsg.ID {
			t.Errorf("message handler inoced with message containing a wrong correlation id, expected %v as correlation id but received %v", cmdBusMsg.ID, message.CorrelationID)
		}

		proceed <- true
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
	eventHandler := func(invocation gbus.Invocation, message *gbus.BusMessage) {
		proceed <- true
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
