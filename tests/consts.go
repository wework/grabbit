package tests

import (
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
	"github.com/wework/grabbit/gbus/policy"
)

var connStr string
var testSvc1 string
var testSvc2 string
var testSvc3 string

func init() {
	connStr = "amqp://rabbitmq:rabbitmq@localhost"
	testSvc1 = "testSvc1"
	testSvc2 = "testSvc2"
	testSvc3 = "testSvc3"
}

func createBusForTest() gbus.Bus {
	return createNamedBusForTest(testSvc1)
}

func createBusWithOptions(svcName string, deadletter string, txnl, pos bool) gbus.Bus {
	busBuilder := builder.
		New().
		Bus(connStr).
		WithPolicies(&policy.Durable{}).
		WithConfirms()

	if txnl {
		busBuilder = busBuilder.Txnl("mysql", "rhinof:rhinof@/rhinof")
	}
	if deadletter != "" {
		busBuilder = busBuilder.WithDeadlettering(deadletter)
	}
	if pos {
		busBuilder = busBuilder.PurgeOnStartUp()
	}

	return busBuilder.Build(svcName)
}

func createNamedBusForTest(svcName string) gbus.Bus {
	return createBusWithOptions(svcName, "dead-grabbit", true, true)
}
