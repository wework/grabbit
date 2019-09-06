package tests

import (
	"time"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
	"github.com/wework/grabbit/gbus/policy"
)

var connStr string
var testSvc1 string
var testSvc2 string
var testSvc3 string
var testSvc4 string

func init() {
	connStr = "amqp://rabbitmq:rabbitmq@localhost"
	testSvc1 = "testSvc1"
	testSvc2 = "testSvc2"
	testSvc3 = "testSvc3"
	testSvc4 = "test-svc4"
}

func createBusWithConfig(svcName string, deadletter string, txnl, pos bool, conf gbus.BusConfiguration) gbus.Bus {
	busBuilder := builder.
		New().
		Bus(connStr).
		WithPolicies(&policy.Durable{}, &policy.TTL{Duration: time.Second * 3600}).
		WorkerNum(3, 1).
		WithConfirms().
		WithConfiguration(conf)

	if txnl {
		busBuilder = busBuilder.Txnl("mysql", "rhinof:rhinof@/rhinof")
	}
	if deadletter != "" {
		busBuilder = busBuilder.WithDeadlettering(deadletter)
	}
	if pos {
		busBuilder = busBuilder.PurgeOnStartUp()
	}
	if conf.Serializer != nil {
		busBuilder = busBuilder.WithSerializer(conf.Serializer)
	}

	return busBuilder.Build(svcName)
}

func createBusForTest() gbus.Bus {
	return createNamedBusForTest(testSvc1)
}

func createNamedBusForTest(svcName string) gbus.Bus {
	return createBusWithConfig(svcName, "dead-grabbit", true, true, gbus.BusConfiguration{MaxRetryCount: 4, BaseRetryDuration: 15})
}
