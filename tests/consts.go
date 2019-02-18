package tests

import (
	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/builder"
	"github.com/rhinof/grabbit/gbus/policy"
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

func createNamedBusForTest(svcName string) gbus.Bus {
	return builder.
		New().
		Bus(connStr).
		PurgeOnStartUp().
		WithPolicies(&policy.Durable{}).
		WithDeadlettering("grabbit-dead").
		WithConfirms().
		Txnl("pg", "user=rhinof password=rhinof dbname=rhinof sslmode=disable").
		Build(svcName)

}
