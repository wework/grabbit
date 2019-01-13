package tests

import (
	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/builder"
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
		WithSagas("").
		Build(svcName)

}
