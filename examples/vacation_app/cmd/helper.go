package cmd

import (
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
)

func createBus(serviceName string) gbus.Bus {
	return builder.
		New().
		Bus("amqp://rabbitmq:rabbitmq@localhost").
		WorkerNum(3, 1).
		WithConfirms().
		PurgeOnStartUp().
		Txnl("mysql", "rhinof:rhinof@/rhinof?parseTime=true").
		Build(serviceName)
}
