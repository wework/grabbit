package tests

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/builder"
)

func TestCustomLogger(t *testing.T) {

	svcName := "kong-service"
	testLogger, hook := test.NewNullLogger()
	buslogger := testLogger.WithField("_custom", "true")
	bus := getBaseBusBuilder().
		WithLogger(buslogger).
		Build(svcName)

	bus.Start()
	defer bus.Shutdown()
	bus.Log().Info("testing custom logger")

	for index := 0; index < len(hook.Entries); index++ {
		loggedServiceName := hook.Entries[index].Data["_service"].(string)
		loggedCustomField := hook.Entries[index].Data["_custom"].(string)
		if loggedServiceName != svcName || loggedCustomField != "true" {
			t.Errorf("_service value was %s. _custom was %s", loggedServiceName, loggedCustomField)
		}
	}
}

func getBaseBusBuilder() gbus.Builder {
	return builder.
		New().
		Bus(connStr).
		Txnl("mysql", "rhinof:rhinof@/rhinof")
}
