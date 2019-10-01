package builder

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/saga"
	"github.com/wework/grabbit/gbus/serialization"
	"github.com/wework/grabbit/gbus/tx/mysql"
)

type defaultBuilder struct {
	PrefetchCount    uint
	connStr          string
	purgeOnStartup   bool
	sagaStoreConnStr string
	txnl             bool
	txConnStr        string
	txnlProvider     string
	workerNum        uint
	serializer       gbus.Serializer
	dlx              string
	defaultPolicies  []gbus.MessagePolicy
	confirm          bool
	dbPingTimeout    time.Duration
	usingPingTimeout bool
	logger           logrus.FieldLogger
	busCfg           gbus.BusConfiguration
}

func (builder *defaultBuilder) Build(svcName string) gbus.Bus {

	gb := &gbus.DefaultBus{
		AmqpConnStr:   builder.connStr,
		PrefetchCount: builder.PrefetchCount,

		SvcName:              svcName,
		PurgeOnStartup:       builder.purgeOnStartup,
		DelayedSubscriptions: [][]string{},
		HandlersLock:         &sync.Mutex{},
		RPCLock:              &sync.Mutex{},
		SenderLock:           &sync.Mutex{},
		ConsumerLock:         &sync.Mutex{},
		Registrations:        make([]*gbus.Registration, 0),
		RPCHandlers:          make(map[string]gbus.MessageHandler),
		Serializer:           builder.serializer,
		DLX:                  builder.dlx,
		DefaultPolicies:      builder.defaultPolicies,
		DbPingTimeout:        3,
		Confirm:              builder.confirm,
	}

	if builder.logger != nil {
		gb.SetLogger(builder.logger)
	} else {
		gb.SetLogger(logrus.New())
	}

	if builder.workerNum < 1 {
		gb.WorkerNum = 1
	} else {
		gb.WorkerNum = builder.workerNum
	}
	var (
		sagaStore      saga.Store
		timeoutManager gbus.TimeoutManager
	)

	switch builder.txnlProvider {

	case "mysql":
		mysqltx, err := mysql.NewTxProvider(builder.txConnStr)
		if err != nil {
			panic(err)
		}
		gb.TxProvider = mysqltx

		mysql.EnsureSchema(mysqltx.Database, gb.SvcName)

		//TODO move purge logic into the NewSagaStore factory method
		sagaStore = mysql.NewSagaStore(gb.SvcName, mysqltx)
		if builder.purgeOnStartup {
			err := sagaStore.Purge()
			if err != nil {
				panic(err)
			}
		}
		gb.Outbox = mysql.NewOutbox(gb.SvcName, mysqltx, builder.purgeOnStartup, builder.busCfg.OutboxCfg)
		gb.Outbox.SetLogger(gb.Log())
		timeoutManager = mysql.NewTimeoutManager(gb, gb.TxProvider, gb.Log, svcName, builder.purgeOnStartup)

	default:
		err := fmt.Errorf("no provider found for passed in value %v", builder.txnlProvider)
		panic(err)
	}
	if builder.usingPingTimeout {
		gb.DbPingTimeout = builder.dbPingTimeout
	}

	//TODO move this into the NewSagaStore factory methods
	if builder.purgeOnStartup {
		err := sagaStore.Purge()
		if err != nil {
			panic(err)
		}
	}
	glue := saga.NewGlue(gb, sagaStore, svcName, gb.TxProvider, gb.Log, timeoutManager)
	glue.SetLogger(gb.Log())
	gb.Glue = glue
	return gb
}

func (builder *defaultBuilder) PurgeOnStartUp() gbus.Builder {
	builder.purgeOnStartup = true
	return builder
}

func (builder *defaultBuilder) WithOutbox(connStr string) gbus.Builder {

	//TODO: Add outbox support to builder
	return builder
}

func (builder *defaultBuilder) WithDeadlettering(deadletterExchange string) gbus.Builder {

	builder.dlx = deadletterExchange
	//TODO: Add outbox support to builder
	return builder
}

func (builder *defaultBuilder) WorkerNum(workers uint, prefetchCount uint) gbus.Builder {
	builder.workerNum = workers
	if prefetchCount > 0 {
		builder.PrefetchCount = prefetchCount
	}
	return builder
}

/*
	WithSagas configures the bus to work with Sagas.
	sagaStoreConnStr: the connection string to the saga store

	Supported Saga Stores and the format of the connection string to use:
	PostgreSQL: "PostgreSQL;User ID=root;Password=myPassword;Host=localhost;Port=5432;Database=myDataBase;"
	In Memory:  ""
*/
func (builder *defaultBuilder) WithSagas(sagaStoreConnStr string) gbus.Builder {
	builder.sagaStoreConnStr = sagaStoreConnStr
	return builder
}

func (builder *defaultBuilder) WithConfirms() gbus.Builder {
	builder.confirm = true
	return builder
}

func (builder *defaultBuilder) WithPolicies(policies ...gbus.MessagePolicy) gbus.Builder {
	builder.defaultPolicies = append(builder.defaultPolicies, policies...)
	return builder
}

func (builder *defaultBuilder) Txnl(provider, connStr string) gbus.Builder {
	builder.txnl = true
	builder.txConnStr = connStr
	builder.txnlProvider = provider
	return builder
}

func (builder *defaultBuilder) WithSerializer(serializer gbus.Serializer) gbus.Builder {
	builder.serializer = serializer
	return builder
}

func (builder *defaultBuilder) ConfigureHealthCheck(timeoutInSeconds time.Duration) gbus.Builder {
	builder.usingPingTimeout = true
	builder.dbPingTimeout = timeoutInSeconds
	return builder
}

func (builder *defaultBuilder) WithConfiguration(config gbus.BusConfiguration) gbus.Builder {

	builder.busCfg = config
	gbus.MaxRetryCount = config.MaxRetryCount

	if config.BaseRetryDuration > 0 {
		gbus.BaseRetryDuration = time.Millisecond * time.Duration(config.BaseRetryDuration)
	}
	return builder
}

func (builder *defaultBuilder) WithLogger(logger logrus.FieldLogger) gbus.Builder {
	builder.logger = logger
	return builder
}

//New :)
func New() Nu {
	return Nu{}
}

//Nu is the new New
type Nu struct {
}

//Bus inits a new BusBuilder
func (Nu) Bus(brokerConnStr string) gbus.Builder {
	return &defaultBuilder{
		busCfg:          gbus.BusConfiguration{},
		PrefetchCount:   1,
		connStr:         brokerConnStr,
		serializer:      serialization.NewGobSerializer(),
		defaultPolicies: make([]gbus.MessagePolicy, 0)}
}
