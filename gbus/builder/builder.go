package builder

import (
	"fmt"
	"sync"
	"time"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/saga"
	"github.com/wework/grabbit/gbus/saga/stores"
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
}

func (builder *defaultBuilder) Build(svcName string) gbus.Bus {

	gb := &gbus.DefaultBus{
		AmqpConnStr:   builder.connStr,
		PrefetchCount: 1,
		Outgoing: &gbus.AMQPOutbox{
			SvcName: svcName,
		},
		SvcName:              svcName,
		PurgeOnStartup:       builder.purgeOnStartup,
		DelayedSubscriptions: [][]string{},
		HandlersLock:         &sync.Mutex{},
		RPCLock:              &sync.Mutex{},
		SenderLock:           &sync.Mutex{},
		ConsumerLock:         &sync.Mutex{},
		IsTxnl:               builder.txnl,
		Registrations:        make([]*gbus.Registration, 0),
		RPCHandlers:          make(map[string]gbus.MessageHandler),
		Serializer:           builder.serializer,
		DLX:                  builder.dlx,
		DefaultPolicies:      builder.defaultPolicies,
		DbPingTimeout:        3}

	gb.Confirm = builder.confirm
	if builder.workerNum < 1 {
		gb.WorkerNum = 1
	} else {
		gb.WorkerNum = builder.workerNum
	}
	gb.PrefetchCount = builder.PrefetchCount
	var (
		sagaStore saga.Store
	)
	if builder.txnl {
		gb.IsTxnl = true
		switch builder.txnlProvider {

		case "mysql":
			mysqltx, err := mysql.NewTxProvider(builder.txConnStr)
			if err != nil {
				panic(err)
			}
			gb.TxProvider = mysqltx
			sagaStore = mysql.NewSagaStore(gb.SvcName, mysqltx)
			if builder.purgeOnStartup {
				err := sagaStore.Purge()
				if err != nil {
					panic(err)
				}
			}
			gb.Outbox = mysql.NewOutbox(gb.SvcName, mysqltx, builder.purgeOnStartup)

		default:
			err := fmt.Errorf("no provider found for passed in value %v", builder.txnlProvider)
			panic(err)
		}
	} else {
		sagaStore = stores.NewInMemoryStore()
	}

	if builder.usingPingTimeout {
		gb.DbPingTimeout = builder.dbPingTimeout
	}

	if builder.purgeOnStartup {
		err := sagaStore.Purge()
		if err != nil {
			panic(err)
		}
	}
	gb.Glue = saga.NewGlue(gb, sagaStore, svcName, gb.TxProvider)
	return gb
}

func (builder *defaultBuilder) PurgeOnStartUp() gbus.Builder {
	builder.purgeOnStartup = true
	return builder
}

func (builder *defaultBuilder) WithOutbox(connStr string) gbus.Builder {

	//TODO: Add outbox suppoert to builder
	return builder
}

func (builder *defaultBuilder) WithDeadlettering(deadletterExchange string) gbus.Builder {

	builder.dlx = deadletterExchange
	//TODO: Add outbox suppoert to builder
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

func (builder *defaultBuilder) RetriesNum(retries uint) gbus.Builder {
	gbus.MaxRetryCount = retries
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
		PrefetchCount:   1,
		connStr:         brokerConnStr,
		serializer:      serialization.NewGobSerializer(),
		defaultPolicies: make([]gbus.MessagePolicy, 0)}
}
