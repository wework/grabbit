package builder

import (
	"go/types"
	"sync" // "github.com/rhinof/grabbit/gbus"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/saga"
	"github.com/rhinof/grabbit/gbus/tx"
	"github.com/streadway/amqp"
)

type defaultBuilder struct {
	handlers         []types.Type
	connStr          string
	purgeOnStartup   bool
	sagaStoreConnStr string
	txnl             bool
	txConnStr        string
}

func (builder *defaultBuilder) Build(svcName string) gbus.Bus {

	gb := &DefaultBus{
		amqpConnStr:          builder.connStr,
		svcName:              svcName,
		purgeOnStartup:       builder.purgeOnStartup,
		connErrors:           make(chan *amqp.Error),
		registeredSchemas:    make(map[string]bool),
		delayedSubscriptions: [][]string{},
		handlersLock:         &sync.Mutex{},
		isTxnl:               builder.txnl,
		msgHandlers:          make(map[string][]gbus.MessageHandler)}

	sagaStore := saga.NewInMemoryStore()
	gb.sagaManager = saga.NewSagaManager(gb, sagaStore, svcName)

	if gb.isTxnl {
		pgtx, err := tx.NewPgProvider(builder.txConnStr)
		if err != nil {
			panic(err)
		}
		gb.txProvider = pgtx
	}

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
	//TODO: Add outbox suppoert to builder
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

func (builder *defaultBuilder) Txnl(txnlResourceConnStr string) gbus.Builder {
	builder.txnl = true
	builder.txConnStr = txnlResourceConnStr
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
	return &defaultBuilder{connStr: brokerConnStr}
}
