package builder

import (
	"fmt"
	"go/types"
	"sync"
	"time"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/saga"
	"github.com/rhinof/grabbit/gbus/saga/stores"
	"github.com/rhinof/grabbit/gbus/serialization"
	"github.com/rhinof/grabbit/gbus/tx/mysql"
	"github.com/rhinof/grabbit/gbus/tx/pg"
)

type defaultBuilder struct {
	handlers         []types.Type
	connStr          string
	purgeOnStartup   bool
	sagaStoreConnStr string
	txnl             bool
	txConnStr        string
	txnlProvider     string
	workerNum        uint
	serializer       gbus.MessageEncoding
	dlx              string
	defaultPolicies  []gbus.MessagePolicy
	confirm          bool
	dbPingTimeout    time.Duration
	usingPingTimeout bool
}

func (builder *defaultBuilder) Build(svcName string) gbus.Bus {

	gb := &gbus.DefaultBus{
		AmqpConnStr:          builder.connStr,
		Outgoing:             &gbus.AMQPOutbox{},
		SvcName:              svcName,
		PurgeOnStartup:       builder.purgeOnStartup,
		DelayedSubscriptions: [][]string{},
		HandlersLock:         &sync.Mutex{},
		RPCLock:              &sync.Mutex{},
		SenderLock:           &sync.Mutex{},
		ConsumerLock:         &sync.Mutex{},
		IsTxnl:               builder.txnl,
		MsgHandlers:          make(map[string][]gbus.MessageHandler),
		RPCHandlers:          make(map[string]gbus.MessageHandler),
		Serializer:           builder.serializer,
		DLX:                  builder.dlx,
		DefaultPolicies:      make([]gbus.MessagePolicy, 0),
		DbPingTimeout:        3}

	gb.Confirm = builder.confirm
	if builder.workerNum < 1 {
		gb.WorkerNum = 1
	} else {
		gb.WorkerNum = builder.workerNum
	}
	var (
		sagaStore saga.Store
		txOutbox  gbus.TxOutbox
	)
	if builder.txnl {
		gb.IsTxnl = true
		switch builder.txnlProvider {
		case "pg":
			pgtx, err := pg.NewTxProvider(builder.txConnStr)
			if err != nil {
				panic(err)
			}
			gb.TxProvider = pgtx
			sagaStore = pg.NewSagaStore(gb.SvcName, pgtx)
		case "mysql":
			mysqltx, err := mysql.NewTxProvider(builder.txConnStr)
			if err != nil {
				panic(err)
			}
			gb.TxProvider = mysqltx
			sagaStore = mysql.NewSagaStore(gb.SvcName, mysqltx)
			if builder.purgeOnStartup {
				sagaStore.Purge()
			}
			txOutbox = mysql.NewTxOutbox(gb.SvcName, gb.Outgoing, mysqltx, builder.purgeOnStartup)
			gb.TxOutgoing = txOutbox

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
		sagaStore.Purge()
	}
	gb.Glue = saga.NewGlue(gb, sagaStore, svcName)
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

func (builder *defaultBuilder) WorkerNum(workers uint) gbus.Builder {
	builder.workerNum = workers
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

func (builder *defaultBuilder) WithSerializer(serializer gbus.MessageEncoding) gbus.Builder {
	builder.serializer = serializer
	return builder
}

func (builder *defaultBuilder) WithDbPingTimeout(timeoutInSeconds time.Duration) gbus.Builder {
	builder.usingPingTimeout = true
	builder.dbPingTimeout = timeoutInSeconds
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
		connStr:         brokerConnStr,
		serializer:      serialization.NewGobSerializer(),
		defaultPolicies: make([]gbus.MessagePolicy, 0)}
}
