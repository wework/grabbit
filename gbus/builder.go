package gbus

//Builder is the main interface that should be used to create an instance of a Bus
type Builder interface {
	PurgeOnStartUp() Builder
	WithOutbox(connStr string) Builder
	WithDeadlettering(deadletterExchange string) Builder
	WithSagas(sagaStoreConnStr string) Builder
	Txnl(txResourceConnStr string) Builder

	Build(svcName string) Bus
}
