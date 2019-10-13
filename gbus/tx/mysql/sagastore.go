package mysql

import (
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/saga"
	"github.com/wework/grabbit/gbus/tx"
)

//SagaStore implements the saga/store interface on top of MySQL
type SagaStore struct {
	*tx.SagaStore
}

//NewSagaStore creates a bew SagaStore
func NewSagaStore(svcName string, txProvider gbus.TxProvider) saga.Store {

	base := &tx.SagaStore{
		Glogged:       &gbus.Glogged{},
		Tx:            txProvider,
		SvcName:       svcName,
		ParamsMarkers: getParamsMarker()}
	store := &SagaStore{
		base}
	return store
}

func getParamsMarker() []string {

	markers := make([]string, 0)
	for i := 0; i < 100; i++ {
		markers = append(markers, "?")
	}

	return markers
}
