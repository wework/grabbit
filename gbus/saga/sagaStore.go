package saga

import "database/sql"

type SagaStore interface {
	GetSagaByID(tx *sql.Tx, sagaID string) (*SagaInstance, error)
	SaveNewSaga(tx *sql.Tx, def *SagaDef, newInstance *SagaInstance) error
	UpdateSaga(tx *sql.Tx, instance *SagaInstance) error
}
