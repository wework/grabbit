package saga

import "database/sql"

//Store abtracts the way sagas get persisted
type Store interface {
	GetSagaByID(tx *sql.Tx, sagaID string) (*Instance, error)
	SaveNewSaga(tx *sql.Tx, def *Def, newInstance *Instance) error
	UpdateSaga(tx *sql.Tx, instance *Instance) error
	DeleteSaga(tx *sql.Tx, instance *Instance) error
}
