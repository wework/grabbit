package mysql

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/saga"
	"github.com/wework/grabbit/gbus/tx"
)

//SagaStore implements the saga/store interface on top of PostgreSQL
type SagaStore struct {
	*tx.SagaStore
}

func (store *SagaStore) log() *log.Entry {
	return log.WithField("_service", store.SvcName)
}

func (store *SagaStore) ensureSchema() {
	store.log().Info("ensuring saga schema exists")
	if tablesExists := store.sagaTablesExist(); !tablesExists {

		store.log().Info("could not find saga schema, attempting to creat schema")

		store.createSagaTables()
	}
}

func (store *SagaStore) sagaTablesExist() bool {

	tblName := store.GetSagatableName()
	tx := store.NewTx()
	defer func() {
		err := tx.Commit()
		if err != nil {
			store.log().WithError(err).Error("could not commit sagaTablesExist")
		}
	}()

	selectSQL := `SELECT 1 FROM ` + tblName + ` LIMIT 1;`

	store.log().Info(selectSQL)

	row := tx.QueryRow(selectSQL)
	var exists int
	err := row.Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false
	}

	return true
}

func (store *SagaStore) createSagaTables() {
	tblName := store.GetSagatableName()
	tx := store.NewTx()

	createTable := `CREATE TABLE ` + tblName + ` (
		rec_id INT PRIMARY KEY AUTO_INCREMENT,
		saga_id VARCHAR(255) UNIQUE NOT NULL,
		saga_type VARCHAR(255)  NOT NULL,
		saga_data LONGBLOB NOT NULL,
		version integer NOT NULL DEFAULT 0,
		last_update timestamp  DEFAULT NOW()
		)`
	createSagaTypeIndex := `CREATE INDEX ` + tblName + `_sagatype_idx ON ` + tblName + ` (saga_type)`

	sqls := [2]string{
		createTable,
		createSagaTypeIndex}

	for i, sql := range sqls {
		_, error := tx.Exec(sql)

		store.log().WithFields(log.Fields{"step": i, "sql": sql}).Info("creating saga tables")

		if error != nil {
			txErr := fmt.Errorf("failed to create saga tables.\n%v", error)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.WithError(rollbackErr).Error("could not rollback transaction")
			}
			panic(txErr)
		}
	}

	error := tx.Commit()
	if error != nil {
		txErr := fmt.Errorf("failed to create saga tables.\n%v", error)
		panic(txErr)
	}

}

//NewSagaStore creates a bew SagaStore
func NewSagaStore(svcName string, txProvider gbus.TxProvider) saga.Store {

	base := &tx.SagaStore{
		Tx:            txProvider,
		SvcName:       svcName,
		ParamsMarkers: getParamsMarker()}
	store := &SagaStore{
		base}
	store.ensureSchema()
	return store
}

func getParamsMarker() []string {

	markers := make([]string, 0)
	for i := 0; i < 100; i++ {
		markers = append(markers, "?")
	}

	return markers
}
