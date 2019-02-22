package pg

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/saga"
	"github.com/rhinof/grabbit/gbus/tx"
)

//SagaStore implements the saga/store interface on top of PostgreSQL
type SagaStore struct {
	tx      tx.Provider
	svcName string
}

func (store *SagaStore) scanInstances(rows *sql.Rows) ([]*saga.Instance, error) {

	instances := make([]*saga.Instance, 0)
	// reader := &Reader{0, -1}

	for rows.Next() {
		var sagaID, sagaType string
		var version int
		var sagaData []byte

		error := rows.Scan(&sagaID, &sagaType, &sagaData, &version)
		if error == sql.ErrNoRows {
			return nil, error
		} else if error != nil {
			log.Printf("failed to scan saga row\n%v", error)
			return nil, error
		}

		reader := bytes.NewReader(sagaData)

		dec := gob.NewDecoder(reader)
		var instance saga.Instance
		decErr := dec.Decode(&instance)
		if decErr != nil {
			log.Printf("failed to decode saga instance\n%v", decErr)
			return nil, decErr
		}

		instances = append(instances, &instance)

	}
	return instances, nil

}

//GetSagasByType implements interface method store.GetSagasByType
func (store *SagaStore) GetSagasByType(tx *sql.Tx, sagaType reflect.Type) ([]*saga.Instance, error) {

	tblName := store.getSagatableName()
	selectSQL := "SELECT saga_id, saga_type, saga_data, version FROM " + tblName + " WHERE saga_type=$1"

	rows, error := tx.Query(selectSQL, sagaType.String())
	defer rows.Close()

	if error != nil {
		return nil, error
	}

	if instances, scanError := store.scanInstances(rows); scanError == nil {
		return instances, nil
	} else {
		log.Printf("SagaStore failed yo scan saga db record\nError:\n%v", error)
		log.Println(error)
		return nil, scanError
	}
}

//UpdateSaga implements interface method store.UpdateSaga
func (store *SagaStore) UpdateSaga(tx *sql.Tx, instance *saga.Instance) error {
	tblName := store.getSagatableName()
	if buf, error := store.serilizeSaga(instance); error != nil {
		log.Printf("SagaStore failed to encode saga with sagaID - %v\n%v", instance.ID, error)
		return error
	} else {
		nextRecVersion := instance.ConcurrencyCtrl + 1
		updateSQL := `UPDATE ` + tblName + ` SET saga_data=$1, version=$2
	WHERE saga_id=$3 AND version=$4`
		result, error := tx.Exec(updateSQL, buf, nextRecVersion, instance.ID, instance.ConcurrencyCtrl)

		if error != nil {
			return error
		} else if rowsAffected, ee := result.RowsAffected(); ee != nil || rowsAffected == 0 {
			return fmt.Errorf("saga with saga_id:%v had stale data when updating. :%v", instance.ID, ee)
		}
	}
	return nil
}

//RegisterSagaType implements interface method store.RegisterSagaType
func (store *SagaStore) RegisterSagaType(saga gbus.Saga) {
	gob.Register(saga)
}

//DeleteSaga implements interface method store.DeleteSaga
func (store *SagaStore) DeleteSaga(tx *sql.Tx, instance *saga.Instance) error {
	tblName := store.getSagatableName()
	deleteSQL := `DELETE FROM ` + tblName + ` WHERE saga_id=$1`
	_, err := tx.Exec(deleteSQL, instance.ID)
	return err
}

//GetSagaByID implements interface method store.GetSagaByID
func (store *SagaStore) GetSagaByID(tx *sql.Tx, sagaID string) (*saga.Instance, error) {

	tblName := store.getSagatableName()
	selectSQL := `SELECT saga_id, saga_type, saga_data, version FROM ` + tblName + ` WHERE saga_id=$1`

	rows, error := tx.Query(selectSQL, sagaID)
	defer rows.Close()
	if error != nil {
		return nil, error
	}
	instances, error := store.scanInstances(rows)
	if error != nil {
		return nil, error
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("no saga found for saga with saga_id:%v", sagaID)
	}
	return instances[0], nil
}

//SaveNewSaga implements interface method store.SaveNewSaga
func (store *SagaStore) SaveNewSaga(tx *sql.Tx, sagaType reflect.Type, newInstance *saga.Instance) error {
	store.RegisterSagaType(newInstance.UnderlyingInstance)
	tblName := store.getSagatableName()
	insertSQL := `INSERT INTO ` + tblName + ` (saga_id, saga_type, saga_data, version)
	VALUES ($1, $2, $3, $4)`

	if buf, error := store.serilizeSaga(newInstance); error != nil {
		log.Printf("failed to encode saga with sagaID - %v\n%v", newInstance.ID, error)
		return error
	} else {
		_, txError := tx.Exec(insertSQL, newInstance.ID, sagaType.String(), buf, newInstance.ConcurrencyCtrl)
		if txError != nil {
			log.Printf("failed saving new saga\n%v\nSQL:\n%v", txError, insertSQL)
			return txError
		}
	}
	return nil
}

func (store *SagaStore) serilizeSaga(instance *saga.Instance) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	error := enc.Encode(instance)
	return buf.Bytes(), error
}

func (store *SagaStore) newTx() *sql.Tx {
	tx, error := store.tx.New()
	if error != nil {
		e := fmt.Errorf("can't initialize sage store.\nerror:\n%s", error)
		panic(e)
	}

	return tx
}

func (store *SagaStore) ensureSchema() {
	log.Println("ensuring saga schema exists")
	if tablesExists := store.sagaTablesExist(); tablesExists == false {
		log.Println("could not find saga schema, attempting to creat schema")
		store.createSagaTables()
	}
}

func (store *SagaStore) getSagatableName() string {

	return strings.ToLower("grabbit_" + store.svcName + "_sagas")
}
func (store *SagaStore) sagaTablesExist() bool {

	tblName := store.getSagatableName()
	tx := store.newTx()

	sql := `SELECT EXISTS (
   SELECT 1
   FROM   information_schema.tables
   WHERE  table_schema = 'public'
   AND    table_name = '` + tblName + `');`

	log.Println(sql)

	row := tx.QueryRow(sql)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		e := fmt.Errorf("can't initialize sage store.\nerror:\n%s", err)
		panic(e)
	}
	return exists
}

func (store *SagaStore) createSagaTables() {
	tblName := store.getSagatableName()
	tx := store.newTx()

	createTable := `CREATE TABLE ` + tblName + ` (
		rec_id SERIAL PRIMARY KEY,
		saga_id VARCHAR(255) UNIQUE NOT NULL,
		saga_type VARCHAR(255)  NOT NULL,
		saga_data bytea NOT NULL,
		version integer NOT NULL DEFAULT 0,
		last_update timestamp  DEFAULT NOW()
		)`
	createSagaTypeIndex := `CREATE INDEX ` + tblName + `_sagatype_idx ON ` + tblName + ` (saga_type)`

	sqls := [2]string{
		createTable,
		createSagaTypeIndex}

	for i, sql := range sqls {
		_, error := tx.Exec(sql)
		log.Printf("creating saga tables - step %v\n%s", i, sql)
		if error != nil {
			txErr := fmt.Errorf("failed to create saga tables.\n%v", error)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Printf("Could not roll back: %v\n", rollbackErr)
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
func NewSagaStore(svcName string, txProvider tx.Provider) saga.Store {
	store := &SagaStore{
		tx:      txProvider,
		svcName: svcName}
	store.ensureSchema()
	return store
}
