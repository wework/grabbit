package tx

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rhinof/wework/grabbit/gbus"
	"github.com/rhinof/wework/grabbit/gbus/saga"
)

//SagaStore base type for embedding for new transactional saga stores
type SagaStore struct {
	Tx            gbus.TxProvider
	SvcName       string
	ParamsMarkers []string
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
			log.Errorf("%v failed to scan saga row\n%v", store.SvcName, error)
			return nil, error
		}

		reader := bytes.NewReader(sagaData)

		dec := gob.NewDecoder(reader)
		var instance saga.Instance
		instance.ConcurrencyCtrl = version
		decErr := dec.Decode(&instance)
		if decErr != nil {
			log.Errorf("%v failed to decode saga instance\n%v", store.SvcName, decErr)
			return nil, decErr
		}

		instances = append(instances, &instance)

	}
	return instances, nil

}

//GetSagasByType implements interface method store.GetSagasByType
func (store *SagaStore) GetSagasByType(tx *sql.Tx, sagaType reflect.Type) (instances []*saga.Instance, err error) {

	tblName := store.GetSagatableName()
	selectSQL := "SELECT saga_id, saga_type, saga_data, version FROM " + tblName + " WHERE saga_type=" + store.ParamsMarkers[0]

	rows, err := tx.Query(selectSQL, sagaType.String())
	defer rows.Close()

	if err != nil {
		return nil, err
	}

	if instances, err = store.scanInstances(rows); err != nil {
		log.Errorf("%v SagaStore failed yo scan saga db record\nError:\n%v", store.SvcName, err)

		return nil, err
	}
	return instances, nil
}

//UpdateSaga implements interface method store.UpdateSaga
func (store *SagaStore) UpdateSaga(tx *sql.Tx, instance *saga.Instance) (err error) {
	tblName := store.GetSagatableName()
	currentVersion := instance.ConcurrencyCtrl
	nextVersion := instance.ConcurrencyCtrl + 1
	instance.ConcurrencyCtrl = nextVersion
	var buf []byte
	if buf, err = store.serilizeSaga(instance); err != nil {
		log.Errorf("%v SagaStore failed to encode saga with sagaID - %v\n%v", store.SvcName, instance.ID, err)
		return err
	}

	updateSQL := `UPDATE ` + tblName + ` SET saga_data=` + store.ParamsMarkers[0] + `, version=` + store.ParamsMarkers[1] + `
WHERE saga_id=` + store.ParamsMarkers[2] + ` AND version=` + store.ParamsMarkers[3] + ``
	result, err := tx.Exec(updateSQL, buf, nextVersion, instance.ID, currentVersion)

	if err != nil {
		return err
	} else if rowsAffected, ee := result.RowsAffected(); ee != nil || rowsAffected == 0 {
		return fmt.Errorf("saga with saga_id:%v had stale data when updating. :%v", instance.ID, ee)
	}
	return nil
}

//RegisterSagaType implements interface method store.RegisterSagaType
func (store *SagaStore) RegisterSagaType(saga gbus.Saga) {
	gob.Register(saga)
}

//DeleteSaga implements interface method store.DeleteSaga
func (store *SagaStore) DeleteSaga(tx *sql.Tx, instance *saga.Instance) error {
	tblName := store.GetSagatableName()
	deleteSQL := `DELETE FROM ` + tblName + ` WHERE saga_id=` + store.ParamsMarkers[0] + ``
	_, err := tx.Exec(deleteSQL, instance.ID)
	return err
}

//GetSagaByID implements interface method store.GetSagaByID
func (store *SagaStore) GetSagaByID(tx *sql.Tx, sagaID string) (*saga.Instance, error) {

	tblName := store.GetSagatableName()
	selectSQL := `SELECT saga_id, saga_type, saga_data, version FROM ` + tblName + ` WHERE saga_id=` + store.ParamsMarkers[0] + ``

	rows, error := tx.Query(selectSQL, sagaID)
	defer rows.Close()
	if error != nil {
		log.Errorf("%v Failed to fetch saga with id %s\n%s", store.GetSagatableName(), sagaID, error)
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
func (store *SagaStore) SaveNewSaga(tx *sql.Tx, sagaType reflect.Type, newInstance *saga.Instance) (err error) {
	store.RegisterSagaType(newInstance.UnderlyingInstance)
	tblName := store.GetSagatableName()
	insertSQL := `INSERT INTO ` + tblName + ` (saga_id, saga_type, saga_data, version)
	VALUES (` + store.ParamsMarkers[0] + `, ` + store.ParamsMarkers[1] + `, ` + store.ParamsMarkers[2] + `, ` + store.ParamsMarkers[3] + `)`

	var buf []byte
	if buf, err = store.serilizeSaga(newInstance); err != nil {
		log.Errorf("%v failed to encode saga with sagaID - %v\n%v", store.SvcName, newInstance.ID, err)
		return err
	}
	_, err = tx.Exec(insertSQL, newInstance.ID, sagaType.String(), buf, newInstance.ConcurrencyCtrl)
	if err != nil {
		log.Errorf("%v failed saving new saga\n%v\nSQL:\n%v", store.SvcName, err, insertSQL)
		return err
	}
	return nil
}

//Purge cleans up the saga store, to be used in tests and in extreme situations in production
func (store *SagaStore) Purge() error {
	tx := store.NewTx()

	log.Printf("Purging saga table %v", store.GetSagatableName())
	results, err := tx.Exec("DELETE FROM  " + store.GetSagatableName())
	if err != nil {
		log.Errorf("%v Failed to purge saga table %s", store.SvcName, err)
		return err
	}
	if txErr := tx.Commit(); txErr != nil {
		return txErr
	}
	rowsEffected, resultsErr := results.RowsAffected()
	if resultsErr != nil {
		log.Warnf("%v Failed to fect number of deleted saga records %s", store.SvcName, err)
	} else {
		log.Printf("%v Purged %d saga instances", store.SvcName, rowsEffected)
	}

	return nil
}

func (store *SagaStore) serilizeSaga(instance *saga.Instance) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	error := enc.Encode(instance)
	return buf.Bytes(), error
}

//NewTx creates a new transaction from the underlying TxProvider
func (store *SagaStore) NewTx() *sql.Tx {
	tx, error := store.Tx.New()
	if error != nil {
		e := fmt.Errorf("can't initialize sage store.\nerror:\n%s", error)
		panic(e)
	}

	return tx
}

//GetSagatableName returns the table name in which to store the Sagas
func (store *SagaStore) GetSagatableName() string {

	var re = regexp.MustCompile("-|;|\\|")
	sanitized := re.ReplaceAllString(store.SvcName, "")

	return strings.ToLower("grabbit_" + sanitized + "_sagas")
}
