package tx

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/saga"
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
			store.log().WithError(error).Error("failed to scan saga row")
			return nil, error
		}

		reader := bytes.NewReader(sagaData)

		dec := gob.NewDecoder(reader)
		var instance saga.Instance
		instance.ConcurrencyCtrl = version
		decErr := dec.Decode(&instance)
		if decErr != nil {
			store.log().WithError(decErr).Error("failed to decode saga instance")
			return nil, decErr
		}

		instances = append(instances, &instance)

	}
	return instances, nil

}

//GetSagasByType implements interface method store.GetSagasByType
func (store *SagaStore) GetSagasByType(tx *sql.Tx, sagaType reflect.Type) (instances []*saga.Instance, err error) {

	tblName := GetSagatableName(store.SvcName)
	selectSQL := "SELECT saga_id, saga_type, saga_data, version FROM " + tblName + " WHERE saga_type=" + store.ParamsMarkers[0]

	rows, err := tx.Query(selectSQL, sagaType.String())
	defer func() {
		err := rows.Close()
		if err != nil {
			store.log().WithError(err).Error("could not close rows")
		}
	}()

	if err != nil {
		return nil, err
	}

	if instances, err = store.scanInstances(rows); err != nil {
		store.log().WithError(err).Error("SagaStore failed to scan saga db record")
		return nil, err
	}
	return instances, nil
}

//UpdateSaga implements interface method store.UpdateSaga
func (store *SagaStore) UpdateSaga(tx *sql.Tx, instance *saga.Instance) (err error) {
	tblName := GetSagatableName(store.SvcName)
	currentVersion := instance.ConcurrencyCtrl
	nextVersion := instance.ConcurrencyCtrl + 1
	instance.ConcurrencyCtrl = nextVersion
	var buf []byte
	if buf, err = store.serilizeSaga(instance); err != nil {
		store.log().WithError(err).WithField("saga_id", instance.ID).Error("SagaStore failed to encode saga")
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
	tblName := GetSagatableName(store.SvcName)
	deleteSQL := `DELETE FROM ` + tblName + ` WHERE saga_id= ?`
	result, err := tx.Exec(deleteSQL, instance.ID)
	if err != nil {
		return err
	}
	rowsEffected, e := result.RowsAffected()
	if rowsEffected == 0 || e != nil {
		return errors.New("couldn't delete saga, saga not found orr an error occurred")
	}

	return nil
}

//GetSagaByID implements interface method store.GetSagaByID
func (store *SagaStore) GetSagaByID(tx *sql.Tx, sagaID string) (*saga.Instance, error) {

	tblName := GetSagatableName(store.SvcName)
	selectSQL := `SELECT saga_id, saga_type, saga_data, version FROM ` + tblName + ` WHERE saga_id=` + store.ParamsMarkers[0] + ``

	rows, err := tx.Query(selectSQL, sagaID)
	defer func() {
		err := rows.Close()
		if err != nil {
			store.log().WithError(err).Error("could not close rows")
		}
	}()
	if err != nil {
		store.log().WithError(err).
			WithFields(log.Fields{"saga_id": sagaID, "table_name": GetSagatableName(store.SvcName)}).
			Error("Failed to fetch saga")

		return nil, err
	}
	instances, err := store.scanInstances(rows)
	if err != nil {
		return nil, err
	}
	if len(instances) == 0 {
		return nil, fmt.Errorf("no saga found for saga with saga_id:%v", sagaID)
	}
	return instances[0], nil
}

//SaveNewSaga implements interface method store.SaveNewSaga
func (store *SagaStore) SaveNewSaga(tx *sql.Tx, sagaType reflect.Type, newInstance *saga.Instance) (err error) {
	store.RegisterSagaType(newInstance.UnderlyingInstance)
	tblName := GetSagatableName(store.SvcName)
	insertSQL := `INSERT INTO ` + tblName + ` (saga_id, saga_type, saga_data, version) VALUES (?, ?, ?, ?)`

	var buf []byte
	if buf, err = store.serilizeSaga(newInstance); err != nil {
		store.log().WithError(err).WithField("saga_id", newInstance.ID).Error("failed to encode saga with sagaID")
		return err
	}
	_, err = tx.Exec(insertSQL, newInstance.ID, sagaType.String(), buf, newInstance.ConcurrencyCtrl)
	if err != nil {
		store.log().WithError(err).Error("failed saving new saga")
		return err
	}
	return nil
}

//Purge cleans up the saga store, to be used in tests and in extreme situations in production
func (store *SagaStore) Purge() error {
	tx := store.NewTx()
	store.log().WithField("saga_table", GetSagatableName(store.SvcName)).Info("Purging saga table")
	deleteSQL := fmt.Sprintf("DELETE FROM %s", GetSagatableName(store.SvcName))
	results, err := tx.Exec(deleteSQL)
	if err != nil {
		store.log().WithError(err).Error("failed to purge saga table")
		return err
	}
	if txErr := tx.Commit(); txErr != nil {
		return txErr
	}
	rowsEffected, resultsErr := results.RowsAffected()
	if resultsErr != nil {
		store.log().WithError(err).Warn("failed to fetch number of deleted saga records")
	} else {
		store.log().WithField("deleted_instances", rowsEffected).Info("purged saga store")
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
	tx, err := store.Tx.New()
	if err != nil {
		e := fmt.Errorf("can't initialize sage store.\nerror:\n%s", err)
		panic(e)
	}

	return tx
}

//GetSagatableName returns the table name in which to store the Sagas
func GetSagatableName(svcName string) string {

	var re = regexp.MustCompile(`-|;|\\|`)
	sanitized := re.ReplaceAllString(svcName, "")

	return strings.ToLower("grabbit_" + sanitized + "_sagas")
}

func (store *SagaStore) log() *log.Entry {
	return log.WithField("store", "mysql")
}
