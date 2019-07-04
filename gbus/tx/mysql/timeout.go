package mysql

import (
	"database/sql"

	"github.com/wework/grabbit/gbus/tx"
)

//TimeoutManager is a mysql implementation of a persistent timeoutmanager
type TimeoutManager struct {
	*tx.TimeoutManager
}

func (tm *TimeoutManager) ensureSchema() error {
	tblName := tm.GetTimeoutsTableName()
	tx, e := tm.Txp.New()
	if e != nil {
		tm.Log().WithError(e).Error("failed to create schema for mysql timeout manager")
		return e
	}

	selectSQL := `SELECT 1 FROM ` + tblName + ` LIMIT 1;`

	tm.Log().Info(selectSQL)

	row := tx.QueryRow(selectSQL)
	var exists int
	err := row.Scan(&exists)
	if err != nil && err != sql.ErrNoRows {

		createTableSQL := `CREATE TABLE ` + tblName + ` (
      rec_id INT PRIMARY KEY AUTO_INCREMENT,
      saga_id VARCHAR(255) UNIQUE NOT NULL,
      timeout DATETIME NOT NULL
      )`

		createSagaTypeIndex := `CREATE INDEX ` + tblName + `_timeout_idx ON ` + tblName + ` (timeout)`

		if _, e := tx.Exec(createTableSQL); e != nil {
			tx.Rollback()
			return e
		}

		if _, e := tx.Exec(createSagaTypeIndex); e != nil {
			tx.Rollback()
			return e
		}
		return tx.Commit()
	} else if err != nil {
		return err
	}

	return nil
}

func (tm *TimeoutManager) purge() error {
	purgeSQL := `DELETE FROM ` + tm.GetTimeoutsTableName()

	tx, e := tm.Txp.New()
	if e != nil {
		tm.Log().WithError(e).Error("failed to purge timeout manager")
		return e
	}

	if _, execErr := tx.Exec(purgeSQL); execErr != nil {
		tm.Log().WithError(execErr).Error("failed to purge timeout manager")
		return tx.Rollback()
	}
	return tx.Commit()
}

//NewTimeoutManager creates a new instance of a mysql based TimeoutManager
func NewTimeoutManager(base *tx.TimeoutManager, purge bool) *TimeoutManager {
	tm := &TimeoutManager{TimeoutManager: base}
	tm.ensureSchema()
	if purge {
		tm.purge()
	}
	return tm
}
