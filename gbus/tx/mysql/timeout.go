package mysql

import (
	"database/sql"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
)

var _ gbus.TimeoutManager = &TimeoutManager{}

//TimeoutManager is a mysql implementation of a persistent timeoutmanager
type TimeoutManager struct {
	Bus         gbus.Bus
	Log         func() logrus.FieldLogger
	TimeoutSaga func(*sql.Tx, string) error
	Txp         gbus.TxProvider
	SvcName     string
	exit        chan bool
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

		createTableSQL := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
      rec_id INT PRIMARY KEY AUTO_INCREMENT,
      saga_id VARCHAR(255) UNIQUE NOT NULL,
	  timeout DATETIME NOT NULL,
	  INDEX ix_` + tm.GetTimeoutsTableName() + `_timeout_date(timeout)
      )`

		if _, e := tx.Exec(createTableSQL); e != nil {
			if rbkErr := tx.Rollback(); rbkErr != nil {
				tm.Log().Warn("timeout manager failed to rollback transaction")
			}
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

func (tm *TimeoutManager) start() {
	go tm.trackTimeouts()
}

func (tm *TimeoutManager) trackTimeouts() {
	tick := time.NewTicker(time.Second * 1).C
	for {
		select {
		case <-tick:
			tx, txe := tm.Txp.New()
			if txe != nil {
				tm.Log().WithError(txe).Warn("timeout manager failed to create a transaction")
				continue
			}
			now := time.Now().UTC()
			getTimeoutsSQL := `select saga_id from ` + tm.GetTimeoutsTableName() + ` where timeout < ? LIMIT 100`
			rows, selectErr := tx.Query(getTimeoutsSQL, now)
			if selectErr != nil {
				tm.Log().WithError(selectErr).Error("timeout manager failed to query for pending timeouts")
				rows.Close()
				continue
			}

			sagaIDs := make([]string, 0)
			for rows.Next() {
				var sagaID string

				if err := rows.Scan(&sagaID); err != nil {
					tm.Log().WithError(err).Error("failed to scan timeout record")
				}
				sagaIDs = append(sagaIDs, sagaID)
			}
			tm.executeTimeout(sagaIDs)
		case <-tm.exit:
			return
		}
	}
}

func (tm *TimeoutManager) executeTimeout(sagaIDs []string) {

	for _, sagaID := range sagaIDs {
		tx, txe := tm.Txp.New()
		if txe != nil {
			tm.Log().WithError(txe).Warn("timeout manager failed to create a transaction")
			return
		}

		callErr := tm.TimeoutSaga(tx, sagaID)
		clrErr := tm.ClearTimeout(tx, sagaID)

		if callErr != nil || clrErr != nil {
			logEntry := tm.Log()
			if callErr != nil {
				logEntry = logEntry.WithError(callErr)
			} else {
				logEntry = logEntry.WithError(clrErr)
			}
			logEntry.WithField("sagaID", sagaID).Error("timing out a saga failed")
			rlbe := tx.Rollback()
			if rlbe != nil {
				tm.Log().WithError(rlbe).Warn("timeout manager failed to rollback transaction")
			}
			return
		}

		cmte := tx.Commit()
		if cmte != nil {
			tm.Log().WithError(cmte).Warn("timeout manager failed to commit transaction")
		}
	}

}

//RegisterTimeout requests a timeout from the timeout manager
func (tm *TimeoutManager) RegisterTimeout(tx *sql.Tx, sagaID string, duration time.Duration) error {

	timeoutTime := time.Now().UTC().Add(duration)

	insertSQL := "INSERT INTO " + tm.GetTimeoutsTableName() + " (saga_id, timeout) VALUES(?, ?)"
	_, insertErr := tx.Exec(insertSQL, sagaID, timeoutTime)
	if insertErr == nil {
		tm.Log().Info("timout inserted into timeout manager")
	}

	return insertErr
}

//ClearTimeout clears a timeout for a specific saga
func (tm *TimeoutManager) ClearTimeout(tx *sql.Tx, sagaID string) error {

	deleteSQL := `delete from ` + tm.GetTimeoutsTableName() + ` where saga_id = ?`
	_, err := tx.Exec(deleteSQL, sagaID)
	return err
}

//AcceptTimeoutFunction accepts the timeouting function
func (tm *TimeoutManager) AcceptTimeoutFunction(timeoutFunc func(tx *sql.Tx, sagaID string) error) {
	tm.TimeoutSaga = timeoutFunc
}

//GetTimeoutsTableName returns the table name in which to store timeouts
func (tm *TimeoutManager) GetTimeoutsTableName() string {

	var re = regexp.MustCompile(`-|;|\\|`)
	sanitized := re.ReplaceAllString(tm.SvcName, "")

	return strings.ToLower("grabbit_" + sanitized + "_timeouts")
}

//NewTimeoutManager creates a new instance of a mysql based TimeoutManager
func NewTimeoutManager(bus gbus.Bus, txp gbus.TxProvider, logger func() logrus.FieldLogger, svcName string, purge bool) *TimeoutManager {
	tm := &TimeoutManager{
		Log:     logger,
		Bus:     bus,
		Txp:     txp,
		SvcName: svcName}

	if err := tm.ensureSchema(); err != nil {
		panic(err)
	}
	if purge {
		if err := tm.purge(); err != nil {
			panic(err)
		}
	}
	tm.start()
	return tm
}
