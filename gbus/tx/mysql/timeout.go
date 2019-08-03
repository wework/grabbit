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
	Bus               gbus.Bus
	Log               func() logrus.FieldLogger
	TimeoutSaga       func(*sql.Tx, string) error
	Txp               gbus.TxProvider
	SvcName           string
	timeoutsTableName string
	exit              chan bool
}

func (tm *TimeoutManager) ensureSchema() error {
	tblName := tm.timeoutsTableName
	tx, e := tm.Txp.New()
	if e != nil {
		tm.Log().WithError(e).Error("failed to create schema for mysql timeout manager")
		return e
	}

	createTableSQL := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
      rec_id INT PRIMARY KEY AUTO_INCREMENT,
      saga_id VARCHAR(255) UNIQUE NOT NULL,
	  timeout DATETIME NOT NULL,
	  INDEX (timeout),
	  INDEX (saga_id)
	 )`

	if _, e := tx.Exec(createTableSQL); e != nil {
		if rbkErr := tx.Rollback(); rbkErr != nil {
			tm.Log().Warn("timeout manager failed to rollback transaction")
		}
		return e
	}
	return tx.Commit()
}

func (tm *TimeoutManager) purge() error {
	purgeSQL := `DELETE FROM ` + tm.timeoutsTableName

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

//Start starts the timeout manager
func (tm *TimeoutManager) Start() error {
	go tm.trackTimeouts()
	return nil
}

//Stop shuts down the timeout manager
func (tm *TimeoutManager) Stop() error {
	tm.exit <- true
	return nil
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
			getTimeoutsSQL := `select saga_id from ` + tm.timeoutsTableName + ` where timeout < ? LIMIT 100`
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

func (tm *TimeoutManager) lockTimeoutRecord(tx *sql.Tx, sagaID string) error {

	selectTimeout := `SELECT saga_id FROM ` + tm.timeoutsTableName + ` WHERE saga_id = ? FOR UPDATE`
	row := tx.QueryRow(selectTimeout, sagaID)
	//scan the row so we can determine if the lock has been successfully acquired
	var x string
	return row.Scan(&x)
}

func (tm *TimeoutManager) executeTimeout(sagaIDs []string) {

	for _, sagaID := range sagaIDs {
		tx, txe := tm.Txp.New()
		if txe != nil {
			tm.Log().WithError(txe).Warn("timeout manager failed to create a transaction")
			return
		}
		lckErr := tm.lockTimeoutRecord(tx, sagaID)
		if lckErr != nil {
			tm.Log().WithField("saga_id", sagaID).Info("failed to obtain lock for saga timeout")
			_ = tx.Rollback()
			continue
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

	insertSQL := "INSERT INTO " + tm.timeoutsTableName + " (saga_id, timeout) VALUES(?, ?)"
	_, insertErr := tx.Exec(insertSQL, sagaID, timeoutTime)
	if insertErr == nil {
		tm.Log().WithField("timeout_duration", duration).Debug("timout inserted into timeout manager")
	}

	return insertErr
}

//ClearTimeout clears a timeout for a specific saga
func (tm *TimeoutManager) ClearTimeout(tx *sql.Tx, sagaID string) error {

	deleteSQL := `delete from ` + tm.timeoutsTableName + ` where saga_id_id = ?`
	_, err := tx.Exec(deleteSQL, sagaID)
	return err
}

//SetTimeoutFunction accepts the timeouting function
func (tm *TimeoutManager) SetTimeoutFunction(timeoutFunc func(tx *sql.Tx, sagaID string) error) {
	tm.TimeoutSaga = timeoutFunc
}

//GetTimeoutsTableName returns the table name in which to store timeouts
func getTimeoutsTableName(svcName string) string {

	var re = regexp.MustCompile(`-|;|\\|`)
	sanitized := re.ReplaceAllString(svcName, "")

	return strings.ToLower("grabbit_" + sanitized + "_timeouts")
}

//NewTimeoutManager creates a new instance of a mysql based TimeoutManager
func NewTimeoutManager(bus gbus.Bus, txp gbus.TxProvider, logger func() logrus.FieldLogger, svcName string, purge bool) *TimeoutManager {

	timeoutsTableName := getTimeoutsTableName(svcName)
	tm := &TimeoutManager{
		Log:               logger,
		Bus:               bus,
		Txp:               txp,
		SvcName:           svcName,
		timeoutsTableName: timeoutsTableName,
		exit:              make(chan bool)}

	if err := tm.ensureSchema(); err != nil {
		panic(err)
	}
	if purge {
		if err := tm.purge(); err != nil {
			panic(err)
		}
	}
	return tm

}
