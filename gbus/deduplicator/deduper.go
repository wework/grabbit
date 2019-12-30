package deduplicator

import (
	"database/sql"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/tx"
)

var _ gbus.Deduplicator = &dedup{}

type dedup struct {
	svcName        string
	policy         gbus.DeduplicationPolicy
	txProvider     gbus.TxProvider
	age            time.Duration
	ticker         *time.Ticker
	done           chan bool
	tableName      string
	started        bool
	startStopMutex sync.Mutex
}

func (d *dedup) Purge(logger logrus.FieldLogger) (err error) {
	truncateSQL := "TRUNCATE TABLE " + d.tableName
	txp, err := d.txProvider.New()
	if err != nil {
		logger.WithError(err).WithField("table_name", d.tableName).Error("failed purging duplicates table")
		return err
	}
	defer func() {
		if err != nil {
			serr := txp.Rollback()
			logger.WithError(serr).Error("failed rolling back transaction after purge")
			err = errors.Append(err, serr)
		}
		err = txp.Commit()
	}()
	_, err = txp.Exec(truncateSQL)
	if err != nil {
		logger.WithError(err).WithField("table_name", d.tableName).Error("failed executing truncate on table")
		return err
	}
	logger.WithField("table_name", d.tableName).Info("successfully truncated table")
	return nil
}

func (d *dedup) Start(l logrus.FieldLogger) {
	d.startStopMutex.Lock()
	defer d.startStopMutex.Unlock()
	logger := d.decoratedLog(l)
	d.ticker = time.NewTicker(time.Minute)
	d.done = make(chan bool)
	deleteQuery := "DELETE FROM " + d.tableName + " WHERE `created_at` < ?"
	go func() {
		for {
			select {
			case <-d.done:
				return
			case <-d.ticker.C:
				oldest := time.Now().Add(-1 * d.age)
				tx, err := d.txProvider.New()
				if err != nil {
					logger.WithError(err).Error("failed to acquire a tx")
					continue
				}
				result, err := tx.Exec(deleteQuery, oldest)
				if err != nil && err != sql.ErrNoRows {
					logger.WithError(err).Error("failed executing delete query")
					continue
				}
				n, err := result.RowsAffected()
				if err != nil {
					logger.WithError(err).Error("failed to get count of affected rows")
				} else {
					logger.WithField("table_name", d.tableName).WithField("rows_deleted", n).
						Info("successfully cleanup duplicates table")
				}
			}
		}
	}()
	d.started = true
}

func (d *dedup) decoratedLog(l logrus.FieldLogger) logrus.FieldLogger {
	logger := l.WithField("grabbit", "dedup")
	return logger
}

func (d *dedup) Stop(logger logrus.FieldLogger) {
	d.decoratedLog(logger).Info("shutting down deduplicator")
	d.startStopMutex.Lock()
	defer d.startStopMutex.Unlock()
	if d.started {
		d.ticker.Stop()
		close(d.done)
		d.started = false
	}
}

//
func (d *dedup) StoreMessageID(logger logrus.FieldLogger, tx *sql.Tx, id string) error {
	insertSQL := "INSERT INTO " + d.tableName + " (id) values (?)"
	_, err := tx.Exec(insertSQL, id)
	if err != nil {
		d.decoratedLog(logger).WithError(err).Error("failed to insert the id of the message into the dedup table")
		return err
	}
	return nil
}

// MessageIDExists checks if a message id is in the deduplication table and returns an error if it fails
func (d *dedup) MessageIDExists(l logrus.FieldLogger, id string) (bool, error) {
	logger := d.decoratedLog(l)
	if d.policy == gbus.DeduplicationPolicyNone {
		logger.Debug("duplication policy is none")
		return false, nil
	}
	tx, err := d.txProvider.New()
	if err != nil {
		logger.WithError(err).Error("failed getting tx from txProvider")
		return true, err
	}
	defer func() {
		err = tx.Rollback()
		if err != nil {
			logger.WithError(err).Error("could not commit tx for query MessageIDExists")
		}
	}()
	selectSQL := "SELECT EXISTS (SELECT id FROM " + d.tableName + " WHERE id = ? limit 1)"

	var exists bool
	err = tx.QueryRow(selectSQL, id).Scan(&exists)
	if err != nil && err == sql.ErrNoRows {
		logger.WithField("table_name", d.tableName).Debug("no rows in result set when looking for messages in duplicates table")
		return false, nil
	}

	if err != nil {
		logger.WithError(err).WithField("table_name", d.tableName).Error("failed executing lookup query in duplicates table")
		return true, err
	}

	return exists, nil
}

func New(svcName string, policy gbus.DeduplicationPolicy, txProvider gbus.TxProvider, age time.Duration) gbus.Deduplicator {
	d := &dedup{
		svcName:    svcName,
		policy:     policy,
		txProvider: txProvider,
		age:        age,
		tableName:  tx.GrabbitTableNameTemplate(svcName, "duplicates"),
		started:    false,
	}
	return d
}
