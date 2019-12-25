package implementation

import (
	"database/sql"
	"time"

	"emperror.dev/errors"
	"github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/deduplicator"
	"github.com/wework/grabbit/gbus/tx"
)

var _ deduplicator.Store = &deduper{}

type deduper struct {
	*gbus.Glogged
	svcName    string
	policy     gbus.DeduplicationPolicy
	txProvider gbus.TxProvider
	age        time.Duration
	ticker     *time.Ticker
	done       chan bool
	tableName  string
}

func (d *deduper) Purge() (err error) {
	truncateSQL := "TRUNCATE TABLE " + d.tableName
	txp, err := d.txProvider.New()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			serr := txp.Rollback()
			err = errors.Append(err, serr)
		}
		err = txp.Commit()
	}()
	_, err = txp.Exec(truncateSQL)
	if err != nil {
		return err
	}
	return nil
}

func (d *deduper) Start() {
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
					d.Log().WithError(err).Error("failed to acquire a tx")
					continue
				}
				result, err := tx.Exec(deleteQuery, oldest)
				if err != nil && err != sql.ErrNoRows {
					d.Log().WithError(err).Error("failed executing delete query")
				}
				n, err := result.RowsAffected()
				if err != nil {
					d.Log().WithError(err).Error("failed to get count of affected rows")
				} else {
					d.Log().WithField("table_name", d.tableName).WithField("rows_deleted", n).
						Info("successfully cleanup duplicates table")
				}
			}
		}
	}()
}

func (d *deduper) Stop() {
	d.Log().Info("shutting down deduplicator")
	d.ticker.Stop()
	close(d.done)
}

//
func (d *deduper) StoreMessageID(tx *sql.Tx, id string) error {
	insertSQL := "INSERT INTO " + d.tableName + " (id) values (?)"
	_, err := tx.Exec(insertSQL, id)
	if err != nil {
		d.Log().WithError(err).Error("failed to insert the id of the message into the dedup table")
		return err
	}
	return nil
}

// MessageExists checks if a message id is in the deduplication table and returns an error if it fails
func (d *deduper) MessageExists(id string) (bool, error) {
	if d.policy == gbus.DeduplicationPolicyNone {
		return false, nil
	}
	tx, err := d.txProvider.New()
	if err != nil {
		return true, err
	}
	defer func() {
		err = tx.Rollback()
		if err != nil {
			d.Log().WithError(err).Error("could not commit tx for query MessageExists")
		}
	}()
	selectSQL := "SELECT EXISTS (SELECT id FROM " + d.tableName + " WHERE id = ? limit 1)"

	var exists bool
	err = tx.QueryRow(selectSQL, id).Scan(&exists)
	if err != nil && err == sql.ErrNoRows {
		return false, nil
	}

	if err != nil {
		return true, err
	}

	return true, nil
}

func NewDeduplicator(svcName string, policy gbus.DeduplicationPolicy, txProvider gbus.TxProvider, age time.Duration, logger logrus.FieldLogger) deduplicator.Store {
	d := &deduper{
		svcName:    svcName,
		policy:     policy,
		txProvider: txProvider,
		age:        age,
		tableName:  tx.GrabbitTableNameTemplate(svcName, "duplicates"),
	}
	l := logger.WithField("grabbit", "deduplicator")
	d.SetLogger(l)
	return d
}
