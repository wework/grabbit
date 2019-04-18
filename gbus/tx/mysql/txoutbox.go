package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rs/xid"
	"github.com/streadway/amqp"
	"github.com/wework/grabbit/gbus"
)

var (
	pending        int
	waitingConfirm = 1
	confirmed      = 2
	//TODO:get these values from configuration
	maxPageSize         = 500
	maxDeliveryAttempts = 50
	sendInterval        = time.Second
	cleanupInterval     = time.Second * 30
	scavengeInterval    = time.Second * 60
)

//TxOutbox is a mysql based transactional outbox
type TxOutbox struct {
	svcName        string
	txProv         gbus.TxProvider
	purgeOnStartup bool
	ID             string
	amqpOutbox     *gbus.AMQPOutbox

	ack  chan uint64
	nack chan uint64
	exit chan bool
}

func (outbox *TxOutbox) log() *log.Entry {
	return log.WithField("_service", outbox.svcName)
}

//Start starts the transactional outbox that is used to send messages in sync with domain object change
func (outbox *TxOutbox) Start(amqpOut *gbus.AMQPOutbox) error {

	tx, e := outbox.txProv.New()
	if e != nil {
		panic(fmt.Sprintf("passed in transaction provider failed with the following error\n%s", e))
	}
	if ensureErr := outbox.ensureSchema(tx, outbox.svcName); ensureErr != nil {
		tx.Rollback()
		return ensureErr
	}
	if outbox.purgeOnStartup {
		if purgeErr := outbox.purge(tx); purgeErr != nil {
			outbox.log().WithError(purgeErr).Error("failed to purge transactional outbox")
			tx.Rollback()
			return purgeErr
		}
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return commitErr
	}
	outbox.amqpOutbox = amqpOut
	outbox.amqpOutbox.NotifyConfirm(outbox.ack, outbox.nack)

	go outbox.processOutbox()
	return nil
}

//Stop forcess the transactional outbox to stop processing additional messages
func (outbox *TxOutbox) Stop() error {
	outbox.exit <- true
	return nil
}

//Save stores a message in a DB to ensure delivery
func (outbox *TxOutbox) Save(tx *sql.Tx, exchange, routingKey string, amqpMessage amqp.Publishing) error {

	insertSQL := `INSERT INTO ` + getOutboxName(outbox.svcName) + ` (
							 message_id,
							 message_type,
							 delivery_tag,
							 exchange,
							 routing_key,
							 publishing,
							 status) VALUES(?, ?, ?, ?, ?, ?, ?)`

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(amqpMessage)

	if err != nil {
		return err
	}
	unknownDeliverTag := -1
	_, insertErr := tx.Exec(insertSQL, amqpMessage.MessageId, amqpMessage.Headers["x-msg-name"], unknownDeliverTag, exchange, routingKey, buf.Bytes(), pending)

	return insertErr
}

func (outbox *TxOutbox) purge(tx *sql.Tx) error {

	purgeSQL := `DELETE FROM  ` + getOutboxName(outbox.svcName)
	_, err := tx.Exec(purgeSQL)
	return err
}

//NewOutbox creates a new mysql transactional outbox
func NewOutbox(svcName string, txProv gbus.TxProvider, purgeOnStartup bool) *TxOutbox {

	txo := &TxOutbox{
		svcName:        svcName,
		txProv:         txProv,
		purgeOnStartup: purgeOnStartup,
		ID:             xid.New().String(),
		ack:            make(chan uint64, 10000),
		nack:           make(chan uint64, 10000),
		exit:           make(chan bool)}
	return txo
}

func (outbox *TxOutbox) processOutbox() {

	send := time.NewTicker(sendInterval).C
	cleanUp := time.NewTicker(cleanupInterval).C
	scavenge := time.NewTicker(scavengeInterval).C

	for {
		select {
		case <-outbox.exit:
			return
		//TODO:get time duration from configuration
		case <-send:
			err := outbox.sendMessages(outbox.getMessageRecords)
			if err != nil {
				outbox.log().WithError(err).Error("failed to send messages from outbox")
			}
		case <-cleanUp:
			err := outbox.deleteCompletedRecords()
			if err != nil {
				outbox.log().WithError(err).Error("failed to delete completed records")
			}
		case <-scavenge:
			err := outbox.sendMessages(outbox.scavengeOrphanedRecords)
			if err != nil {
				outbox.log().WithError(err).Error("failed to scavenge records")
			}
		case ack := <-outbox.ack:
			if err := outbox.updateAckedRecord(ack); err != nil {
				outbox.log().WithError(err).WithField("delivery_tag", ack).Error("failed to update delivery tag")
			}
		case nack := <-outbox.nack:
			outbox.log().WithField("deliver_tag", nack).Info("nack received for delivery tag")
		}
	}
}

func (outbox *TxOutbox) deleteCompletedRecords() error {

	tx, txErr := outbox.txProv.New()
	if txErr != nil {
		return txErr
	}
	deleteSQL := "DELETE FROM " + getOutboxName(outbox.svcName) + " WHERE status=?"
	result, execErr := tx.Exec(deleteSQL, confirmed)
	if execErr != nil {
		outbox.log().WithError(execErr).Error("failed to delete processed records")

		tx.Rollback()
		return execErr
	}

	commitErr := tx.Commit()
	records, ree := result.RowsAffected()
	if commitErr == nil && ree == nil && records > 0 {
		outbox.log().WithField("records", records).Info("cleaned records from outbox")
	}

	return commitErr
}

func (outbox *TxOutbox) updateAckedRecord(deliveryTag uint64) error {
	tx, txErr := outbox.txProv.New()
	if txErr != nil {
		outbox.log().WithError(txErr).WithField("delivery_tag", deliveryTag).Error("failed to create transaction for updating acked delivery tag")
		return txErr
	}
	outbox.log().WithField("delivery_tag", deliveryTag).Info("ack received for delivery tag")

	updateSQL := "UPDATE " + getOutboxName(outbox.svcName) + " SET status=? WHERE delivery_tag=? AND relay_id=?"
	_, execErr := tx.Exec(updateSQL, confirmed, deliveryTag, outbox.ID)
	if execErr != nil {
		outbox.log().WithError(execErr).
			WithFields(log.Fields{"delivery_tag": deliveryTag, "relay_id": outbox.ID}).
			Error("failed to update delivery tag")
		tx.Rollback()
	}
	return tx.Commit()
}

func (outbox *TxOutbox) getMessageRecords(tx *sql.Tx) (*sql.Rows, error) {
	selectSQL := "SELECT rec_id, exchange, routing_key, publishing FROM " + getOutboxName(outbox.svcName) + " WHERE status = 0 AND delivery_attempts < " + strconv.Itoa(maxDeliveryAttempts) + " ORDER BY rec_id ASC LIMIT " + strconv.Itoa(maxPageSize) + " FOR UPDATE SKIP LOCKED"
	return tx.Query(selectSQL)
}

func (outbox *TxOutbox) scavengeOrphanedRecords(tx *sql.Tx) (*sql.Rows, error) {
	selectSQL := "SELECT rec_id, exchange, routing_key, publishing FROM " + getOutboxName(outbox.svcName) + " WHERE status = 1  ORDER BY rec_id ASC LIMIT " + strconv.Itoa(maxPageSize) + " FOR UPDATE SKIP LOCKED"
	return tx.Query(selectSQL)
}

func (outbox *TxOutbox) sendMessages(recordSelector func(tx *sql.Tx) (*sql.Rows, error)) error {
	tx, txNewErr := outbox.txProv.New()

	if txNewErr != nil {
		outbox.log().WithError(txNewErr).Error("failed creating transaction for outbox")

		return txNewErr
	}

	rows, selectErr := recordSelector(tx)

	if selectErr != nil {
		outbox.log().WithError(selectErr).Error("failed fetching messages from outbox")

		return selectErr
	}

	successfulDeliveries := make(map[uint64]int, 0)
	failedDeliveries := make([]int, 0)

	for rows.Next() {
		var (
			recID                int
			exchange, routingKey string
			publishingBytes      []byte
		)
		if err := rows.Scan(&recID, &exchange, &routingKey, &publishingBytes); err != nil {
			outbox.log().WithError(err).Error("failed to scan outbox record")
		}

		//de-serialize the amqp message to send over the wire
		reader := bytes.NewReader(publishingBytes)
		dec := gob.NewDecoder(reader)
		var publishing amqp.Publishing
		decErr := dec.Decode(&publishing)

		if decErr != nil {
			outbox.log().WithError(decErr).Error("failed to decode amqp message from outbox record")
			continue
		}
		outbox.log().WithField("message_id", publishing.MessageId).Info("relay message")

		//send the amqp message to rabbitmq
		if deliveryTag, postErr := outbox.amqpOutbox.Post(exchange, routingKey, publishing); postErr != nil {

			outbox.log().WithError(postErr).
				WithFields(log.Fields{"message_name": publishing.Headers["x-msg-name"], "message_id": publishing.MessageId}).
				Error("failed to send amqp message")
			failedDeliveries = append(failedDeliveries, recID)
		} else {
			successfulDeliveries[deliveryTag] = recID
		}
	}
	rows.Close()

	for deliveryTag, id := range successfulDeliveries {
		_, updateErr := tx.Exec("UPDATE "+getOutboxName(outbox.svcName)+" SET status=1, delivery_tag=?, relay_id=? WHERE rec_id=?", deliveryTag, outbox.ID, id)
		if updateErr != nil {
			outbox.log().WithError(updateErr).
				WithFields(log.Fields{"record_id": id, "delivery_tag": deliveryTag, "relay_id": outbox.ID}).
				Warn("failed to update transactional outbox for message relay")

		}
	}

	for recid := range failedDeliveries {
		_, updateErr := tx.Exec("UPDATE "+getOutboxName(outbox.svcName)+" SET delivery_attempts=delivery_attempts+1  WHERE rec_id=?", recid)
		if updateErr != nil {
			outbox.log().WithError(updateErr).WithField("record_id", recid).Warn("failed to update transactional outbox with failed deivery attempt for record")
		}
	}
	if cmtErr := tx.Commit(); cmtErr != nil {
		outbox.log().WithError(cmtErr).Error("Error committing outbox transaction")
	}
	return nil
}

func (outbox *TxOutbox) ensureSchema(tx *sql.Tx, svcName string) error {

	schemaExists := outbox.outBoxTablesExists(tx, svcName)

	if schemaExists {
		/*
			The follwoing  performs an alter schema to accommodate for breaking change introduced in commit 6a9f5df
			so that earlier consumers of grabbit will not break once the upgrade to the 1.0.0 release.
			Once a proper DB migration stratagy will be in place and implemented (post 1.0.0) the following code
			will be deleted.
		*/

		return outbox.migrate0_9To1_0(tx, svcName)
	}

	createTablesSQL := `CREATE TABLE IF NOT EXISTS ` + getOutboxName(svcName) + ` (
	rec_id int NOT NULL AUTO_INCREMENT,
	message_id varchar(50) NOT NULL UNIQUE,
	message_type varchar(50) NOT NULL,
	exchange	varchar(50) NOT NULL,
	routing_key	varchar(50) NOT NULL,
	publishing	longblob NOT NULL,
	status	int(11) NOT NULL,
	relay_id varchar(50)  NULL,
	delivery_tag	bigint(20) NOT NULL,
	delivery_attempts int NOT NULL DEFAULT 0,
	insert_date	timestamp DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY(rec_id))`

	_, createErr := tx.Exec(createTablesSQL)

	return createErr

}

func (outbox *TxOutbox) outBoxTablesExists(tx *sql.Tx, svcName string) bool {

	tblName := getOutboxName(svcName)

	selectSQL := `SELECT 1 FROM ` + tblName + ` LIMIT 1;`

	outbox.log().Info(selectSQL)

	row := tx.QueryRow(selectSQL)
	var exists int
	err := row.Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false
	}

	return true
}

func (outbox *TxOutbox) migrate0_9To1_0(tx *sql.Tx, svcName string) error {
	tblName := getOutboxName(svcName)
	alter := `ALTER TABLE ` + tblName + ` CHANGE COLUMN delivery_attemtps delivery_attempts int NOT NULL DEFAULT 0;`
	_, execErr := tx.Exec(alter)
	if execErr != nil {
		outbox.log().WithField("sql_err", execErr).Info("renaming column")
	}
	return nil
}

func getOutboxName(svcName string) string {

	return strings.ToLower("grabbit_" + sanitizeTableName(svcName) + "_outbox")
}
