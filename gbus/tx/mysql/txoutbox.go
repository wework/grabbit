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

	"github.com/rhinof/grabbit/gbus"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

var (
	pending        int
	waitingConfirm = 1
	confirmed      = 2
	//TODO:get these values from configuration
	maxPageSize         = 500
	maxDeliveryAttempts = 50
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

//Start starts the transactional outbox that is used to send messages in sync with domain object change
func (outbox *TxOutbox) Start(amqpOut *gbus.AMQPOutbox) error {

	tx, e := outbox.txProv.New()
	if e != nil {
		panic(fmt.Sprintf("passed in transaction provider failed with the following error\n%s", e))
	}
	if ensureErr := ensureSchema(tx, outbox.svcName); ensureErr != nil {
		tx.Rollback()
		return ensureErr
	}
	if outbox.purgeOnStartup {
		if purgeErr := outbox.purge(tx); purgeErr != nil {
			log.Errorf("%v failed to purge transactional outbox %v", outbox.svcName, purgeErr)
			tx.Rollback()
			return purgeErr
		}
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return commitErr
	}
	outbox.amqpOutbox = amqpOut
	outbox.amqpOutbox.NotifyConfirm(outbox.ack, outbox.nack)
	go outbox.cleanOutbox()
	go outbox.processOutbox()
	go outbox.scavenge()
	return nil
}

//Stop forcess the transactional outbox to stop processing additional messages
func (outbox *TxOutbox) Stop() error {
	close(outbox.exit)
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

	for {
		select {
		case <-outbox.exit:
			return
		//TODO:get time duration from configuration
		case <-time.After(time.Second * 1):
			err := outbox.sendMessages(outbox.getMessageRecords)
			if err != nil {
				log.Errorf("%v failed to processOutbox %v", outbox.svcName, err)
			}
		case ack := <-outbox.ack:
			outbox.updateAckedRecord(ack)
		case nack := <-outbox.nack:
			log.Printf("%v nack received for delivery tag %v", outbox.svcName, nack)
		}
	}
}

func (outbox *TxOutbox) cleanOutbox() {
	for {
		select {
		case <-outbox.exit:
			return
			//TODO:get time duration from configuration
		case <-time.After(time.Second * 30):
			err := outbox.deleteCompletedRecords()
			if err != nil {
				log.Errorf("%v failed to delete completed records %v", outbox.svcName, err)
			}
		}
	}
}

func (outbox *TxOutbox) scavenge() {

	for {
		select {
		case <-outbox.exit:
			return
			//TODO:get time duration from configuration
		case <-time.After(time.Second * 20):
			err := outbox.sendMessages(outbox.scavengeOrphanedRecords)
			if err != nil {
				log.Errorf("%v failed to scavenge records %v", outbox.svcName, err)
			}
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
		log.Errorf("%v failed to delete processed records %v", outbox.svcName, execErr)
		tx.Rollback()
		return execErr
	}

	commitErr := tx.Commit()
	records, ree := result.RowsAffected()
	if commitErr == nil && ree == nil && records > 0 {
		log.Printf("%v cleaned %v records from outbox", outbox.svcName, records)
	}

	return commitErr
}

func (outbox *TxOutbox) updateAckedRecord(deliveryTag uint64) error {
	tx, txErr := outbox.txProv.New()
	if txErr != nil {
		return txErr
	}
	log.Printf("ack received for delivery tag %v", deliveryTag)
	updateSQL := "UPDATE " + getOutboxName(outbox.svcName) + " SET status=? WHERE relay_id=? AND delivery_tag=?"
	_, execErr := tx.Exec(updateSQL, confirmed, outbox.ID, deliveryTag)
	if execErr != nil {
		log.Errorf("%v failed to update delivery tag %v for relay_id %v error:%v", outbox.svcName, deliveryTag, outbox.ID, execErr)
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
		log.Errorf("%v failed creating transaction for outbox.\n%s", outbox.svcName, txNewErr)
		return txNewErr
	}

	rows, selectErr := recordSelector(tx)

	if selectErr != nil {
		log.Errorf("%v failed fetching messages from outbox.\n%s", outbox.svcName, selectErr)
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
			log.Errorf("%v failed to scan outbox record\n%s", outbox.svcName, err)

		}

		//de-serialize the amqp message to send over the wire
		reader := bytes.NewReader(publishingBytes)
		dec := gob.NewDecoder(reader)
		var publishing amqp.Publishing
		decErr := dec.Decode(&publishing)

		if decErr != nil {
			log.Errorf("%v failed to decode amqp message from outbox record \n%v", outbox.svcName, decErr)
			continue
		}
		log.Printf("%v relay message %v", outbox.svcName, publishing.MessageId)

		//send the amqp message to rabbitmq
		if deliveryTag, postErr := outbox.amqpOutbox.Post(exchange, routingKey, publishing); postErr != nil {
			log.Warnf("%v failed to send amqp message %v with id %v.\n%v", outbox.svcName, publishing.Headers["x-msg-name"], publishing.MessageId, postErr)
			failedDeliveries = append(failedDeliveries, recID)
		} else {
			successfulDeliveries[deliveryTag] = recID
		}
	}
	rows.Close()

	for deliveryTag, id := range successfulDeliveries {
		_, updateErr := tx.Exec("UPDATE "+getOutboxName(outbox.svcName)+" SET status=1, delivery_tag=?, relay_id=? WHERE rec_id=?", deliveryTag, outbox.ID, id)
		if updateErr != nil {
			log.Printf("failed to update transactional outbox with delivery tag %v for message %v\n%v", 111, id, updateErr)
		}
	}

	for recid := range failedDeliveries {
		_, updateErr := tx.Exec("UPDATE "+getOutboxName(outbox.svcName)+" SET delivery_attempts=delivery_attempts+1  WHERE rec_id=?", recid)
		if updateErr != nil {
			log.Printf("failed to update transactional outbox with failed deivery attempt for record %v\n%v", recid, updateErr)
		}
	}
	if cmtErr := tx.Commit(); cmtErr != nil {
		log.Printf("Error committing outbox transaction %v", cmtErr)
	}
	return nil
}

func ensureSchema(tx *sql.Tx, svcName string) error {

	schemaExists := outBoxTablesExists(tx, svcName)

	if schemaExists {
		return nil
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

func outBoxTablesExists(tx *sql.Tx, svcName string) bool {

	tblName := getOutboxName(svcName)

	selectSQL := `SELECT 1 FROM ` + tblName + ` LIMIT 1;`

	log.Println(selectSQL)

	row := tx.QueryRow(selectSQL)
	var exists int
	err := row.Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return false
	}

	return true
}

func getOutboxName(svcName string) string {

	return strings.ToLower("grabbit_" + sanitizeTableName(svcName) + "_outbox")
}
