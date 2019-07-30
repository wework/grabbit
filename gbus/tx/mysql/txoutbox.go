package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"github.com/lopezator/migrator"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/wework/grabbit/gbus"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	pending int
	//waitingConfirm = 1

	//TODO:get these values from configuration
	maxPageSize         = 500
	maxDeliveryAttempts = 50
	sendInterval        = time.Second

	scavengeInterval = time.Second * 60
	ackers           = 10
)

//TxOutbox is a mysql based transactional outbox
type TxOutbox struct {
	svcName                string
	txProv                 gbus.TxProvider
	purgeOnStartup         bool
	ID                     string
	amqpOutbox             *gbus.AMQPOutbox
	recordsPendingConfirms map[uint64]int
	ack                    chan uint64
	nack                   chan uint64
	exit                   chan bool
	gl                     *sync.Mutex
}

func (outbox *TxOutbox) log() *log.Entry {
	return log.WithField("tx", "mysql")
}

//Start starts the transactional outbox that is used to send messages in sync with domain object change
func (outbox *TxOutbox) Start(amqpOut *gbus.AMQPOutbox) error {
	outbox.gl = &sync.Mutex{}
	outbox.recordsPendingConfirms = make(map[uint64]int)
	tx, e := outbox.txProv.New()
	if e != nil {
		panic(fmt.Sprintf("passed in transaction provider failed with the following error\n%s", e))
	}
	if ensureErr := outbox.ensureSchema(outbox.txProv.GetDb(), outbox.svcName); ensureErr != nil {
		err := tx.Rollback()
		if err != nil {
			outbox.log().WithError(err).Error("could not rollback the transaction for creation of schemas")
		}
		return ensureErr
	}
	if outbox.purgeOnStartup {
		if purgeErr := outbox.purge(tx); purgeErr != nil {
			outbox.log().WithError(purgeErr).Error("failed to purge transactional outbox")
			err := tx.Rollback()
			if err != nil {
				outbox.log().WithError(err).Error("could not rollback the transaction for purge")
			}
			return purgeErr
		}
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return commitErr
	}
	outbox.amqpOutbox = amqpOut
	outbox.amqpOutbox.NotifyConfirm(outbox.ack, outbox.nack)

	go outbox.processOutbox()
	for i := 0; i < ackers; i++ {
		go outbox.ackRec()
	}

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

	purgeSQL := fmt.Sprintf("DELETE FROM %s", getOutboxName(outbox.svcName))
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
		ack:            make(chan uint64, 1000000),
		nack:           make(chan uint64, 1000000),
		exit:           make(chan bool)}
	return txo
}

func (outbox *TxOutbox) ackRec() {
	for {
		select {
		case <-outbox.exit:
			return
		case ack := <-outbox.ack:
			outbox.log().WithField("channel_len", len(outbox.ack)).Debug("length of ack channel")
			if err := outbox.updateAckedRecord(ack); err != nil {
				outbox.log().WithError(err).WithField("delivery_tag", ack).Error("failed to update delivery tag")
			}
		case nack := <-outbox.nack:
			outbox.log().WithField("deliver_tag", nack).Info("nack received for delivery tag")
			outbox.log().WithField("channel_len", len(outbox.nack)).Debug("length of nack channel")

		}
	}
}

func (outbox *TxOutbox) processOutbox() {

	send := time.NewTicker(sendInterval).C
	// cleanUp := time.NewTicker(cleanupInterval).C
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

		case <-scavenge:
			err := outbox.sendMessages(outbox.scavengeOrphanedRecords)
			if err != nil {
				outbox.log().WithError(err).Error("failed to scavenge records")
			}
		}
	}
}

func (outbox *TxOutbox) updateAckedRecord(deliveryTag uint64) error {
	tx, txErr := outbox.txProv.New()
	if txErr != nil {
		outbox.log().WithError(txErr).WithField("delivery_tag", deliveryTag).Error("failed to create transaction for updating acked delivery tag")
		return txErr
	}
	outbox.log().WithField("delivery_tag", deliveryTag).Debug("ack received for delivery tag")

	outbox.gl.Lock()
	recID := outbox.recordsPendingConfirms[deliveryTag]
	outbox.gl.Unlock()
	/*
			since the messages get sent to rabbitmq and then the outbox table gets updated with the deilvery tag for teh record
			it may be that we recived a acked deliveryTag that is not yet registered in the outbox table.
		  in that case we just place the deliveryTag back in the ack channel so it can be picked up and re processed later
		  we place it in the channel using a new goroutine so to not deadlock if there is only a single goroutine draining the ack channel
	*/
	if recID == 0 {
		go func() { outbox.ack <- deliveryTag }()
	}

	deleteSQL := "DELETE FROM " + getOutboxName(outbox.svcName) + "  WHERE rec_id=?"
	_, execErr := tx.Exec(deleteSQL, recID)
	if execErr != nil {
		outbox.log().WithError(execErr).
			WithFields(log.Fields{"delivery_tag": deliveryTag, "relay_id": outbox.ID}).
			Error("failed to update delivery tag")
		err := tx.Rollback()
		if err != nil {
			outbox.log().WithError(err).Error("could not rollback update in outbox")
		}
	}
	return tx.Commit()
}

func (outbox *TxOutbox) getMessageRecords(tx *sql.Tx) (*sql.Rows, error) {
	selectSQL := "SELECT rec_id, exchange, routing_key, publishing FROM " + getOutboxName(outbox.svcName) + " USE INDEX (status_delivery) WHERE status = 0 AND delivery_attempts < " + strconv.Itoa(maxDeliveryAttempts) + " ORDER BY rec_id ASC LIMIT " + strconv.Itoa(maxPageSize) + " FOR UPDATE SKIP LOCKED"
	return tx.Query(selectSQL)
}

func (outbox *TxOutbox) scavengeOrphanedRecords(tx *sql.Tx) (*sql.Rows, error) {
	selectSQL := "SELECT rec_id, exchange, routing_key, publishing FROM " + getOutboxName(outbox.svcName) + " WHERE status = 1  ORDER BY rec_id ASC LIMIT ? FOR UPDATE SKIP LOCKED"
	return tx.Query(selectSQL, strconv.Itoa(maxPageSize))
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
		err := rows.Close()
		if err != nil {
			outbox.log().WithError(err).Error("could not close Rows")
		}
		return selectErr
	}

	successfulDeliveries := make(map[uint64]int)
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

		//send the amqp message to rabbitmq
		if deliveryTag, postErr := outbox.amqpOutbox.Post(exchange, routingKey, publishing); postErr != nil {
			outbox.log().WithError(postErr).
				WithFields(log.Fields{"message_name": publishing.Headers["x-msg-name"], "message_id": publishing.MessageId}).
				Error("failed to send amqp message")
			failedDeliveries = append(failedDeliveries, recID)
		} else {
			outbox.log().WithFields(log.Fields{"message_id": publishing.MessageId, "delivery_tag": deliveryTag}).Debug("relay message")
			successfulDeliveries[deliveryTag] = recID
		}

	}
	err := rows.Close()
	if err != nil {
		outbox.log().WithError(err).Error("could not close Rows")
	}
	if messagesSent := len(successfulDeliveries); messagesSent > 0 {
		outbox.log().WithField("messages_sent", len(successfulDeliveries)).Info("outbox relayed messages")
	}
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
	} else {
		//only after the tx has commited successfully add the recordids so they can be picked up by confirms
		outbox.gl.Lock()
		defer outbox.gl.Unlock()
		for deliveryTag, recID := range successfulDeliveries {
			outbox.recordsPendingConfirms[deliveryTag] = recID
		}
	}

	return nil
}

func (outbox *TxOutbox) ensureSchema(db *sql.DB, svcName string) error {

	migrationsTable := "grabbitMigrations"

	createOutboxTablesSQL := `CREATE TABLE IF NOT EXISTS ` + getOutboxName(svcName) + ` (
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
	PRIMARY KEY(rec_id),
	INDEX status_delivery (rec_id, status, delivery_attempts))`

	migrate := migrator.NewNamed(migrationsTable,
		&migrator.Migration{
			Name: "create outbox table",
			Func: func(tx *sql.Tx) error {
				if _, err := tx.Exec(createOutboxTablesSQL); err != nil {
					return err
				}
				return nil
			},
		},
	)
	err := migrate.Migrate(db)
	if err != nil {
		outbox.log().WithField("sql_err", err).Info("migration error")
	}

	return err

}

func getOutboxName(svcName string) string {

	return strings.ToLower("grabbit_" + sanitizeTableName(svcName) + "_outbox")
}
