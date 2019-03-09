package mysql

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"strings"

	"github.com/rhinof/grabbit/gbus"
	"github.com/streadway/amqp"
)

//TxOutbox is a mysql based transactional outbox
type TxOutbox struct {
	*gbus.AMQPOutbox
	svcName string
	txProv  gbus.TxProvider
}

func (outbox *TxOutbox) Start() error {
	return nil
}

func (outbox *TxOutbox) Save(tx *sql.Tx, exchange, routingKey string, amqpMessage amqp.Publishing) error {

	insertSQL := `INSERT INTO ` + outbox.getOutboxName() + ` (delivery_tag, exchange, routing_key, delivery, status) VALUES(?, ?, ?, ?, ?)`

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(amqpMessage)

	if err != nil {
		return err
	}

	var pending uint = 0
	_, insertErr := tx.Exec(insertSQL, -1, exchange, routingKey, buf.Bytes(), pending)

	return insertErr
}

func (outbox *TxOutbox) purge(tx *sql.Tx) {

	purgeSQL := `TRUNCATE TABLE ` + outbox.getOutboxName()
	tx.Exec(purgeSQL)
}

func (outbox *TxOutbox) ensureSchema(tx *sql.Tx) {

	schemaExists := outbox.outBoxTablesExists(tx)

	if schemaExists {
		return
	}

	createTablesSQL := `CREATE TABLE IF NOT EXISTS ` + outbox.getOutboxName() + ` (
	rec_id int NOT NULL AUTO_INCREMENT,
	exchange	varchar(50) NULL,
	routing_key	varchar(50) NOT NULL,
	delivery	longblob NOT NULL,
	status	int(11) NOT NULL,
	delivery_tag	bigint(20) NOT NULL,
	insert_date	timestamp DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY(rec_id))`

	_, createErr := tx.Exec(createTablesSQL)

	if createErr != nil {
		panic(createErr)
	}

}

func (outbox *TxOutbox) outBoxTablesExists(tx *sql.Tx) bool {

	tblName := outbox.getOutboxName()

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

func (outbox *TxOutbox) getOutboxName() string {

	return strings.ToLower("grabbit_" + sanitizeTableName(outbox.svcName) + "_outbox")
}

//NewTxOutbox creates a new mysql transactional outbox
func NewTxOutbox(svcName string, amqpOut *gbus.AMQPOutbox, txProv gbus.TxProvider, purgeOnStartup bool) *TxOutbox {

	txo := &TxOutbox{
		amqpOut,
		svcName,
		txProv}
	tx, e := txProv.New()
	if e != nil {
		panic(fmt.Sprintf("passed in transaction provider failed with the following error\n%s", e))
	}
	txo.ensureSchema(tx)
	if purgeOnStartup {
		txo.purge(tx)
	}
	return txo
}
