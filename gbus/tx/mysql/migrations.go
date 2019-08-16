package mysql

import (
	"database/sql"

	"fmt"
	"github.com/lopezator/migrator"
	"github.com/wework/grabbit/gbus/tx"
)

func SagaStoreTableMigration(svcName string) *migrator.Migration {
	tblName := tx.GetSagatableName(svcName)

	createTableQuery := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
		rec_id INT PRIMARY KEY AUTO_INCREMENT,
		saga_id VARCHAR(255) UNIQUE NOT NULL,
		saga_type VARCHAR(255)  NOT NULL,
		saga_data LONGBLOB NOT NULL,
		version integer NOT NULL DEFAULT 0,
		last_update timestamp  DEFAULT NOW(),
		INDEX ` + tblName + `_sagatype_idx (saga_type))`

	return &migrator.Migration{
		Name: "create saga store table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(createTableQuery); err != nil {
				return err
			}
			return nil
		},
	}
}

func OutboxMigrations(svcName string) *migrator.Migration {

	query := `CREATE TABLE IF NOT EXISTS ` + getOutboxName(svcName) + ` (
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

	return &migrator.Migration{
		Name: "create outbox table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(query); err != nil {
				return err
			}
			return nil
		},
	}
}

func TimoutTableMigration(svcName string) *migrator.Migration {
	tblName := GetTimeoutsTableName(svcName)

	createTableQuery := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
      rec_id INT PRIMARY KEY AUTO_INCREMENT,
      saga_id VARCHAR(255) UNIQUE NOT NULL,
	  timeout DATETIME NOT NULL,
	  INDEX (timeout),
	  INDEX (saga_id)
	 )`

	return &migrator.Migration{
		Name: "create timeout table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(createTableQuery); err != nil {
				return err
			}
			return nil
		},
	}
}

func EnsureSchema(db *sql.DB, svcName string) {
	migrationsTable := fmt.Sprintf("grabbitMigrations_%s", svcName)

	migrate, err := migrator.New(migrator.TableName(migrationsTable), migrator.Migrations(
		OutboxMigrations(svcName),
		SagaStoreTableMigration(svcName),
		TimoutTableMigration(svcName),
	))
	if err != nil {
		panic(err)
	}
	err = migrate.Migrate(db)
	if err != nil {
		panic(err)
	}
}
