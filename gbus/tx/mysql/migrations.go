package mysql

import (
	"database/sql"
	"strings"

	"github.com/lopezator/migrator"
	"github.com/wework/grabbit/gbus/tx"
)

func sagaStoreTableMigration(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "sagas")

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

func sagaStoreAddSagaCreatorDetails(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "sagas")

	addCreatorDetailsSQL := `ALTER TABLE ` + tblName + ` ADD COLUMN started_by_request_of_svc VARCHAR(2048) AFTER saga_data, ADD COLUMN started_by_request_of_saga VARCHAR(255) AFTER started_by_request_of_svc`

	return &migrator.Migration{
		Name: "add saga creator details columns",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(addCreatorDetailsSQL); err != nil {
				return err
			}
			return nil
		},
	}
}

func sagaStoreAddRPCIDDetails(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "sagas")

	addCreatorDetailsSQL := `ALTER TABLE ` + tblName + ` ADD COLUMN started_by_msg_id VARCHAR(50) AFTER started_by_request_of_svc, ADD COLUMN started_by_rpcid VARCHAR(50) AFTER started_by_msg_id`

	return &migrator.Migration{
		Name: "adding the started_by_msg_id and started_by_rpcid columns to the saga table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(addCreatorDetailsSQL); err != nil {
				return err
			}
			return nil
		},
	}
}

func sagaStoreAddCreatedAtDetails(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "sagas")

	addCreatorDetailsSQL := `ALTER TABLE ` + tblName + ` ADD COLUMN created_at DATETIME NOT NULL DEFAULT NOW() AFTER version`

	return &migrator.Migration{
		Name: "adding the created_at column to the saga table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(addCreatorDetailsSQL); err != nil {
				return err
			}
			return nil
		},
	}
}

func outboxMigrations(svcName string) *migrator.Migration {

	tblName := tx.GrabbitTableNameTemplate(svcName, "outbox")

	query := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
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

func outboxChangeColumnLength(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "outbox")
	increaseLengthSQL := `ALTER TABLE ` + tblName + ` MODIFY message_type VARCHAR(2048) NOT NULL, MODIFY exchange VARCHAR(2048) NOT NULL, MODIFY routing_key VARCHAR(2048) NOT NULL`
	return &migrator.Migration{
		Name: "increase column length to 2048",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(increaseLengthSQL); err != nil {
				return err
			}
			return nil
		},
	}
}

func timoutTableMigration(svcName string) *migrator.Migration {
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

func legacyMigrationsTable(svcName string) *migrator.Migration {

	query := `DROP TABLE IF EXISTS grabbitmigrations_` + sanitizeSvcName(svcName)

	return &migrator.Migration{
		Name: "drop legacy migrations table",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(query); err != nil {
				return err
			}
			return nil
		},
	}
}

//EnsureSchema implements Grabbit's migrations strategy
func EnsureSchema(db *sql.DB, svcName string) {

	tblName := tx.GrabbitTableNameTemplate(svcName, "migrations")

	migrate, err := migrator.New(migrator.TableName(tblName), migrator.Migrations(
		outboxMigrations(svcName),
		sagaStoreTableMigration(svcName),
		timoutTableMigration(svcName),
		legacyMigrationsTable(svcName),
		outboxChangeColumnLength(svcName),
		sagaStoreAddSagaCreatorDetails(svcName),
		sagaStoreAddRPCIDDetails(svcName),
		sagaStoreAddCreatedAtDetails(svcName),
		createDeduplicationTable(svcName),
	))
	if err != nil {
		panic(err)
	}
	err = migrate.Migrate(db)
	if err != nil {
		panic(err)
	}
}

func createDeduplicationTable(svcName string) *migrator.Migration {
	tblName := tx.GrabbitTableNameTemplate(svcName, "duplicates")

	createTableQuery := `CREATE TABLE IF NOT EXISTS ` + tblName + ` (
		id VARCHAR(256) NOT NULL PRIMARY KEY,
		created_at	timestamp DEFAULT CURRENT_TIMESTAMP,
		INDEX ` + tblName + `_created_at_idx (created_at))`

	return &migrator.Migration{
		Name: "create a table to manage duplicate messages",
		Func: func(tx *sql.Tx) error {
			if _, err := tx.Exec(createTableQuery); err != nil {
				return err
			}
			return nil
		},
	}
}

func sanitizeSvcName(svcName string) string {

	sanitized := tx.SanitizeTableName(svcName)
	return strings.ToLower(sanitized)
}
