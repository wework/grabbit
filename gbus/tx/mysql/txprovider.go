package mysql

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql" //blank import
	"github.com/wework/grabbit/gbus"
	"github.com/wework/grabbit/gbus/tx"
)

//NewTxProvider returns a new PgProvider
func NewTxProvider(connStr string) (gbus.TxProvider, error) {
	db, err := sql.Open("mysql", connStr)
	db.SetConnMaxLifetime(time.Second)
	if err != nil {
		return nil, err
	}
	connErr := db.Ping()
	if connErr != nil {
		return nil, connErr
	}
	provider := tx.Provider{
		Database: db}
	return &provider, nil

}
