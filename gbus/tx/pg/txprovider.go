package pg

import (
	"database/sql"

	_ "github.com/lib/pq" //blank import
	"github.com/rhinof/wework/grabbit/gbus"
	"github.com/rhinof/wework/grabbit/gbus/tx"
)

//NewTxProvider returns a new PgProvider
func NewTxProvider(connStr string) (gbus.TxProvider, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	pg := tx.Provider{
		Database: db}
	return &pg, nil

}
