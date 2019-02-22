package pg

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq" //blank import
	"github.com/rhinof/grabbit/gbus"
)

//Provider for PostgreSQL
type Provider struct {
	db *sql.DB
}

//New PostgreSQL transaction
func (pg *Provider) New() (*sql.Tx, error) {
	ctx := context.Background()

	return pg.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})

}

//Dispose database connections
func (pg *Provider) Dispose() {
	pg.db.Close()
}

//NewTxProvider returns a new PgProvider
func NewTxProvider(connStr string) (gbus.TxProvider, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	pg := Provider{
		db: db}
	return &pg, nil

}
