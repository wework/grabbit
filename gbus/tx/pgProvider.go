package tx

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq" //blank import
)

//PgProvider for PostgreSQL
type PgProvider struct {
	db *sql.DB
}

//New PostgreSQL transaction
func (pg *PgProvider) New() (*sql.Tx, error) {
	ctx := context.Background()

	return pg.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

}

//Dispose database connections
func (pg *PgProvider) Dispose() {
	pg.db.Close()
}

//NewPgProvider returns a new PgProvider
func NewPgProvider(connStr string) (Provider, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	pg := PgProvider{
		db: db}
	return &pg, nil

}
