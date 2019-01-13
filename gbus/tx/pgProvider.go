package tx

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type PgProvider struct {
	db *sql.DB
}

func (pg *PgProvider) New() (*sql.Tx, error) {
	ctx := context.Background()

	return pg.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

}

func (pg *PgProvider) Dispose() {
	pg.db.Close()
}

func NewPgProvider(connStr string) (Provider, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	pg := PgProvider{
		db: db}
	return &pg, nil

}
