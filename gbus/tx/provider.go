package tx

import (
	"context"
	"database/sql"
)

//Provider for PostgreSQL
type Provider struct {
	Database *sql.DB
}

//New PostgreSQL transaction
func (pg *Provider) New() (*sql.Tx, error) {
	ctx := context.Background()

	return pg.Database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})

}

//Dispose database connections
func (pg *Provider) Dispose() {
	pg.Database.Close()
}
