package tx

import (
	"context"
	"database/sql"
	"time"
)

//Provider for PostgreSQL or MySQ
type Provider struct {
	Database *sql.DB
}

//New transaction
func (provider *Provider) New() (*sql.Tx, error) {
	ctx := context.Background()

	return provider.Database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})

}

//Dispose database connections
func (provider *Provider) Dispose() {
	provider.Database.Close()
}

//Ping checks if the provider is available and timeouts according to the given duration
func (provider *Provider) Ping(timeoutInSeconds time.Duration) bool {

	ctx, cancel := context.WithTimeout(context.Background(), timeoutInSeconds*time.Second)

	defer cancel()

	hasPing := true
	if err := provider.Database.PingContext(ctx); err != nil {
		hasPing = false
	}

	return hasPing
}
