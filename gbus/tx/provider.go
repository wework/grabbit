package tx

import "database/sql"

//Provider provides a new Tx from the configured driver to the bus
type Provider interface {
	New() (*sql.Tx, error)
	Dispose()
}
