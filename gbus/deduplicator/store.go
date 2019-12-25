package deduplicator

import (
	"database/sql"
)

// DeduplicatorStore abtracts the way deduplicateor manages the
type DeduplicatorStore interface {
	StoreMessageId(tx *sql.Tx, id string) error
	MessageExists(id string) (bool, error)
	Purge() error
	Start()
	Stop()
}
