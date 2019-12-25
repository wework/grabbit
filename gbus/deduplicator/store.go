package deduplicator

import (
	"database/sql"
)

// Store abstracts the way deduplicateor manages the
type Store interface {
	StoreMessageID(tx *sql.Tx, id string) error
	MessageExists(id string) (bool, error)
	Purge() error
	Start()
	Stop()
}
