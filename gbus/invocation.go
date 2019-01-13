package gbus

import "database/sql"

//Invocation context for a specific processed message
type Invocation interface {
	Reply(message BusMessage)
	Bus() Messaging
	Tx() *sql.Tx
}
