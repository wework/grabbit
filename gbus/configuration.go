package gbus

import "time"

//BusConfiguration provides configuration passed to the bus builder
type BusConfiguration struct {
	MaxRetryCount     uint
	BaseRetryDuration int //TODO:Change type to uint
	OutboxCfg         OutboxConfiguration
}

//OutboxConfiguration configures the transactional outbox
type OutboxConfiguration struct {
	//PageSize is the amount of pending messsage records the outbox selects from the database every iteration, the default is 500
	PageSize uint
	//SendInterval is the duration the outbox waits before each iteration, default is 1 second
	SendInterval time.Duration
	/*
		ScavengeInterval is the duration the outbox waits before attempting to re-send messages that
		were already sent to the broker but were not yet confirmed.
		Default is 60 seconds
	*/
	ScavengeInterval time.Duration
	/*
		Ackers the number of goroutines configured to drain incoming ack/nack signals from the broker.
		Increase this value if you are experiencing deadlocks.
		Default is 10
	*/

	Ackers uint
}
