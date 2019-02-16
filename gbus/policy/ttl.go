package policy

import (
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

//TTL sets the ttl for the outgoing amqp message
type TTL struct {
	Duration time.Duration
}

//Apply the policy on outgoing amqp message
func (ttl *TTL) Apply(publishing *amqp.Publishing) {

	ms := int64(ttl.Duration / time.Millisecond)
	publishing.Headers["x-message-ttl"] = strconv.FormatInt(ms, 10)
}
