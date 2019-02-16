package policy

import "github.com/streadway/amqp"

//Durable sets the outgoing amqp message delivery-mode property to durable (2)
type Durable struct {
}

//Apply the policy on outgoing amqp message
func (*Durable) Apply(publishing *amqp.Publishing) {
	publishing.DeliveryMode = amqp.Persistent
}

//NonDurable sets the outgoing amqp message delivery-mode property to transient (1)
type NonDurable struct {
}

//Apply the policy on outgoing amqp message
func (*NonDurable) Apply(publishing *amqp.Publishing) {
	publishing.DeliveryMode = amqp.Transient
}
