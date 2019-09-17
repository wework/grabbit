package policy

import "github.com/streadway/amqp"

type Generic struct {
	Funk func(publishing *amqp.Publishing)
}

func (g *Generic) Apply(publishing *amqp.Publishing) {
	g.Funk(publishing)
}
