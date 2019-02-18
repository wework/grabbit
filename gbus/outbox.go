package gbus

import (
	"log"
	"sync"

	"github.com/streadway/amqp"
)

//AMQPOutbox sends messages to the amqp transport
type AMQPOutbox struct {
	channel  *amqp.Channel
	confirm  bool
	sequence uint64
	ack      chan uint64
	nack     chan uint64
	resends  chan pendingConfirmation
	locker   *sync.Mutex
	pending  map[uint64]pendingConfirmation
	active   bool
}

func (out *AMQPOutbox) init(amqp *amqp.Channel, confirm bool) error {
	out.active = true
	out.pending = make(map[uint64]pendingConfirmation)
	out.locker = &sync.Mutex{}
	out.channel = amqp
	out.confirm = confirm
	if confirm {

		out.ack = make(chan uint64, 1000000)
		out.nack = make(chan uint64, 1000000)
		out.resends = make(chan pendingConfirmation)

		err := out.channel.Confirm(false /*noWait*/)
		out.channel.NotifyConfirm(out.ack, out.nack)

		if err != nil {
			return err
		}
		go out.confirmationLoop()
	}

	return nil
}

func (out *AMQPOutbox) shutdown() {
	out.active = false

}

//Send implements Outbox.Send
func (out *AMQPOutbox) send(exchange, routingKey string, amqpMessage amqp.Publishing) error {

	if out.confirm {
		out.locker.Lock()
		out.sequence++

		p := pendingConfirmation{
			exchange:    exchange,
			routingKey:  routingKey,
			amqpMessage: amqpMessage}
		out.pending[out.sequence] = p
		out.locker.Unlock()
		return out.sendToChannel(exchange, routingKey, amqpMessage)
	}
	return out.sendToChannel(exchange, routingKey, amqpMessage)
}

func (out *AMQPOutbox) confirmationLoop() {

	for out.active {
		select {
		case ack := <-out.ack:
			if ack <= 0 {
				continue
			}
			out.locker.Lock()
			pending := out.pending[ack]
			if pending.deliveryTag > 0 {
				log.Printf("ack recived for a pending delivery with tag %v", ack)
			}
			delete(out.pending, ack)
			out.locker.Unlock()
		case nack := <-out.nack:
			if nack <= 0 {
				continue
			}
			log.Printf("nack recived for delivery tag %v", nack)
			out.locker.Lock()
			pending := out.pending[nack]
			pending.deliveryTag = nack
			out.resends <- pending
			delete(out.pending, nack)
			out.locker.Unlock()
		case resend := <-out.resends:
			out.send(resend.exchange, resend.routingKey, resend.amqpMessage)
		}

	}
}

func (out *AMQPOutbox) sendToChannel(exchange, routingKey string, amqpMessage amqp.Publishing) error {

	return out.channel.Publish(exchange, /*exchange*/
		routingKey, /*key*/
		false,      /*mandatory*/
		false,      /*immediate*/
		amqpMessage /*msg*/)
}

func NewAMQPOutbox(amqp *amqp.Channel, confirm bool) (*AMQPOutbox, error) {
	outbox := &AMQPOutbox{}
	e := outbox.init(amqp, confirm)
	return outbox, e
}

type pendingConfirmation struct {
	deliveryTag uint64
	exchange    string
	routingKey  string
	amqpMessage amqp.Publishing
}
