package gbus

import (
	"log"
	"sync"

	"github.com/streadway/amqp"
)

//AMQPOutbox sends messages to the amqp transport
type AMQPOutbox struct {
	channel      *amqp.Channel
	confirm      bool
	resendOnNack bool
	sequence     uint64
	ack          chan uint64
	nack         chan uint64
	resends      chan pendingConfirmation
	locker       *sync.Mutex
	pending      map[uint64]pendingConfirmation
	stop         chan bool
}

func (out *AMQPOutbox) init(amqp *amqp.Channel, confirm, resendOnNack bool) error {
	out.stop = make(chan bool)
	out.pending = make(map[uint64]pendingConfirmation)
	out.locker = &sync.Mutex{}
	out.channel = amqp
	out.confirm = confirm
	if confirm {

		out.ack = make(chan uint64, 10)
		out.nack = make(chan uint64, 10)
		out.resends = make(chan pendingConfirmation)

		err := out.channel.Confirm(false /*noWait*/)

		if err != nil {
			return err
		}
		if resendOnNack {
			out.resendOnNack = resendOnNack
			out.channel.NotifyConfirm(out.ack, out.nack)
			go out.confirmationLoop()
		}

	}

	return nil
}

func (out *AMQPOutbox) shutdown() {
	out.stop <- true

}

//Post implements Outbox.Send
func (out *AMQPOutbox) Post(exchange, routingKey string, amqpMessage amqp.Publishing) (uint64, error) {

	out.locker.Lock()
	defer out.locker.Unlock()
	//generate the delivery tag for this message
	nextSequence := out.sequence + 1

	if out.confirm {
		p := pendingConfirmation{
			exchange:    exchange,
			routingKey:  routingKey,
			amqpMessage: amqpMessage}
		out.pending[nextSequence] = p
	}

	sendErr := out.sendToChannel(exchange, routingKey, amqpMessage)
	if sendErr != nil {
		//if an error was received then move the pending confirmation from the pending map
		delete(out.pending, nextSequence)
		return 0, sendErr

	} else {
		// only update the global sequence if the send optation on the channel does not return an error
		// so that the global sequence and the channel delivery tag counter stay in sync
		out.sequence = nextSequence
	}
	return out.sequence, nil
}

func (out *AMQPOutbox) confirmationLoop() {

	for {
		select {
		case <-out.stop:
			return
		case ack := <-out.ack:
			if ack <= 0 {
				continue
			}
			out.locker.Lock()
			pending := out.pending[ack]
			if pending.deliveryTag > 0 {
				log.Printf("ack received for a pending delivery with tag %v", ack)
			}
			delete(out.pending, ack)
			out.locker.Unlock()
		case nack := <-out.nack:
			if nack <= 0 {
				continue
			}
			log.Printf("nack received for delivery tag %v", nack)
			out.locker.Lock()
			pending := out.pending[nack]
			pending.deliveryTag = nack
			out.resends <- pending
			delete(out.pending, nack)
			out.locker.Unlock()
		case resend := <-out.resends:
			out.Post(resend.exchange, resend.routingKey, resend.amqpMessage)
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

func (out *AMQPOutbox) NotifyConfirm(ack, nack chan uint64) {
	out.channel.NotifyConfirm(ack, nack)
}

type pendingConfirmation struct {
	deliveryTag uint64
	exchange    string
	routingKey  string
	amqpMessage amqp.Publishing
}
