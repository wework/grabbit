package gbus

import (
	"sync"

	log "github.com/sirupsen/logrus"
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
	SvcName      string
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

//Shutdown stops the outbox
func (out *AMQPOutbox) Shutdown() {
	close(out.stop)
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

	}
	// only update the global sequence if the send optation on the channel does not return an error
	// so that the global sequence and the channel delivery tag counter stay in sync
	out.sequence = nextSequence
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
				out.log().WithField("tag", ack).Debug("ack received for a pending delivery with tag")
			}
			delete(out.pending, ack)
			out.locker.Unlock()
		case nack := <-out.nack:
			if nack <= 0 {
				continue
			}
			out.log().WithField("tag", nack).Debug("nack received for a pending delivery with tag")
			out.locker.Lock()
			pending := out.pending[nack]
			pending.deliveryTag = nack
			out.resends <- pending
			delete(out.pending, nack)
			out.locker.Unlock()
		case resend := <-out.resends:
			_, err := out.Post(resend.exchange, resend.routingKey, resend.amqpMessage)
			if err != nil {
				out.log().WithError(err).Error("could not post message to exchange")
			}
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

//NotifyConfirm send an amqp notification
func (out *AMQPOutbox) NotifyConfirm(ack, nack chan uint64) {
	out.channel.NotifyConfirm(ack, nack)
}

func (out *AMQPOutbox) log() *log.Entry {
	return log.WithField("_service", out.SvcName)
}

type pendingConfirmation struct {
	deliveryTag uint64
	exchange    string
	routingKey  string
	amqpMessage amqp.Publishing
}
