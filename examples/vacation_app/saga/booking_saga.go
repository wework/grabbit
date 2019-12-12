package saga

import (
	"context"
	"vacation_app/messages"

	"github.com/wework/grabbit/gbus"
)

type BookingSaga struct {
	FinishedWithFlights bool
	FinishedWithHotels  bool
	CancelingFlights    bool
	Destination         string
}

func (bs *BookingSaga) StartedBy() []gbus.Message {
	startedBy := make([]gbus.Message, 0)
	startedBy = append(startedBy, messages.BookVacationCmd{})
	return startedBy
}

func (bs *BookingSaga) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(messages.BookVacationCmd{}, bs.HandleBookingCommand)
	register.HandleMessage(messages.BookFlightsRsp{}, bs.HandleBookFlightsRsp)
	register.HandleMessage(messages.BookHotelRsp{}, bs.HandleBookHotelRsp)
	register.HandleMessage(messages.CancelFlightsRsp{}, bs.HandleCancelFlightsRsp)
}

func (bs *BookingSaga) IsComplete() bool {
	return bs.FinishedWithFlights && bs.FinishedWithHotels
}

func (bs *BookingSaga) New() gbus.Saga {
	return &BookingSaga{}
}

func (bs *BookingSaga) HandleBookingCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	cmd := message.Payload.(*messages.BookVacationCmd)
	invocation.Log().Infof("booking a vacation to %s", cmd.Destination)
	bs.Destination = cmd.Destination
	bookFlights := gbus.NewBusMessage(
		messages.BookFlightsCmd{
			Destination: bs.Destination,
		})

	bookHotels := gbus.NewBusMessage(
		messages.BookHotelCmd{
			Destination: bs.Destination,
		})
	hErr := invocation.Bus().Send(invocation.Ctx(), "hotels-service", bookHotels)
	if hErr != nil {
		return hErr
	}
	return invocation.Bus().Send(invocation.Ctx(), "flights-service", bookFlights)
}

func (bs *BookingSaga) HandleBookFlightsRsp(invocation gbus.Invocation, message *gbus.BusMessage) error {
	response := message.Payload.(*messages.BookFlightsRsp)
	invocation.Log().Infof("flights were booked ? %t", response.Success)
	if bs.CancelingFlights == false {
		bs.FinishedWithFlights = true
	}
	return bs.notifyInitiatorIfSagaCompletes(true, invocation.Ctx(), invocation)
}

func (bs *BookingSaga) HandleBookHotelRsp(invocation gbus.Invocation, message *gbus.BusMessage) error {
	response := message.Payload.(*messages.BookHotelRsp)
	bs.FinishedWithHotels = true
	if response.Success {
		invocation.Log().Infof("hotel in %s was booked", bs.Destination)

	} else {
		bs.FinishedWithFlights = false
		bs.CancelingFlights = true
		invocation.Log().Infof("hotel in %s was not booked !!", bs.Destination)
		invocation.Log().Infof("canceling flights to %s !!", bs.Destination)
		cancelFlightsCmd := gbus.NewBusMessage(
			messages.CancelFlightsCmd{
				Destination: bs.Destination},
		)
		return invocation.Bus().Send(invocation.Ctx(), "flights-service", cancelFlightsCmd)
	}

	return bs.notifyInitiatorIfSagaCompletes(true, invocation.Ctx(), invocation)

}

func (bs *BookingSaga) HandleCancelFlightsRsp(invocation gbus.Invocation, message *gbus.BusMessage) error {
	response := message.Payload.(*messages.CancelFlightsRsp)
	invocation.Log().Infof("flights were canceled ? %t", response.Success)
	bs.FinishedWithFlights = true

	return bs.notifyInitiatorIfSagaCompletes(false, invocation.Ctx(), invocation)
}

func (bs *BookingSaga) notifyInitiatorIfSagaCompletes(success bool, ctx context.Context, invocation gbus.Invocation) error {

	if bs.IsComplete() {

		sagaInvocation := invocation.(gbus.SagaInvocation)
		reply := gbus.NewBusMessage(messages.BookingComplete{
			Destination: bs.Destination,
			Success:     success,
		})
		return sagaInvocation.ReplyToInitiator(invocation.Ctx(), reply)

	}
	return nil
}
