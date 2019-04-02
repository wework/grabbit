# Saga

grabbit makes it easier implementing orchestration based saga's by allowing to couple message exchange with a specific instance of persistent state.

Let's assume we were tasked to implement a business process of booking a vacation which consists of coordinating between the flight reservation service and hotel booking service.

Reacting to a BookVacation command our service will issue a BookFlight command to the flight reservation service and a BookHotel command to the hotel booking service.
We will then wait for replies from these services confirming or declining the booking requests and issue a VacationBookingComplete event if all parties accepted our booking or issue compensating commands if either the hotel or flight service declined our command.

Finally we would like to set a timeout for the entire process ensuring that if the process is not complete by a given timeframe a VacationBookingTimedout event is raised


## Defining a Saga

### Step 1 - Declare a struct that will hold the state of the saga
```go
import (
	"github.com/rhinof/grabbit/gbus"
)

type BookVacationSaga struct {
	BookingId string
	GotCarSvcResponse bool
	GotHotelSvcResponse bool
}
```

### Step 2 - Implement the gbus.Saga interface

In order to use your defined struct as a saga  you will need to implement the gbus.Saga interface inorder to let grabbit know the following things

	1. In response to what messages a new instance of the saga be created
	2. Which commands/replies and events should be routed to the saga instance
	3. How to create a new instance of the saga
	4. When the saga has completed (and should be removed)

In order to do all of the above, you will need your saga to implement the gbus.Saga interface which is defined as follows

```go
//Saga is the base interface for all Sagas.
type Saga interface {
	StartedBy() []Message
	RegisterAllHandlers(register HandlerRegister)
	IsComplete() bool
	New() Saga
}
```

Define the messages that should create a new instance of the sage:

```go
func (*BookVacationSaga) StartedBy() []gbus.Message {
	starters := make([]gbus.Message, 0)
	return append(starters, BookVacation{})
}
```
Implement the factory method that will be called each time a new saga instance needs to be created

```go
func (s *BookVacationSaga) New() gbus.Saga {
	return &BookVacationSaga{}
}
```

Define when the saga should be marked as complete

```go
func (s *BookVacationSaga) IsComplete() bool {
	return s.GotCarSvcResponse && s.GotHotelSvcResponse
}
```

Subscribe on the messages that should be handled by the saga and map them to handlers

```go
func (s *BookVacationSaga) RegisterAllHandlers(register gbus.HandlerRegister) {
	register.HandleMessage(BookVacation{}, s.HandleBookVacationCommand)
	register.HandleMessage(BookHotelResponse{}, s.HandleBookHotelResponse)
	register.HandleMessage(BookFlightResponse{}, s.HandleBookFlightResponse)
}

```

### Step3 - Implement the business logic of the handlers:

Handle the startup command and send commands to the flight and hotel booking services

```go
func (s *BookVacationSaga) HandleBookVacationCommand(invocation gbus.Invocation, message *gbus.BusMessage) error {
	
	s.BookingId = getSomeUUID()
	
	//send a command to the flight service to book a flight
	bookFlightCmd := gbus.NewBusMessage(BookFlight{})
	bookFlightErr := invocation.Bus().Send(invocation.Ctx(), "flightSvc", bookFlightCmd)
	if bookFlightErr != nil{
	  return bookFlightErr
	}
	
	//send a command to the hotel service to book a flight
	bookHotelCmd := gbus.NewBusMessage(BookFlight{})
	bookHotelErr := invocation.Bus().Send(invocation.Ctx(), "hotelSvc", bookHotelCmd)
	
	if bookHotelErr != nil{
	  return bookHotelErr
	}
	
	reply := gbus.NewBusMessage(BookVacationReply{
		BookingId: s.BookingId})
	//reply to the command so the caller can continue with his execution flow	
	return invocation.Reply(noopTraceContext(), reply)
}
```

handle the response of the hotel booking service

```go
func (s *BookVacationSaga) HandleBookHotelResponse(invocation gbus.Invocation, message *gbus.BusMessage) error {
	
	hotelBookingResponse := message.Payload.(BookHotelResponse)
	if !hotelBookingResponse.Booked{
	 cancleFlightBooking := gbus.NewBusMessage(CancleFlightBooking{
	 	BookingRef : s.BookingId
	 })
	 
	 return invocation.Bus().Send(invocation.Ctx(), "flightSvc", cancleFlightBooking)
	
	}
	
	log.Printf("do some business logic %v", responseMsg)
	
	s.GotHotelSvcResponse = true
	
	//publish event if saga is complete
	if s.IsComplete(){
	 event :=  gbus.NewBusMessage(VacationBookingComplete{})
	 invocation.Bus().Publish(invocation.Ctx(), "some_exchange", "some.topic", event )
	
	}
	
	return nil

}
```

handle the response of the flight booking service

```go
func (s *BookVacationSaga) HandleBookFlightResponse(invocation gbus.Invocation, message *gbus.BusMessage) error {
	
	flightBookingResponse := message.Payload.(BookHotelResponse)
	if !flightBookingResponse.Booked{
	 cancleHoteltBooking := gbus.NewBusMessage(CancleHotelBooking{
	 	BookingRef : s.BookingId
	 })
	 
	 return invocation.Bus().Send(invocation.Ctx(), "hotelSvc", cancleHoteltBooking)
	
	}
	
	s.GotFlightSvcResponse = true
	
	//publish event if saga is complete
	if s.IsComplete(){
	 event :=  gbus.NewBusMessage(VacationBookingComplete{})
	 invocation.Bus().Publish(invocation.Ctx(), "some_exchange", "some.topic", event )
	
	}
	
	return nil

}
```
### Step 4 - Handling the  timeout requirement

In order to define a timeout for the saga and have grabbit call the saga instance once that timeout is reached (assuming the saga hasn't completed yet) the saga needs to implement the gbus.RequestSagaTimeout interface

```go
type RequestSagaTimeout interface {
	TimeoutDuration() time.Duration
	Timeout(invocation Invocation, message *BusMessage) error
}
```

So in order to fulfill our requerment we will need to add the following to our saga

```go

func (s *BookVacationSaga) RequestTimeout() time.Duration {
	//request to timeout if after 15 minutes the saga is not complete
	return time.Minute * 15
}

func (s *BookVacationSaga) Timeout(invocation gbus.Invocation, message *gbus.BusMessage) error {
	return invocation.Bus().Publish(invocation.Ctx(), "some_exchange", "some.topic.1", gbus.NewBusMessage(VacationBookingTimedOut{}))
}

```


### Step 5 - Register the saga with bus
```go

gb := getBus("vacationSvc")
gb.RegisterSaga(BookVacationSaga{})

```
