package messages

/**** Booking ****/

type BookVacationCmd struct {
	Destination string
}

func (BookVacationCmd) SchemaName() string {
	return "BookVacationCmd"
}

type BookVacationRsp struct {
	Result bool
}

func (BookVacationRsp) SchemaName() string {
	return "BookVacationRsp"
}

/**** Flights ****/

type BookFlightsCmd struct {
	Destination string
}

func (BookFlightsCmd) SchemaName() string {
	return "BookFlightsCmd"
}

type BookFlightsRsp struct {
	Success bool
}

func (BookFlightsRsp) SchemaName() string {
	return "BookFlightsRsp"
}

type CancelFlightsCmd struct {
	Destination string
}

func (CancelFlightsCmd) SchemaName() string {
	return "CancelFlightsCmd"
}

type CancelFlightsRsp struct {
	Success bool
}

func (CancelFlightsRsp) SchemaName() string {
	return "CancelFlightsRsp"
}

/**** Hotels ****/

type BookHotelCmd struct {
	Destination string
}

func (BookHotelCmd) SchemaName() string {
	return "BookHotelCmd"
}

type BookHotelRsp struct {
	Success bool
}

func (BookHotelRsp) SchemaName() string {
	return "BookHotelRsp"
}

type BookingComplete struct {
	Destination string
	Success     bool
}

func (BookingComplete) SchemaName() string {
	return "BookingComplete"
}

// *** CancelFlightCmd ***//

// *** CancelFlighRpl ***//
