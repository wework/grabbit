package saga

import (
	"time"

	"github.com/rhinof/grabbit/gbus"
)

//TimeoutManager manages timeouts for sagas
//TODO:Make it persistant
type TimeoutManager struct {
	bus gbus.Bus
}

/*
	RequestTimeout requests a timeout from the timeout manager
*/
func (tm *TimeoutManager) RequestTimeout(svcName, sagaID string, duration time.Duration) {

	go func(svcName, sagaID string, tm *TimeoutManager) {
		c := time.After(duration)
		<-c
		reuqestTimeout := gbus.SagaTimeoutMessage{
			SagaID: sagaID}
		msg := gbus.NewBusMessage(reuqestTimeout)
		msg.SagaCorrelationID = sagaID
		tm.bus.Send(svcName, msg)

	}(svcName, sagaID, tm)
}
