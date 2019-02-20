package saga

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/rhinof/grabbit/gbus"
)

//TimeoutManager manages timeouts for sagas
//TODO:Make it persistent
type TimeoutManager struct {
	bus gbus.Bus
}

//RequestTimeout requests a timeout from the timeout manager
func (tm *TimeoutManager) RequestTimeout(svcName, sagaID string, duration time.Duration) {

	go func(svcName, sagaID string, tm *TimeoutManager) {
		c := time.After(duration)
		<-c
		reuqestTimeout := gbus.SagaTimeoutMessage{
			SagaID: sagaID}
		msg := gbus.NewBusMessage(reuqestTimeout)
		msg.SagaCorrelationID = sagaID
		span := opentracing.GlobalTracer().StartSpan("timeout")
		if span != nil {
			defer span.Finish()
		}
		if err := tm.bus.Send(opentracing.ContextWithSpan(context.Background(), span), svcName, msg); err != nil {
			//TODO: add logger
			logrus.WithError(err).Error("could not send timeout to bus")
		}

	}(svcName, sagaID, tm)
}
