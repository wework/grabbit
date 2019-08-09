package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var SagaTimeoutCounter = newSagaTimeoutCounter()

func GetSagaTimeoutCounterValue() (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := SagaTimeoutCounter.Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}

func newSagaTimeoutCounter() prometheus.Counter {
	return promauto.NewCounter(prometheus.CounterOpts{
		Namespace: grabbitPrefix,
		Subsystem: "saga",
		Name:      "timedout_sagas",
		Help:      "counting the number of timedout saga instances",
	})
}
