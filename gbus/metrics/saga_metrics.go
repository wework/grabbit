package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

//SagaTimeoutCounter is the prometheus counter counting timed out saga instances
var (
	SagaTimeoutCounter = newSagaTimeoutCounter()
	SagaLatencySummary = newSagaLatencySummary()
)

//GetSagaTimeoutCounterValue gets the counter value of timed out sagas reported to prometheus
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

func newSagaLatencySummary() prometheus.SummaryVec {
	return prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: grabbitPrefix,
		Subsystem: "saga",
		Name:      "latency",
		Help:      "The latency of the entire saga",
	}, []string{"SagaId", "Service", "SagaType"})

}
