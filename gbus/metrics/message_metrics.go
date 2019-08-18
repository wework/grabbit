package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_model/go"
)

var (
	rejectedMessages = newRejectedMessagesCounter()
)

func ReportRejectedMessage() {
	rejectedMessages.Inc()
}

func GetRejectedMessagesValue() (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := rejectedMessages.Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}

func newRejectedMessagesCounter() prometheus.Counter {
	return promauto.NewCounter(prometheus.CounterOpts{
		Namespace: grabbitPrefix,
		Subsystem: "messages",
		Name:      "rejected_messages",
		Help:      "counting the rejected messages",
	})
}
