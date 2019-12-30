package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	rejectedMessages       = newRejectedMessagesCounter()
	duplicateMessageAck    = newDuplicateMessageCounter("ack")
	duplicateMessageReject = newDuplicateMessageCounter("reject")
)

//ResetRejectedMessagesCounter resets the counter intended to be used in tests only
func ResetRejectedMessagesCounter() {

	prometheus.Unregister(rejectedMessages)
	rejectedMessages = newRejectedMessagesCounter()
}

//ReportRejectedMessage reports a message being rejected to the metrics counter
func ReportRejectedMessage() {
	rejectedMessages.Inc()
}

//GetRejectedMessagesValue gets the value of the rejected message counter
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

// ResetDuplicateMessagesCounters resets the counter intended to be used in tests only
func ResetDuplicateMessagesCounters() {
	prometheus.Unregister(duplicateMessageAck)
	duplicateMessageAck = newDuplicateMessageCounter("ack")
	prometheus.Unregister(duplicateMessageReject)
	duplicateMessageReject = newDuplicateMessageCounter("reject")
}

// ReportDuplicateMessageAck reports a message is a duplicate and Acked
func ReportDuplicateMessageAck() {
	duplicateMessageAck.Inc()
}

// ReportDuplicateMessageReject reports a message is a duplicate and Rejected
func ReportDuplicateMessageReject() {
	duplicateMessageReject.Inc()
}

// GetDuplicateMessageRejectValue gets the value of the rejected message counter
func GetDuplicateMessageRejectValue() (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := duplicateMessageReject.Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}

// GetDuplicateMessageAckValue gets the value of the rejected message counter
func GetDuplicateMessageAckValue() (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := duplicateMessageAck.Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}

func newDuplicateMessageCounter(rabbitAnswer string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   grabbitPrefix,
		Subsystem:   "messages",
		Name:        "duplicate_messages",
		Help:        "counting the duplicate messages",
		ConstLabels: prometheus.Labels{"type": rabbitAnswer},
	})
}
