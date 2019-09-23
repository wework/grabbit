package metrics

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	handlerMetricsByHandlerName = &sync.Map{}
	handlersResultCounter       = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: grabbitPrefix,
		Subsystem: handlers,
		Name:      handlerResult,
		Help:      "The result of the message handler. The handler's name, message type and result are labeled",
	}, []string{handler, messageTypeLabel, handlerResult})
	handlersLatencySummary = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: grabbitPrefix,
		Subsystem: handlers,
		Name:      handlerLatency,
		Help:      "The latency of the message handler. The handler's name and message type are labeled",
	}, []string{handler, messageTypeLabel})
)

const (
	failure          = "failure"
	success          = "success"
	handlerResult    = "result"
	handlers         = "handlers"
	grabbitPrefix    = "grabbit"
	messageTypeLabel = "message_type"
	handlerLatency   = "latency"
	handler          = "handler"
)

//HandlerMetrics holds the metrics results for a handler
type HandlerMetrics struct {
	result  *prometheus.CounterVec
	latency prometheus.Summary
}

//AddHandlerMetrics adds a handler to be tracked with metrics
func AddHandlerMetrics(handlerName string) {
	handlerMetrics := newHandlerMetrics(handlerName)
	_, exists := handlerMetricsByHandlerName.LoadOrStore(handlerName, handlerMetrics)

	if !exists {
		prometheus.MustRegister(handlerMetrics.latency, handlerMetrics.result)
	}
}

//RunHandlerWithMetric runs a specific handler with metrics being collected and reported to prometheus
func RunHandlerWithMetric(handleMessage func() error, handlerName, messageType string, logger logrus.FieldLogger) error {
	handlerMetrics := GetHandlerMetrics(handlerName)
	defer func() {
		if p := recover(); p != nil {
			if handlerMetrics != nil {
				handlerMetrics.result.WithLabelValues(failure).Inc()
			}
			handlersResultCounter.With(prometheus.Labels{handler: handlerName, messageTypeLabel: messageType, handlerResult: failure}).Inc()
			panic(p)
		}
	}()

	if handlerMetrics == nil {
		logger.WithField("handler", handlerName).Warn("Running with metrics - couldn't find metrics for the given handler")
		return trackTime(handleMessage, handlersLatencySummary.WithLabelValues(handlerName, messageType))
	}

	err := trackTime(handleMessage, handlerMetrics.latency, handlersLatencySummary.WithLabelValues(handlerName, messageType))

	if err != nil {
		handlerMetrics.result.WithLabelValues(failure).Inc()
		handlersResultCounter.With(prometheus.Labels{handler: handlerName, messageTypeLabel: messageType, handlerResult: failure}).Inc()
	} else {
		handlerMetrics.result.WithLabelValues(success).Inc()
		handlersResultCounter.With(prometheus.Labels{handler: handlerName, messageTypeLabel: messageType, handlerResult: success}).Inc()
	}

	return err
}

//GetHandlerMetrics gets the metrics handler associated with the handlerName
func GetHandlerMetrics(handlerName string) *HandlerMetrics {
	entry, ok := handlerMetricsByHandlerName.Load(handlerName)
	if ok {
		return entry.(*HandlerMetrics)
	}

	return nil
}

func newHandlerMetrics(handlerName string) *HandlerMetrics {
	return &HandlerMetrics{
		result: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: grabbitPrefix,
				Subsystem: handlers,
				Name:      fmt.Sprintf("%s_result", handlerName),
				Help:      fmt.Sprintf("The %s's result", handlerName),
			},
			[]string{handlerResult}),
		latency: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace: grabbitPrefix,
				Subsystem: handlers,
				Name:      fmt.Sprintf("%s_latency", handlerName),
				Help:      fmt.Sprintf("The %s's latency", handlerName),
			}),
	}
}

func trackTime(functionToTrack func() error, observers ...prometheus.Observer) error {
	timers := make([]*prometheus.Timer, 0)
	for _, observer := range observers {
		timers = append(timers, prometheus.NewTimer(observer))
	}

	defer func() {
		for _, timer := range timers {
			timer.ObserveDuration()
		}
	}()

	return functionToTrack()
}

//GetSuccessCountByMessageTypeAndHandlerName gets the counter value for the successful handlers' run for a given message type and handler's name
func GetSuccessCountByMessageTypeAndHandlerName(messageType, handlerName string) (float64, error) {
	return getCounterValue(handlersResultCounter.With(prometheus.Labels{messageTypeLabel: messageType, handler: handlerName, handlerResult: success}))
}

//GetFailureCountByMessageTypeAndHandlerName gets the counter value for the failed handlers' run for a given message type and handler's name
func GetFailureCountByMessageTypeAndHandlerName(messageType, handlerName string) (float64, error) {
	return getCounterValue(handlersResultCounter.With(prometheus.Labels{messageTypeLabel: messageType, handler: handlerName, handlerResult: failure}))
}

//GetLatencySampleCountByMessageTypeAndHandlerName gets the summary sample count value for the handlers' run for a given message type and handler's name
func GetLatencySampleCountByMessageTypeAndHandlerName(messageType, handlerName string) (*uint64, error) {
	summary, ok := handlersLatencySummary.With(prometheus.Labels{messageTypeLabel: messageType, handler: handlerName}).(prometheus.Summary)

	if !ok {
		return nil, fmt.Errorf("couldn't find summary for event type: %s", messageType)
	}

	return getSummarySampleCount(summary)
}

//GetSuccessCount gets the value of the handlers success value
func (hm *HandlerMetrics) GetSuccessCount() (float64, error) {
	return getCounterValue(hm.result.WithLabelValues(success))
}

//GetFailureCount gets the value of the handlers failure value
func (hm *HandlerMetrics) GetFailureCount() (float64, error) {
	return getCounterValue(hm.result.WithLabelValues(failure))
}

//GetLatencySampleCount gets the value of the handlers latency value
func (hm *HandlerMetrics) GetLatencySampleCount() (*uint64, error) {
	return getSummarySampleCount(hm.latency)
}

func getCounterValue(counter prometheus.Counter) (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := counter.Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}

func getSummarySampleCount(summary prometheus.Summary) (*uint64, error) {
	m := &io_prometheus_client.Metric{}
	err := summary.Write(m)

	if err != nil {
		return nil, err
	}

	return m.GetSummary().SampleCount, nil
}
