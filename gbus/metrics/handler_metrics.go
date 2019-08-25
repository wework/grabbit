package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
)

var (
	handlerMetricsByHandlerName = &sync.Map{}
	handlersResultCounter       = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: grabbitPrefix,
		Subsystem: handlers,
		Name:      handlerResult,
		Help:      "The result of the message handler. The message type and result are labeled",
	}, []string{messageType, handlerResult})
	handlersLatencySummary = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: grabbitPrefix,
		Subsystem: handlers,
		Name:      handlerLatency,
		Help:      "The latency of the message handler. The message type is labeled",
	}, []string{messageType})
)

const (
	failure        = "failure"
	success        = "success"
	handlerResult  = "result"
	handlers       = "handlers"
	grabbitPrefix  = "grabbit"
	messageType    = "message_type"
	handlerLatency = "latency"
)

type handlerMetrics struct {
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
			handlersResultCounter.WithLabelValues(messageType, failure).Inc()
			panic(p)
		}
	}()

	if handlerMetrics == nil {
		logger.WithField("handler", handlerName).Warn("Running with metrics - couldn't find metrics for the given handler")
		return trackTime(handleMessage, handlersLatencySummary.WithLabelValues(messageType))
	}

	err := trackTime(handleMessage, handlerMetrics.latency, handlersLatencySummary.WithLabelValues(messageType))

	if err != nil {
		handlerMetrics.result.WithLabelValues(failure).Inc()
		handlersResultCounter.WithLabelValues(messageType, failure).Inc()
	} else {
		handlerMetrics.result.WithLabelValues(success).Inc()
		handlersResultCounter.WithLabelValues(messageType, success).Inc()
	}

	return err
}

//GetHandlerMetrics gets the metrics handler associated with the handlerName
func GetHandlerMetrics(handlerName string) *handlerMetrics {
	entry, ok := handlerMetricsByHandlerName.Load(handlerName)
	if ok {
		return entry.(*handlerMetrics)
	}

	return nil
}

func newHandlerMetrics(handlerName string) *handlerMetrics {
	return &handlerMetrics{
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

//GetSuccessCountByMessageType gets the counter value for the successful handlers' run for a given event type
func GetSuccessCountByMessageType(messageType string) (float64, error) {
	return getCounterValue(handlersResultCounter.WithLabelValues(messageType, success))
}

//GetFailureCountByMessageType gets the counter value for the failed handlers' run for a given event type
func GetFailureCountByMessageType(messageType string) (float64, error) {
	return getCounterValue(handlersResultCounter.WithLabelValues(messageType, failure))
}

//GetLatencySampleCountByMessageType gets the summary sample count value for the handlers' run for a given event type
func GetLatencySampleCountByMessageType(messageType string) (*uint64, error) {
	summary, ok := handlersLatencySummary.WithLabelValues(messageType).(prometheus.Summary)

	if !ok {
		return nil, fmt.Errorf("couldn't find summary for event type: %s", messageType)
	}

	return getSummarySampleCount(summary)
}

//GetSuccessCount gets the value of the handlers success value
func (hm *handlerMetrics) GetSuccessCount() (float64, error) {
	return getCounterValue(hm.result.WithLabelValues(success))
}

//GetFailureCount gets the value of the handlers failure value
func (hm *handlerMetrics) GetFailureCount() (float64, error) {
	return getCounterValue(hm.result.WithLabelValues(failure))
}

//GetLatencySampleCount gets the value of the handlers latency value
func (hm *handlerMetrics) GetLatencySampleCount() (*uint64, error) {
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
