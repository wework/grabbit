package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"sync"
)

var (
	handlerMetricsByHandlerName  = make(map[string]*HandlerMetrics)
	lock      					 = &sync.Mutex{}
)

const (
	Failure         = "failure"
	Success         = "success"
	ExceededRetries = "exceeded_retries"
	HandlerResult   = "result"
)

type HandlerMetrics struct {
	result  *prometheus.CounterVec
	latency prometheus.Histogram
}

func DefaultBuckets() []float64 {
	return []float64{0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 1}
}

func AddHandlerMetrics(handlerName string, buckets []float64) {
	lock.Lock()
	defer lock.Unlock()
	_, ok := handlerMetricsByHandlerName[handlerName]
	if !ok {
		handlerMetricsByHandlerName[handlerName] = newHandlerMetrics(handlerName, buckets)
	}
}

func RunHandlerWithMetric(handleMessage func() error, handlerName string) error {
	handlerMetrics, ok := handlerMetricsByHandlerName[handlerName]

	if !ok {
		logrus.WithField("handler", handlerName).Warn("Running with metrics - couldn't find metrics for the given handler")
		return handleMessage()
	}

	err := trackTime(handleMessage, handlerMetrics.latency)

	if err != nil {
		handlerMetrics.result.WithLabelValues(Failure).Inc()
	} else {
		handlerMetrics.result.WithLabelValues(Success).Inc()
	}

	return err
}

func ReportHandlerExceededMaxRetries(handlerName string) {
	handlerMetrics, ok := handlerMetricsByHandlerName[handlerName]

	if !ok {
		logrus.WithField("handler", handlerName).Warn("Report handler exceeded retries - couldn't find metrics for the given handler")
	}

	handlerMetrics.result.WithLabelValues(ExceededRetries).Inc()
}

func GetHandlerMetrics(handlerName string) *HandlerMetrics {
	return handlerMetricsByHandlerName[handlerName]
}

func newHandlerMetrics(handlerName string, buckets []float64) *HandlerMetrics {
	return &HandlerMetrics{
		result: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: "handlers",
				Name:      fmt.Sprintf("%s_result", handlerName),
				Help:      fmt.Sprintf("The %s's result", handlerName),
			},
			[]string{HandlerResult}),
		latency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Subsystem: "handlers",
				Name:      fmt.Sprintf("%s_latency", handlerName),
				Help:      fmt.Sprintf("The %s's latency", handlerName),
				Buckets:   buckets,
			}),
	}
}

func trackTime(functionToTrack func() error, observer prometheus.Observer) error {
	timer := prometheus.NewTimer(observer)
	defer timer.ObserveDuration()

	return functionToTrack()
}

func (hm *HandlerMetrics) GetSuccessCount() (float64, error) {
	return hm.getCounterValue(Success)
}

func (hm *HandlerMetrics) GetFailureCount() (float64, error) {
	return hm.getCounterValue(Failure)
}

func (hm *HandlerMetrics) GetExceededRetiesCount() (float64, error) {
	return hm.getCounterValue(ExceededRetries)
}

func (hm *HandlerMetrics) GetLatencyBuckets() ([]*io_prometheus_client.Bucket, error) {
	m := &io_prometheus_client.Metric{}
	err := hm.latency.Write(m)
	if err != nil {
		return nil, err
	}

	return m.GetHistogram().Bucket, nil
}

func (hm *HandlerMetrics) getCounterValue(label string) (float64, error) {
	m := &io_prometheus_client.Metric{}
	err := hm.result.WithLabelValues(label).Write(m)

	if err != nil {
		return 0, err
	}

	return m.GetCounter().GetValue(), nil
}