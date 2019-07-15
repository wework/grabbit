package tests

import (
	"errors"
	"github.com/wework/grabbit/gbus/metrics"
	"testing"
)

var (
	buckets 	 = []float64{1}
	runningTries = 5
)

func TestAddHandlerMetrics(t *testing.T) {
	name := "handler1"
	metrics.AddHandlerMetrics(name, buckets)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Error("Failed to create handler metrics")
	}

	metrics.AddHandlerMetrics(name, buckets)
	hm1 := metrics.GetHandlerMetrics(name)

	if hm1 == nil {
		t.Error("Failed to create handler metrics")
	}

	if hm1 != hm {
		t.Error("Created two handlers with the same name")
	}

	differentName := "handler2"
	metrics.AddHandlerMetrics(differentName, buckets)
	hm2 := metrics.GetHandlerMetrics(differentName)

	if hm2 == nil {
		t.Error("Failed to create handler metrics")
	}

	if hm2 == hm {
		t.Error("Failed to create a different handler metrics")
	}
}

func TestRunHandlerWithMetric_FailureCounter(t *testing.T) {
	name := "failure"
	metrics.AddHandlerMetrics(name, buckets)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}
	failure := func() error {
		return errors.New("error in running handler")
	}

	for i := 1; i < runningTries; i++ {
		err := metrics.RunHandlerWithMetric(failure, name)

		if err == nil {
			t.Error("Failed handler run should return an error")
		}

		count := hm.GetFailureCount()

		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the failure counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_SuccessCounter(t *testing.T) {
	name := "success"
	metrics.AddHandlerMetrics(name, buckets)
	success := func() error {
		return nil
	}
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		err := metrics.RunHandlerWithMetric(success, name)

		if err != nil {
			t.Error("Successful handler run shouldn't return an error")
		}

		count := hm.GetSuccessCount()

		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the success counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_ExceededRetriesCounter(t *testing.T) {
	name := "exceededRetries"
	metrics.AddHandlerMetrics(name, buckets)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		metrics.ReportHandlerExceededMaxRetries(name)
		count := hm.GetExceededRetiesCount()

		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the exceeded retries counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_Latency(t *testing.T) {
	name := "latency"
	metrics.AddHandlerMetrics(name, buckets)
	success := func() error {
		return nil
	}
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		_ = metrics.RunHandlerWithMetric(success, name)
		lb := hm.GetLatencyBuckets()

		if len(lb) != len(buckets) {
			t.Errorf("Expected latency buckets array length to equal the given buckets length: %d but was %d", len(buckets), len(lb))
		}

		if *lb[0].CumulativeCount != uint64(i) {
			t.Errorf("Expected to get %d as the value of the bucket cumulative count, but got %d", uint64(i), *lb[0].CumulativeCount)
		}
	}
}