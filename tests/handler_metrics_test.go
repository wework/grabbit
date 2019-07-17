package tests

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus/metrics"
	"testing"
)

var (
	logger  logrus.FieldLogger
	runningTries = 5
)

func TestAddHandlerMetrics(t *testing.T) {
	name := "handler1"
	metrics.AddHandlerMetrics(name)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Error("Failed to create handler metrics")
	}

	metrics.AddHandlerMetrics(name)
	hm1 := metrics.GetHandlerMetrics(name)

	if hm1 == nil {
		t.Error("Failed to create handler metrics")
	}

	if hm1 != hm {
		t.Error("Created two handlers with the same name")
	}

	differentName := "handler2"
	metrics.AddHandlerMetrics(differentName)
	hm2 := metrics.GetHandlerMetrics(differentName)

	if hm2 == nil {
		t.Error("Failed to create handler metrics")
	}

	if hm2 == hm {
		t.Error("Failed to create a different handler metrics")
	}
}

func TestRunHandlerWithMetric_FailureCounter(t *testing.T) {
	logger = logrus.WithField("testCase", "TestRunHandlerWithMetric_FailureCounter")
	name := "failure"
	metrics.AddHandlerMetrics(name)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}
	failure := func() error {
		return errors.New("error in running handler")
	}

	for i := 1; i < runningTries; i++ {
		err := metrics.RunHandlerWithMetric(failure, name, logger)

		if err == nil {
			t.Error("Failed handler run should return an error")
		}

		count, err := hm.GetFailureCount()

		if err != nil {
			t.Errorf("Failed to get counter value: %e", err)
		}
		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the failure counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_SuccessCounter(t *testing.T) {
	logger = logrus.WithField("testCase", "TestRunHandlerWithMetric_SuccessCounter")
	name := "success"
	metrics.AddHandlerMetrics(name)
	success := func() error {
		return nil
	}
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		err := metrics.RunHandlerWithMetric(success, name, logger)

		if err != nil {
			t.Error("Successful handler run shouldn't return an error")
		}

		count, err := hm.GetSuccessCount()

		if err != nil {
			t.Errorf("Failed to get counter value: %e", err)
		}
		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the success counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_ExceededRetriesCounter(t *testing.T) {
	logger = logrus.WithField("testCase", "TestRunHandlerWithMetric_ExceededRetriesCounter")
	name := "exceededRetries"
	metrics.AddHandlerMetrics(name)
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		metrics.ReportHandlerExceededMaxRetries(name, logger)
		count, err := hm.GetExceededRetiesCount()

		if err != nil {
			t.Errorf("Failed to get counter value: %e", err)
		}

		if count != float64(i) {
			t.Errorf("Expected to get %f as the value of the exceeded retries counter, but got %f", float64(i), count)
		}
	}
}

func TestRunHandlerWithMetric_Latency(t *testing.T) {
	logger = logrus.WithField("testCase", "TestRunHandlerWithMetric_ExceededRetriesCounter")
	name := "latency"
	metrics.AddHandlerMetrics(name)
	success := func() error {
		return nil
	}
	hm := metrics.GetHandlerMetrics(name)

	if hm == nil {
		t.Errorf("Couldn't find handler with the name %s", name)
	}

	for i := 1; i < runningTries; i++ {
		_ = metrics.RunHandlerWithMetric(success, name, logger)
		sc, err := hm.GetLatencySampleCount()

		if err != nil {
			t.Errorf("Failed to get latency value: %e", err)
		}
		if sc == nil {
			t.Errorf("Expected latency sample count not be nil")
		}
		if *sc != uint64(i) {
			t.Errorf("Expected to get %d as the value of the latency sample count, but got %d", uint64(i), *sc)
		}
	}
}
