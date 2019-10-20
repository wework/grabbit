package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var OutboxSize = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: grabbitPrefix,
	Name:      "outbox_total_records",
	Subsystem: "outbox",
	Help:      "reports the total amount of records currently in the outbox",
})

var PendingMessages = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: grabbitPrefix,
	Name:      "outbox_pending_delivery",
	Subsystem: "outbox",
	Help:      "reports the total amount of records pending delivery currently in the outbox",
})

var SentMessages = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: grabbitPrefix,
	Name:      "outbox_pending_removal",
	Subsystem: "outbox",
	Help:      "reports the total amount of records that were sent and pending removal currently in the outbox",
})
