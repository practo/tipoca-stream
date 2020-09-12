package redshiftbatcher

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

var (
	batchProcessingSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "redshiftbatcher",
			Subsystem: "batch",
			Name:      "processing_seconds",
			Help:      "Number of seconds to complete one batch succesfully.",
		},
		[]string{"topic"},
	)

	batchMessageProcessingSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "redshiftbatcher",
			Subsystem: "batch",
			Name:      "message_processing_seconds",
			Help:      "Number of seconds to complete one batch message succesfully.",
		},
		[]string{"topic"},
	)
)

func init() {
	prometheus.MustRegister(batchProcessingSeconds)
	prometheus.MustRegister(batchMessageProcessingSeconds)
}

func setBatchProcessingSeconds(now time.Time, topic string) {
	batchProcessingSeconds.WithLabelValues(topic).Set(
		time.Since(now).Seconds(),
	)
}

func setBatchMessageProcessingSeconds(now time.Time, topic string) {
	batchMessageProcessingSeconds.WithLabelValues(topic).Set(
		time.Since(now).Seconds(),
	)
}
