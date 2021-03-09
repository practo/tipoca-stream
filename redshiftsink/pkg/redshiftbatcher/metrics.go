package redshiftbatcher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	msgsPerSecMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "messages_processed_per_second",
			Help:      "number of messages processed per second",
		},
		[]string{"topic"},
	)
)

func init() {
	prometheus.MustRegister(msgsPerSecMetric)
}

func setMsgsProcessedPerSecond(topic string, msgsPerSec float64) {
	msgsPerSecMetric.WithLabelValues(topic).Set(msgsPerSec)
}
