package redshiftbatcher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesPerSecMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "bytes_processed_per_second",
			Help:      "bytes processed per second",
		},
		[]string{"consumergroup", "topic"},
	)
	msgsPerSecMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "messages_processed_per_second",
			Help:      "number of messages processed per second",
		},
		[]string{"consumergroup", "topic"},
	)
)

func init() {
	prometheus.MustRegister(bytesPerSecMetric)
	prometheus.MustRegister(msgsPerSecMetric)
}

func setBytesProcessedPerSecond(consumergroup string, topic string, bytesPerSec float64) {
	bytesPerSecMetric.WithLabelValues(
		consumergroup,
		topic,
	).Set(bytesPerSec)
}

func setMsgsProcessedPerSecond(consumergroup string, topic string, msgsPerSec float64) {
	msgsPerSecMetric.WithLabelValues(
		consumergroup,
		topic,
	).Set(msgsPerSec)
}
