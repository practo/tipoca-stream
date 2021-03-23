package redshiftloader

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesPerSecMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "bytes_loaded_per_second",
			Help:      "bytes loaded per second",
		},
		[]string{"consumergroup", "topic"},
	)
	msgsPerSecMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "messages_loaded_per_second",
			Help:      "number of messages loaded per second",
		},
		[]string{"consumergroup", "topic"},
	)
)

func init() {
	prometheus.MustRegister(bytesPerSecMetric)
	prometheus.MustRegister(msgsPerSecMetric)
}

func setBytesLoadedPerSecond(consumergroup string, topic string, bytesPerSec float64) {
	bytesPerSecMetric.WithLabelValues(
		consumergroup,
		topic,
	).Set(bytesPerSec)
}

func setMsgsLoadedPerSecond(consumergroup string, topic string, msgsPerSec float64) {
	msgsPerSecMetric.WithLabelValues(
		consumergroup,
		topic,
	).Set(msgsPerSec)
}
