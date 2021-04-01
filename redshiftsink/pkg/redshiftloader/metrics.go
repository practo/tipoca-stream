package redshiftloader

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesLoadedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "bytes_loaded",
			Help:      "total number of bytes loaded",
		},
		[]string{"consumergroup", "topic"},
	)
	msgsLoadedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "messages_loaded",
			Help:      "total number of messages loaded",
		},
		[]string{"consumergroup", "topic"},
	)
)

func init() {
	prometheus.MustRegister(bytesLoadedMetric)
	prometheus.MustRegister(msgsLoadedMetric)
}

func setBytesLoaded(consumergroup string, topic string, bytes float64) {
	bytesLoadedMetric.WithLabelValues(
		consumergroup,
		topic,
	).Add(bytes)
}

func setMsgsLoaded(consumergroup string, topic string, msgs float64) {
	msgsLoadedMetric.WithLabelValues(
		consumergroup,
		topic,
	).Add(msgs)
}

func setMetrics(consumergroup, topic string, bytes, msgs float64) {
	setBytesLoaded(consumergroup, topic, bytes)
	setMsgsLoaded(consumergroup, topic, msgs)
}
