package redshiftbatcher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesProcessedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "bytes_processed",
			Help:      "total number of bytes processed",
		},
		[]string{"consumergroup", "topic"},
	)
	msgsProcessedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "messages_processed",
			Help:      "total number of messages processed",
		},
		[]string{"consumergroup", "topic"},
	)
)

func init() {
	prometheus.MustRegister(bytesProcessedMetric)
	prometheus.MustRegister(msgsProcessedMetric)
}

func setBytesProcessed(consumergroup string, topic string, bytes float64) {
	bytesProcessedMetric.WithLabelValues(
		consumergroup,
		topic,
	).Add(bytes)
}

func setMsgsProcessed(consumergroup string, topic string, msgs float64) {
	msgsProcessedMetric.WithLabelValues(
		consumergroup,
		topic,
	).Add(msgs)
}

func setMetrics(consumergroup, topic string, bytes, msgs float64) {
	setBytesProcessed(consumergroup, topic, bytes)
	setMsgsProcessed(consumergroup, topic, msgs)
}
