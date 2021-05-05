package redshiftbatcher

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	bytesProcessedMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "bytes_processed",
			Help:      "total number of bytes processed",
		},
		[]string{"consumergroup", "topic", "sinkGroup"},
	)
	msgsProcessedMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "batcher",
			Name:      "messages_processed",
			Help:      "total number of messages processed",
		},
		[]string{"consumergroup", "topic", "sinkGroup"},
	)
)

func init() {
	prometheus.MustRegister(bytesProcessedMetric)
	prometheus.MustRegister(msgsProcessedMetric)
}

type metricSetter struct {
	consumergroup string
	topic         string
	sinkGroup     string
}

func (m metricSetter) setBytesProcessed(bytes int64) {
	bytesProcessedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(float64(bytes))
}

func (m metricSetter) setMsgsProcessed(msgs int) {
	msgsProcessedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(float64(msgs))
}
