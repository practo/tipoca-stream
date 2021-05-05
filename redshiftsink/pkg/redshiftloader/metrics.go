package redshiftloader

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	buckets = []float64{10, 30, 60, 120, 180, 240, 300, 480, 600, 900}

	bytesLoadedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "bytes_loaded",
			Help:      "total number of bytes loaded",
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	msgsLoadedMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "messages_loaded",
			Help:      "total number of messages loaded",
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)

	// duration metrics
	durationMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "seconds",
			Help:      "total time taken to load data in Redshift in seconds",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	copyStageMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "copystage_seconds",
			Help:      "time taken to create staging table and load data in it in seconds",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	deDupeMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "dedupe_seconds",
			Help:      "time taken to de duplicate table in staging in seconds",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	deleteCommonMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "deletecommon_seconds",
			Help:      "time taken to delete common in seconds",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	deleteOpStageMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "deleteop_seconds",
			Help:      "time taken to delete rows with operations delete in seconds",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
	copyTargetMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "copytarget_seconds",
			Help:      "time taken to copy to target table from staging table",
			Buckets:   buckets,
		},
		[]string{"consumergroup", "topic", "sink_group"},
	)
)

func init() {
	prometheus.MustRegister(bytesLoadedMetric)
	prometheus.MustRegister(msgsLoadedMetric)

	prometheus.MustRegister(durationMetric)

	prometheus.MustRegister(copyStageMetric)
	prometheus.MustRegister(deDupeMetric)
	prometheus.MustRegister(deleteCommonMetric)
	prometheus.MustRegister(deleteOpStageMetric)
	prometheus.MustRegister(copyTargetMetric)
}

type metricSetter struct {
	consumergroup string
	topic         string
	sinkGroup     string
}

func (m metricSetter) setBytesLoaded(bytes int64) {
	bytesLoadedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Add(float64(bytes))
}

func (m metricSetter) setMsgsLoaded(msgs int) {
	msgsLoadedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Add(float64(msgs))
}

// duration metrics below

func (m metricSetter) setLoadSeconds(seconds float64) {
	durationMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}

func (m metricSetter) setCopyStageSeconds(seconds float64) {
	copyStageMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}

func (m metricSetter) setDedupeSeconds(seconds float64) {
	deDupeMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}

func (m metricSetter) setDeleteCommonSeconds(seconds float64) {
	deleteCommonMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}

func (m metricSetter) setDeleteOpStageSeconds(seconds float64) {
	deleteOpStageMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}

func (m metricSetter) setCopyTargetSeconds(seconds float64) {
	copyTargetMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Observe(seconds)
}
