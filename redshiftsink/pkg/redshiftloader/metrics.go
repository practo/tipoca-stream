package redshiftloader

import (
	"fmt"
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
	durationMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "seconds",
			Help:      "total time taken to load data in Redshift in seconds",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
	)
	copyStageMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "copystage_seconds",
			Help:      "time taken to create staging table and load data in it in seconds",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
	)
	deDupeMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "dedupe_seconds",
			Help:      "time taken to de duplicate table in staging in seconds",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
	)
	deleteCommonMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "deletecommon_seconds",
			Help:      "time taken to delete common in seconds",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
	)
	deleteOpStageMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "deleteop_seconds",
			Help:      "time taken to delete rows with operations delete in seconds",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
	)
	copyTargetMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rsk",
			Subsystem: "loader",
			Name:      "copytarget_seconds",
			Help:      "time taken to copy to target table from staging table",
		},
		[]string{"consumergroup", "topic", "sink_group", "messages", "bytes"},
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
	bytes         float64
}

func (m metricSetter) setBytesLoaded(bytes float64) {
	bytesLoadedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Add(bytes)
}

func (m metricSetter) setMsgsLoaded(msgs float64) {
	msgsLoadedMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
	).Add(msgs)
}

func (m metricSetter) setLoadSeconds(bytes float64, msgs float64, seconds float64) {
	durationMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}

func (m metricSetter) setCopyStageSeconds(bytes int64, msgs int, seconds float64) {
	copyStageMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}

func (m metricSetter) setDedupeSeconds(bytes int64, msgs int, seconds float64) {
	deDupeMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}

func (m metricSetter) setDeleteCommonSeconds(bytes int64, msgs int, seconds float64) {
	deleteCommonMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}

func (m metricSetter) setDeleteOpStageSeconds(bytes int64, msgs int, seconds float64) {
	deleteOpStageMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}

func (m metricSetter) setCopyTargetSeconds(bytes int64, msgs int, seconds float64) {
	copyTargetMetric.WithLabelValues(
		m.consumergroup,
		m.topic,
		m.sinkGroup,
		fmt.Sprintf("%v", bytes),
		fmt.Sprintf("%v", msgs),
	).Set(seconds)
}
