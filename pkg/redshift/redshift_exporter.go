package redshift

import (
	"context"

	"github.com/practo/klog/v2"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Namespace     = "redshift"
	SubSystemScan = "scan"
)

type RedshiftCollector struct {
	client           *Redshift
	queryTotalMetric *prometheus.Desc
}

func NewRedshiftCollector(client *Redshift) *RedshiftCollector {
	return &RedshiftCollector{
		client: client,
		queryTotalMetric: prometheus.NewDesc(
			prometheus.BuildFQName(Namespace, SubSystemScan, "query_total"),
			"Total number of redshift queries executed",
			[]string{"database", "schema", "tablename", "tableid"},
			nil,
		),
	}
}

func (c *RedshiftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queryTotalMetric
}

func (c *RedshiftCollector) Collect(ch chan<- prometheus.Metric) {
	queryTotalRows, err := c.client.ScanQueryTotal(context.Background())
	if err != nil {
		klog.Fatalf("Redshift Collector shutdown due to error: %v", err)
	}

	for _, queryTotalRow := range queryTotalRows {
		ch <- prometheus.MustNewConstMetric(
			c.queryTotalMetric,
			prometheus.CounterValue,
			queryTotalRow.QueryTotal,
			queryTotalRow.Database,
			queryTotalRow.Schema,
			queryTotalRow.TableName,
			queryTotalRow.TableID,
		)
	}
}
