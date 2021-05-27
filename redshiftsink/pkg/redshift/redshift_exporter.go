package redshift

import (
	"context"

	"github.com/practo/klog/v2"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

var (
	Namespace     = "redshift"
	SubSystemScan = "scan"
)

type RedshiftCollector struct {
	client           *Redshift
	queryTotalMetric *prometheus.Desc

	ready      bool
	queryTotal sync.Map
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

func (c *RedshiftCollector) updateQueryTotal(ctx context.Context) {
	klog.V(2).Info("fetching redshift.scan.query_total")
	queryTotalRows, err := c.client.ScanQueryTotal(ctx)
	if err != nil {
		klog.Fatalf("Redshift Collector shutdown due to error: %v", err)
	}

	c.queryTotal.Store("", queryTotalRows)
}

func (c *RedshiftCollector) Fetch(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	klog.V(2).Info("fetching redshift.scan.query_total (first scan)")
	c.updateQueryTotal(ctx)
	c.ready = true
	klog.V(2).Info("fetching redshift.scan.query_total (first scan completed)")

	for {
		select {
		case <-ctx.Done():
			klog.V(2).Infof("ctx cancelled, bye collector")
			return
		case <-time.After(time.Second * 120):
			c.updateQueryTotal(ctx)
		}
	}
}

func (c *RedshiftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queryTotalMetric
}

func (c *RedshiftCollector) Collect(ch chan<- prometheus.Metric) {
	for !c.ready {
		klog.V(2).Infof("waiting for the scan query to be ready")
		time.Sleep(10 * time.Second)
	}

	loaded, ok := c.queryTotal.Load("")
	if !ok {
		klog.Warningf("unexpected empty load for queryTotal")
		return
	}
	queryTotalRows := loaded.([]QueryTotalRow)

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
