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
	clients          []*Redshift
	queryTotalMetric *prometheus.Desc

	ready      bool
	queryTotal sync.Map
}

func NewRedshiftCollector(clients []*Redshift) *RedshiftCollector {
	return &RedshiftCollector{
		clients: clients,
		queryTotalMetric: prometheus.NewDesc(
			prometheus.BuildFQName(Namespace, SubSystemScan, "query_total"),
			"Total number of redshift queries executed",
			[]string{"database", "schema", "tablename", "tableid"},
			nil,
		),
	}
}

func (c *RedshiftCollector) updateQueryTotal(ctx context.Context) {
	var rows []QueryTotalRow
	for i, client := range c.clients {
		klog.V(3).Infof("fetching query_total for database:%v", i)
		dbRows, err := client.ScanQueryTotal(ctx)
		if err != nil {
			klog.Fatalf("Redshift Collector shutdown due to error: %v", err)
		}
		rows = append(rows, dbRows...)
	}

	c.queryTotal.Store("", rows)
}

func (c *RedshiftCollector) Fetch(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	klog.V(2).Info("fetching query_total (first scan)")
	c.updateQueryTotal(ctx)
	c.ready = true

	for {
		select {
		case <-ctx.Done():
			klog.V(2).Infof("ctx cancelled, bye collector")
			return
		case <-time.After(time.Second * 300):
			klog.V(2).Info("fetching query_total (every 120s)")
			c.updateQueryTotal(ctx)
			klog.V(4).Info("fetch query_total complete")
		}
	}
}

func (c *RedshiftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queryTotalMetric
}

func (c *RedshiftCollector) Collect(ch chan<- prometheus.Metric) {
	for !c.ready {
		klog.V(2).Info("waiting for the first query_total fetch to complete...")
		return
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
			prometheus.GaugeValue,
			queryTotalRow.QueryTotal,
			queryTotalRow.Database,
			queryTotalRow.Schema,
			queryTotalRow.TableName,
			queryTotalRow.TableID,
		)
	}
}
