package metric

import (
	"go-dcp-pg-redis/redis/bulk"

	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	bulkLayer *bulk.Bulk

	processLatency     prometheus.Gauge
	bulkRequestLatency prometheus.Gauge
	batchSize          prometheus.Gauge
	flushSuccessTotal  prometheus.Counter
	flushErrorTotal    prometheus.Counter
	totalProcessed     prometheus.Counter
}

func NewCollector(b *bulk.Bulk) *Collector {
	return &Collector{
		bulkLayer: b,
		processLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pg_redis_connector_latency_ms",
			Help: "End-to-end latency from CDC event to Redis write (ms).",
		}),
		bulkRequestLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pg_redis_connector_bulk_request_process_latency_ms",
			Help: "Redis pipeline execution latency (ms).",
		}),
		batchSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pg_redis_connector_batch_size",
			Help: "Number of events in the last flushed batch.",
		}),
		flushSuccessTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pg_redis_connector_flush_success_total",
			Help: "Total number of successful pipeline flushes.",
		}),
		flushErrorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pg_redis_connector_flush_error_total",
			Help: "Total number of failed pipeline flushes.",
		}),
		totalProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pg_redis_connector_processed_total",
			Help: "Total number of CDC events successfully written to Redis.",
		}),
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	m := c.bulkLayer.GetMetric()

	c.processLatency.Set(float64(m.ProcessLatencyMs))
	c.bulkRequestLatency.Set(float64(m.BulkRequestProcessLatencyMs))
	c.batchSize.Set(float64(m.CurrentBatchSize))

	successDelta := float64(m.FlushSuccessTotal.Load())
	errorDelta := float64(m.FlushErrorTotal.Load())
	processedDelta := float64(m.TotalProcessed.Load())
	c.flushSuccessTotal.Add(0)
	c.flushErrorTotal.Add(0)
	c.totalProcessed.Add(0)

	c.processLatency.Collect(ch)
	c.bulkRequestLatency.Collect(ch)
	c.batchSize.Collect(ch)

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("pg_redis_connector_flush_success_total", "Total number of successful pipeline flushes.", nil, nil),
		prometheus.CounterValue,
		successDelta,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("pg_redis_connector_flush_error_total", "Total number of failed pipeline flushes.", nil, nil),
		prometheus.CounterValue,
		errorDelta,
	)
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc("pg_redis_connector_processed_total", "Total number of CDC events successfully written to Redis.", nil, nil),
		prometheus.CounterValue,
		processedDelta,
	)
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.processLatency.Describe(ch)
	c.bulkRequestLatency.Describe(ch)
	c.batchSize.Describe(ch)

	ch <- prometheus.NewDesc("pg_redis_connector_flush_success_total", "Total number of successful pipeline flushes.", nil, nil)
	ch <- prometheus.NewDesc("pg_redis_connector_flush_error_total", "Total number of failed pipeline flushes.", nil, nil)
	ch <- prometheus.NewDesc("pg_redis_connector_processed_total", "Total number of CDC events successfully written to Redis.", nil, nil)
}
