package metric

import (
	"go-dcp-pg-redis/redis/bulk"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	RedisConnectorLatency            = "pg_redis_connector_latency_ms"
	RedisConnectorBulkRequestLatency = "pg_redis_connector_bulk_request_process_latency_ms"
	RedisConnectorBatchSize          = "pg_redis_connector_batch_size"
)

type Collector struct {
	bulkLayer                 *bulk.Bulk
	redisConnectorLatency     prometheus.Gauge
	redisConnectorBulkLatency prometheus.Gauge
	redisConnectorBatchSize   prometheus.Gauge
}

func NewCollector(b *bulk.Bulk) *Collector {
	return &Collector{
		bulkLayer: b,
		redisConnectorLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorLatency,
			Help: "End-to-end latency from CDC event to Redis write (ms).",
		}),
		redisConnectorBulkLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorBulkRequestLatency,
			Help: "Redis pipeline execution latency (ms).",
		}),
		redisConnectorBatchSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorBatchSize,
			Help: "Number of events in the last flushed batch.",
		}),
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	m := c.bulkLayer.GetMetric()
	c.redisConnectorLatency.Set(float64(m.ProcessLatencyMs))
	c.redisConnectorBulkLatency.Set(float64(m.BulkRequestProcessLatencyMs))
	c.redisConnectorBatchSize.Set(float64(m.CurrentBatchSize))
	c.redisConnectorLatency.Collect(ch)
	c.redisConnectorBulkLatency.Collect(ch)
	c.redisConnectorBatchSize.Collect(ch)
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	c.redisConnectorLatency.Describe(ch)
	c.redisConnectorBulkLatency.Describe(ch)
	c.redisConnectorBatchSize.Describe(ch)
}
