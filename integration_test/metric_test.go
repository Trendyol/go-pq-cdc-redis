package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pgredis "go-dcp-pg-redis"
	"go-dcp-pg-redis/config"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsEndpoint(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_metric_test"
	cfg.Postgres.Publication.Name = "pub_metric_test"
	cfg.Postgres.Metric.Port = 18082
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "metric:", KeyColumn: "id", StorageType: "json"},
	}
	cfg.Redis.BatchTickerDuration = 1 * time.Second

	conn, err := newPostgresConn(cfg.Postgres)
	require.NoError(t, err)
	require.NoError(t, setupUsersTable(ctx, conn))
	_ = pgExec(ctx, conn, "DROP PUBLICATION IF EXISTS "+cfg.Postgres.Publication.Name)
	flushRedis()

	connector, err := pgredis.NewConnectorBuilder(&cfg).Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		dropSlotAndPublication(ctx, conn, cfg.Postgres.Slot.Name, cfg.Postgres.Publication.Name)
		conn.Close(ctx)
	})

	go connector.Start(ctx)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	for i := 1; i <= 3; i++ {
		require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(
			"INSERT INTO users(id, name) VALUES(%d, 'metric-user-%d')", i, i,
		)))
	}

	require.Eventually(t, func() bool {
		keys, _ := RedisClient.Keys(ctx, "metric:*").Result()
		return len(keys) == 3
	}, 15*time.Second, 500*time.Millisecond)

	t.Run("MetricsAvailable", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", cfg.Postgres.Metric.Port))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, _ := io.ReadAll(resp.Body)
		bodyStr := string(body)

		assert.True(t, strings.Contains(bodyStr, "pg_redis_connector_latency_ms"), "latency metric missing")
		assert.True(t, strings.Contains(bodyStr, "pg_redis_connector_bulk_request_process_latency_ms"), "bulk latency metric missing")
		assert.True(t, strings.Contains(bodyStr, "pg_redis_connector_batch_size"), "batch size metric missing")
		assert.True(t, strings.Contains(bodyStr, "pg_redis_connector_flush_success_total"), "flush success metric missing")
		assert.True(t, strings.Contains(bodyStr, "pg_redis_connector_processed_total"), "processed total metric missing")
	})

	t.Run("StatusEndpoint", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/status", cfg.Postgres.Metric.Port))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
