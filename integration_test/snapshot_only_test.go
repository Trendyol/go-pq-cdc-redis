package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pgredis "go-dcp-pg-redis"
	"go-dcp-pg-redis/config"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotOnly(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_snapshot_only_test"
	cfg.Postgres.Publication.Name = "pub_snapshot_only_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Postgres.Snapshot = cdcconfig.SnapshotConfig{
		Enabled:   true,
		Mode:      "snapshot_only",
		ChunkSize: 1000,
		Tables: publication.Tables{
			{Name: "users", Schema: "public"},
		},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "snap:", KeyColumn: "id", StorageType: "json"},
	}
	cfg.Redis.BatchTickerDuration = 1 * time.Second

	conn, err := newPostgresConn(cfg.Postgres)
	require.NoError(t, err)
	require.NoError(t, setupUsersTable(ctx, conn))
	_ = pgExec(ctx, conn, "DROP PUBLICATION IF EXISTS "+cfg.Postgres.Publication.Name)
	flushRedis()

	rowCount := 20
	for i := 1; i <= rowCount; i++ {
		require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(
			"INSERT INTO users(id, name, email, age) VALUES(%d, 'snap-user-%d', 'snap%d@test.com', %d)",
			i, i, i, 20+i,
		)))
	}

	connector, err := pgredis.NewConnectorBuilder(&cfg).Build(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS cdc_snapshot_job"))
		_ = pgExec(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS cdc_snapshot_chunks"))
		dropSlotAndPublication(ctx, conn, cfg.Postgres.Slot.Name, cfg.Postgres.Publication.Name)
		conn.Close(ctx)
	})

	completeCh := make(chan struct{})
	go func() {
		connector.Start(ctx)
		close(completeCh)
	}()

	select {
	case <-completeCh:
		t.Log("snapshot-only mode completed")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for snapshot-only to complete")
	}

	// Allow batch ticker flush
	time.Sleep(3 * time.Second)

	keys, _ := RedisClient.Keys(ctx, "snap:*").Result()
	assert.Equal(t, rowCount, len(keys), "expected %d keys after snapshot, got %d", rowCount, len(keys))

	val, err := RedisClient.Get(ctx, "snap:1").Result()
	require.NoError(t, err)
	var row map[string]any
	require.NoError(t, json.Unmarshal([]byte(val), &row))
	assert.Equal(t, "snap-user-1", row["name"])
	assert.Equal(t, "snap1@test.com", row["email"])
}
