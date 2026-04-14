package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pgredis "go-dcp-pg-redis"
	"go-dcp-pg-redis/config"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertUpdateDelete(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_crud_test"
	cfg.Postgres.Publication.Name = "pub_crud_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "user:", KeyColumn: "id", StorageType: "json"},
	}
	cfg.Redis.BatchTickerDuration = 1 * time.Second
	cfg.Redis.MaxBatchSize = 100

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

	t.Run("Insert", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			err := pgExec(ctx, conn, fmt.Sprintf(
				"INSERT INTO users(id, name, email, age) VALUES(%d, 'user-%d', 'user%d@test.com', %d)",
				i, i, i, 20+i,
			))
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			keys, _ := RedisClient.Keys(ctx, "user:*").Result()
			return len(keys) == 5
		}, 15*time.Second, 500*time.Millisecond, "expected 5 keys in redis")

		val, err := RedisClient.Get(ctx, "user:1").Result()
		require.NoError(t, err)

		var row map[string]any
		require.NoError(t, json.Unmarshal([]byte(val), &row))
		assert.Equal(t, "user-1", row["name"])
		assert.Equal(t, "user1@test.com", row["email"])
	})

	t.Run("Update", func(t *testing.T) {
		err := pgExec(ctx, conn, "UPDATE users SET name = 'updated-user' WHERE id = 1")
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			val, err := RedisClient.Get(ctx, "user:1").Result()
			if err != nil {
				return false
			}
			var row map[string]any
			_ = json.Unmarshal([]byte(val), &row)
			return row["name"] == "updated-user"
		}, 15*time.Second, 500*time.Millisecond, "expected user:1 to be updated")
	})

	t.Run("Delete", func(t *testing.T) {
		err := pgExec(ctx, conn, "DELETE FROM users WHERE id = 1")
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			exists, _ := RedisClient.Exists(ctx, "user:1").Result()
			return exists == 0
		}, 15*time.Second, 500*time.Millisecond, "expected user:1 to be deleted from redis")

		keys, _ := RedisClient.Keys(ctx, "user:*").Result()
		assert.Equal(t, 4, len(keys))
	})
}

func TestBulkInsert(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_bulk_test"
	cfg.Postgres.Publication.Name = "pub_bulk_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "bulk:", KeyColumn: "id", StorageType: "json"},
	}
	cfg.Redis.BatchTickerDuration = 2 * time.Second
	cfg.Redis.MaxBatchSize = 50

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

	count := 100
	for i := 1; i <= count; i++ {
		err := pgExec(ctx, conn, fmt.Sprintf(
			"INSERT INTO users(id, name, email) VALUES(%d, 'bulk-user-%d', 'bulk%d@test.com')",
			i, i, i,
		))
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		keys, _ := RedisClient.Keys(ctx, "bulk:*").Result()
		return len(keys) == count
	}, 30*time.Second, 1*time.Second, "expected %d keys in redis", count)
}
