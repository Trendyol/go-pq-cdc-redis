package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pgredis "go-dcp-pg-redis"
	pgconfig "go-dcp-pg-redis/config"
	"go-dcp-pg-redis/postgres"
	"go-dcp-pg-redis/redis"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCustomMapper(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_custom_mapper_test"
	cfg.Postgres.Publication.Name = "pub_custom_mapper_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []pgconfig.TableKeyMapping{
		{Schema: "public", Table: "users", KeyColumn: "id"},
	}
	cfg.Redis.BatchTickerDuration = 1 * time.Second

	conn, err := newPostgresConn(cfg.Postgres)
	require.NoError(t, err)
	require.NoError(t, setupUsersTable(ctx, conn))
	_ = pgExec(ctx, conn, "DROP PUBLICATION IF EXISTS "+cfg.Postgres.Publication.Name)
	flushRedis()

	mapper := func(ctx postgres.Context) []redis.Model {
		e := ctx.Event
		if e.IsInsert || e.IsUpdate {
			if e.NewRow == nil {
				return nil
			}
			id, err := e.KeyFromRow("id", e.NewRow)
			if err != nil {
				return nil
			}
			b, _ := json.Marshal(e.NewRow)
			return []redis.Model{
				&redis.Set{Key: "custom:" + id, Value: string(b)},
				&redis.HSet{Key: "custom:index", Field: id, Value: e.NewRow["name"]},
			}
		}
		if e.IsDelete {
			if e.OldRow == nil {
				return nil
			}
			id, err := e.KeyFromRow("id", e.OldRow)
			if err != nil {
				return nil
			}
			return []redis.Model{
				&redis.Del{Key: "custom:" + id},
				&redis.HDel{Key: "custom:index", Field: id},
			}
		}
		return nil
	}

	connector, err := pgredis.NewConnectorBuilder(&cfg).
		SetMapper(mapper).
		Build(ctx)
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

	t.Run("InsertProducesMultipleCommands", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, conn, "INSERT INTO users(id, name, email) VALUES(1, 'mapper-user', 'test@test.com')"))

		require.Eventually(t, func() bool {
			return RedisClient.Exists(ctx, "custom:1").Val() == 1
		}, 15*time.Second, 500*time.Millisecond)

		val, _ := RedisClient.Get(ctx, "custom:1").Result()
		var row map[string]any
		require.NoError(t, json.Unmarshal([]byte(val), &row))
		assert.Equal(t, "mapper-user", row["name"])

		indexVal, _ := RedisClient.HGet(ctx, "custom:index", "1").Result()
		assert.Equal(t, "mapper-user", indexVal)
	})

	t.Run("DeleteCleansMultipleKeys", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, conn, "DELETE FROM users WHERE id = 1"))

		require.Eventually(t, func() bool {
			return RedisClient.Exists(ctx, "custom:1").Val() == 0
		}, 15*time.Second, 500*time.Millisecond)

		exists, _ := RedisClient.HExists(ctx, "custom:index", "1").Result()
		assert.False(t, exists)
	})
}
