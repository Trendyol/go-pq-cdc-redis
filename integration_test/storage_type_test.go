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

func TestStorageTypeJSON(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_json_test"
	cfg.Postgres.Publication.Name = "pub_json_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "json:", KeyColumn: "id", StorageType: "json"},
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

	require.NoError(t, pgExec(ctx, conn, "INSERT INTO users(id, name, email, age) VALUES(1, 'alice', 'alice@test.com', 30)"))

	require.Eventually(t, func() bool {
		return RedisClient.Exists(ctx, "json:1").Val() == 1
	}, 15*time.Second, 500*time.Millisecond)

	val, _ := RedisClient.Get(ctx, "json:1").Result()
	var row map[string]any
	require.NoError(t, json.Unmarshal([]byte(val), &row))
	assert.Equal(t, "alice", row["name"])
	assert.Equal(t, "alice@test.com", row["email"])
	assert.Equal(t, float64(30), row["age"])
}

func TestStorageTypeString(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_string_test"
	cfg.Postgres.Publication.Name = "pub_string_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "str:", KeyColumn: "id", StorageType: "string"},
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

	require.NoError(t, pgExec(ctx, conn, "INSERT INTO users(id, name, email, age) VALUES(1, 'alice', 'alice@test.com', 30)"))

	require.Eventually(t, func() bool {
		return RedisClient.Exists(ctx, "str:1").Val() == 1
	}, 15*time.Second, 500*time.Millisecond)

	val, _ := RedisClient.Get(ctx, "str:1").Result()
	assert.Equal(t, "1", val)
}

func TestStorageTypeHash(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_hash_test"
	cfg.Postgres.Publication.Name = "pub_hash_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "hash:", KeyColumn: "id", StorageType: "hash", HashField: "data"},
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

	require.NoError(t, pgExec(ctx, conn, "INSERT INTO users(id, name, email, age) VALUES(1, 'alice', 'alice@test.com', 30)"))

	require.Eventually(t, func() bool {
		return RedisClient.Exists(ctx, "hash:1").Val() == 1
	}, 15*time.Second, 500*time.Millisecond)

	val, _ := RedisClient.HGet(ctx, "hash:1", "data").Result()
	var row map[string]any
	require.NoError(t, json.Unmarshal([]byte(val), &row))
	assert.Equal(t, "alice", row["name"])

	t.Run("HashDelete", func(t *testing.T) {
		require.NoError(t, pgExec(ctx, conn, "DELETE FROM users WHERE id = 1"))

		require.Eventually(t, func() bool {
			exists, _ := RedisClient.HExists(ctx, "hash:1", "data").Result()
			return !exists
		}, 15*time.Second, 500*time.Millisecond)
	})
}

func TestStorageTypeTTL(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_ttl_test"
	cfg.Postgres.Publication.Name = "pub_ttl_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "ttl:", KeyColumn: "id", StorageType: "json", TTL: 3 * time.Second},
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

	require.NoError(t, pgExec(ctx, conn, "INSERT INTO users(id, name) VALUES(1, 'temp-user')"))

	require.Eventually(t, func() bool {
		return RedisClient.Exists(ctx, "ttl:1").Val() == 1
	}, 15*time.Second, 500*time.Millisecond)

	ttl, _ := RedisClient.TTL(ctx, "ttl:1").Result()
	assert.True(t, ttl > 0 && ttl <= 3*time.Second, "expected TTL between 0 and 3s, got %v", ttl)

	time.Sleep(4 * time.Second)
	exists, _ := RedisClient.Exists(ctx, "ttl:1").Result()
	assert.Equal(t, int64(0), exists, "key should have expired")
}

func TestKeyPrefixSuffix(t *testing.T) {
	ctx := context.Background()

	cfg := TestConfig
	cfg.Postgres.Slot.Name = "slot_prefix_test"
	cfg.Postgres.Publication.Name = "pub_prefix_test"
	cfg.Postgres.Publication.Tables = publication.Tables{
		{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
	}
	cfg.Redis.TableKeyMapping = []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "pre:", KeySuffix: ":suf", KeyColumn: "id", StorageType: "json"},
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

	require.NoError(t, pgExec(ctx, conn, fmt.Sprintf("INSERT INTO users(id, name) VALUES(42, 'prefixed')")))

	require.Eventually(t, func() bool {
		return RedisClient.Exists(ctx, "pre:42:suf").Val() == 1
	}, 15*time.Second, 500*time.Millisecond, "expected key pre:42:suf to exist")
}
