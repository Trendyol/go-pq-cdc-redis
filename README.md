# go-dcp-pg-redis

Go implementation of PostgreSQL to Redis connector using [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc).

It streams PostgreSQL changes via logical replication (CDC) and applies them to Redis in near real-time.
Architecture and API conventions follow [go-dcp-redis](https://github.com/Trendyol/go-dcp-redis) (Couchbase → Redis connector).

## Features

- **Logical Replication** — uses PostgreSQL's built-in `pgoutput` protocol via `go-pq-cdc`
- **Snapshot Support** — initial data load before starting CDC
- **Pipeline Batching** — buffers events and flushes via Redis pipelines for high throughput
- **Configurable Batch** — `maxBatchSize` + `batchTickerDuration` control flush behaviour
- **Multiple Storage Types** — `string`, `json`, `hash` per table mapping
- **Redis Topologies** — standalone, Sentinel, and Cluster
- **Custom Mapper** — override `DefaultMapper` with any `func(postgres.Context) []redis.Model`
- **Prometheus Metrics** — latency, batch size, plus all `go-pq-cdc` built-in metrics
- **YAML / Code Config** — pass `*config.Connector`, `config.Connector`, or a YAML file path

## Usage

```
go get go-dcp-pg-redis
```

### Programmatic Config

```go
package main

import (
	"context"
	"log/slog"
	"os"

	pgredis "go-dcp-pg-redis"
	connconfig "go-dcp-pg-redis/config"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

func main() {
	cfg := &connconfig.Connector{
		Postgres: cdcconfig.Config{
			Host:     "127.0.0.1",
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{
				CreateIfNotExists: true,
				Name:              "cdc_slot_pg_redis",
			},
			Metric: cdcconfig.MetricConfig{Port: 8081},
		},
		Redis: connconfig.Redis{
			Host: "127.0.0.1",
			Port: 6379,
			TableKeyMapping: []connconfig.TableKeyMapping{{
				Table:       "users",
				KeyPrefix:   "user:",
				KeyColumn:   "id",
				StorageType: "json",
			}},
		},
	}

	conn, err := pgredis.NewConnectorBuilder(cfg).Build()
	if err != nil {
		slog.Error("build", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.Start(context.Background())
}
```

### YAML Config

```go
conn, err := pgredis.NewConnectorBuilder("config.yml").Build()
```

```yaml
postgres:
  host: 127.0.0.1
  username: cdc_user
  password: cdc_pass
  database: cdc_db
  publication:
    createIfNotExists: true
    name: cdc_publication
    operations: [INSERT, UPDATE, DELETE]
    tables:
      - name: users
        replicaIdentity: FULL
  slot:
    createIfNotExists: true
    name: cdc_slot_pg_redis
  metric:
    port: 8081

redis:
  host: 127.0.0.1
  port: 6379
  batchTickerDuration: 10s
  maxBatchSize: 1000
  tableKeyMapping:
    - table: users
      keyPrefix: "user:"
      keyColumn: id
      storageType: json
```

### Custom Mapper

```go
func myMapper(ctx postgres.Context) []redis.Model {
	e := ctx.Event
	if e.IsInsert || e.IsUpdate {
		b, _ := json.Marshal(e.NewRow)
		id, _ := e.KeyFromRow("id", e.NewRow)
		return []redis.Model{
			&redis.Set{Key: "user:" + id, Value: string(b)},
		}
	}
	if e.IsDelete {
		id, _ := e.KeyFromRow("id", e.OldRow)
		return []redis.Model{&redis.Del{Key: "user:" + id}}
	}
	return nil
}

conn, err := pgredis.NewConnectorBuilder(cfg).SetMapper(myMapper).Build()
```

## Configuration

### PostgreSQL (CDC)

All fields from [go-pq-cdc configuration](https://github.com/Trendyol/go-pq-cdc#configuration) are supported under the `postgres` key.

### Redis

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `redis.host` | string | yes* | — | Redis host. Required unless Sentinel/Cluster is configured. |
| `redis.port` | uint16 | no | `6379` | Redis port. |
| `redis.username` | string | no | — | Redis username (ACL). |
| `redis.password` | string | no | — | Redis password. |
| `redis.db` | int | no | `0` | Redis database index. |
| `redis.batchTickerDuration` | duration | no | `10s` | Interval for periodic pipeline flush. |
| `redis.maxBatchSize` | int | no | `1000` | Max events buffered before forced flush. |
| `redis.defaultTTL` | duration | no | `0` | Default TTL for keys (0 = no expiry). Applied to mappings without explicit TTL. |

### Redis Sentinel

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `redis.sentinel.masterName` | string | yes | — | Sentinel master name. |
| `redis.sentinel.sentinelAddrs` | []string | yes | — | Sentinel addresses. |
| `redis.sentinel.username` | string | no | — | Sentinel username. Falls back to `redis.username`. |
| `redis.sentinel.password` | string | no | — | Sentinel password. Falls back to `redis.password`. |

### Redis Cluster

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `redis.cluster.addrs` | []string | yes | — | Cluster node addresses. |
| `redis.cluster.username` | string | no | — | Cluster username. Falls back to `redis.username`. |
| `redis.cluster.password` | string | no | — | Cluster password. Falls back to `redis.password`. |
| `redis.cluster.routeByLatency` | bool | no | `false` | Route reads to lowest latency node. |
| `redis.cluster.routeRandomly` | bool | no | `false` | Route reads randomly across nodes. |
| `redis.cluster.readOnly` | bool | no | `false` | Enable reading from replica nodes. |

### Table Key Mapping

| Variable | Type | Required | Default | Description |
|---|---|---|---|---|
| `tableKeyMapping[].schema` | string | no | `public` | PostgreSQL schema name. |
| `tableKeyMapping[].table` | string | yes | — | PostgreSQL table name. |
| `tableKeyMapping[].keyPrefix` | string | no | — | Redis key prefix. |
| `tableKeyMapping[].keySuffix` | string | no | — | Redis key suffix. |
| `tableKeyMapping[].keyColumn` | string | yes | — | Column used as Redis key identifier. |
| `tableKeyMapping[].storageType` | string | no | `string` | Redis storage type: `string`, `json`, `hash`. |
| `tableKeyMapping[].ttl` | duration | no | `0` | Key TTL. Overrides `defaultTTL`. |
| `tableKeyMapping[].hashField` | string | no | `value` | Hash field name (only for `storageType: hash`). |

## Architecture

```
PostgreSQL WAL → go-pq-cdc (logical replication) → Listener
    → postgres.ContextFromMessage → Mapper → []redis.Model
    → Bulk (batch buffer) → Redis Pipeline → CDC Ack
```

The connector buffers CDC events and flushes them via Redis pipelines:
- **On threshold**: when buffer reaches `maxBatchSize`, immediate flush
- **On timer**: `batchTickerDuration` triggers periodic flush for low-traffic periods
- **On shutdown**: remaining buffer is flushed before closing

Only the last event's LSN is acknowledged after a successful pipeline execution, which naturally covers all preceding events (at-least-once semantics).

## Exposed Metrics

| Metric Name | Description | Value Type |
|---|---|---|
| `pg_redis_connector_latency_ms` | End-to-end latency from CDC event to Redis write | Gauge |
| `pg_redis_connector_bulk_request_process_latency_ms` | Redis pipeline execution latency | Gauge |
| `pg_redis_connector_batch_size` | Number of events in the last flushed batch | Gauge |

Plus all [go-pq-cdc metrics](https://github.com/Trendyol/go-pq-cdc#exposed-metrics).

## Examples

| Example | Description |
|---|---|
| [simple](example/simple) | Programmatic configuration |
| [yaml](example/yaml) | YAML file configuration |
| [custom-mapper](example/custom-mapper) | Custom mapper with multi-command per event |

## Development

```bash
# start dependencies
docker compose up -d

# run example
go run example/simple/main.go

# tests
go test ./...

# lint
golangci-lint run
```

## Compatibility

| go-dcp-pg-redis | go-pq-cdc | Min PostgreSQL |
|---|---|---|
| latest | v1.7.8+ | 10 (proto v1) / 14 (proto v2) |
