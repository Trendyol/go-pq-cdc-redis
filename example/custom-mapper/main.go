package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	pgredis "go-dcp-pg-redis"
	connconfig "go-dcp-pg-redis/config"
	"go-dcp-pg-redis/postgres"
	"go-dcp-pg-redis/redis"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

func main() {
	ctx := context.Background()

	cfg := &connconfig.Connector{
		Postgres: cdcconfig.Config{
			Host:      "127.0.0.1",
			Port:      5433,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					{
						Name:            "orders",
						ReplicaIdentity: publication.ReplicaIdentityFull,
						Schema:          "public",
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot_custom",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Redis: connconfig.Redis{
			Host: "127.0.0.1",
			Port: 6379,
			TableKeyMapping: []connconfig.TableKeyMapping{
				{
					Schema:    "public",
					Table:     "orders",
					KeyColumn: "id",
				},
			},
		},
	}

	conn, err := pgredis.NewConnectorBuilder(cfg).
		SetMapper(customMapper).
		Build()
	if err != nil {
		slog.Error("connector build failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	conn.Start(ctx)
}

func customMapper(ctx postgres.Context) []redis.Model {
	e := ctx.Event

	switch {
	case e.IsInsert || e.IsUpdate:
		if e.NewRow == nil {
			return nil
		}
		id, err := e.KeyFromRow("id", e.NewRow)
		if err != nil {
			slog.Error("key extraction failed", "error", err)
			return nil
		}

		b, _ := json.Marshal(e.NewRow)
		return []redis.Model{
			&redis.Set{Key: "order:" + id, Value: string(b)},
			&redis.HSet{Key: "order:status", Field: id, Value: e.NewRow["status"]},
		}

	case e.IsDelete:
		if e.OldRow == nil {
			return nil
		}
		id, err := e.KeyFromRow("id", e.OldRow)
		if err != nil {
			slog.Error("key extraction failed", "error", err)
			return nil
		}
		return []redis.Model{
			&redis.Del{Key: "order:" + id},
			&redis.HDel{Key: "order:status", Field: id},
		}
	}

	return nil
}
