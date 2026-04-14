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
						Name:            "users",
						ReplicaIdentity: publication.ReplicaIdentityFull,
						Schema:          "public",
					},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot_pg_redis",
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
					Schema:      "public",
					Table:       "users",
					KeyPrefix:   "user:",
					KeyColumn:   "id",
					StorageType: "json",
				},
			},
		},
	}

	conn, err := pgredis.NewConnectorBuilder(cfg).Build(ctx)
	if err != nil {
		slog.Error("connector build failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	conn.Start(ctx)
}
