package main

import (
	"context"
	"log/slog"
	"os"

	pgredis "go-dcp-pg-redis"
)

func main() {
	ctx := context.Background()
	conn, err := pgredis.NewConnectorBuilder("config.yml").Build(ctx)
	if err != nil {
		slog.Error("connector build failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	conn.Start(context.Background())
}
