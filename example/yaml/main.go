package main

import (
	"context"
	"log/slog"
	"os"

	pgredis "go-dcp-pg-redis"
)

func main() {
	conn, err := pgredis.NewConnectorBuilder("config.yml").Build()
	if err != nil {
		slog.Error("connector build failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	conn.Start(context.Background())
}
