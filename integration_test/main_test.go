package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"go-dcp-pg-redis/config"
)

var (
	TestConfig        config.Connector
	PostgresContainer testcontainers.Container
	RedisContainer    testcontainers.Container
	RedisClient       *goredis.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	var err error

	PostgresContainer, err = setupPostgresContainer(ctx)
	if err != nil {
		log.Fatal("setup postgres container: ", err)
	}
	defer PostgresContainer.Terminate(ctx)

	RedisContainer, err = setupRedisContainer(ctx)
	if err != nil {
		log.Fatal("setup redis container: ", err)
	}
	defer RedisContainer.Terminate(ctx)

	pgPort, _ := PostgresContainer.MappedPort(ctx, "5432/tcp")
	redisPort, _ := RedisContainer.MappedPort(ctx, "6379/tcp")

	TestConfig = config.Connector{
		Postgres: cdcconfig.Config{
			Host:     "localhost",
			Port:     pgPort.Int(),
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "test_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationUpdate,
					publication.OperationDelete,
				},
				Tables: publication.Tables{
					{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull, Schema: "public"},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "test_slot",
				SlotActivityCheckerInterval: 1000,
			},
			Metric: cdcconfig.MetricConfig{Port: 18081},
		},
		Redis: config.Redis{
			Host: "localhost",
			Port: uint16(redisPort.Int()),
			TableKeyMapping: []config.TableKeyMapping{
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
	TestConfig.ApplyDefaults()

	conn, err := newPostgresConn(TestConfig.Postgres)
	if err != nil {
		log.Fatal("postgres conn: ", err)
	}
	defer conn.Close(ctx)

	if err := createCDCUser(ctx, conn, TestConfig.Postgres); err != nil {
		log.Fatal("create cdc user: ", err)
	}

	RedisClient = goredis.NewClient(&goredis.Options{
		Addr: fmt.Sprintf("localhost:%d", redisPort.Int()),
	})

	os.Exit(m.Run())
}

func setupPostgresContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "docker.io/postgres:16-alpine",
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_DB":       "cdc_db",
			},
			ExposedPorts: []string{"5432/tcp"},
			Cmd:          []string{"postgres", "-c", "fsync=off", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"},
		},
		Started: true,
	}
	err := testcontainers.WithWaitStrategy(
		wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	).Customize(&req)
	if err != nil {
		return nil, err
	}
	return testcontainers.GenericContainer(ctx, req)
}

func setupRedisContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "docker.io/redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
		},
		Started: true,
	}
	err := testcontainers.WithWaitStrategy(
		wait.ForLog("Ready to accept connections").
			WithStartupTimeout(10 * time.Second),
	).Customize(&req)
	if err != nil {
		return nil, err
	}
	return testcontainers.GenericContainer(ctx, req)
}

func newPostgresConn(cfg cdcconfig.Config) (pq.Connection, error) {
	c := cdcconfig.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Username: "postgres",
		Password: "postgres",
		Database: cfg.Database,
	}
	return pq.NewConnection(context.TODO(), c.DSN())
}

func createCDCUser(ctx context.Context, conn pq.Connection, cfg cdcconfig.Config) error {
	commands := []string{
		fmt.Sprintf("CREATE USER %s WITH SUPERUSER REPLICATION PASSWORD '%s';", cfg.Username, cfg.Password),
		fmt.Sprintf("GRANT CONNECT, CREATE ON DATABASE %s TO %s;", cfg.Database, cfg.Username),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s;", cfg.Username),
	}
	for _, cmd := range commands {
		if err := pgExec(ctx, conn, cmd); err != nil {
			return err
		}
	}
	return nil
}

func pgExec(ctx context.Context, conn pq.Connection, command string) error {
	rr := conn.Exec(ctx, command)
	if _, err := rr.ReadAll(); err != nil {
		return err
	}
	return rr.Close()
}

func setupUsersTable(ctx context.Context, conn pq.Connection) error {
	return pgExec(ctx, conn, `
		DROP TABLE IF EXISTS users;
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			age INT DEFAULT 0
		);
	`)
}

func dropSlotAndPublication(ctx context.Context, conn pq.Connection, slotName, pubName string) {
	_ = pgExec(ctx, conn, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	_ = pgExec(ctx, conn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
}

func flushRedis() {
	RedisClient.FlushAll(context.Background())
}
