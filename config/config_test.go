package config

import (
	"testing"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

func validBaseConnector() *Connector {
	return &Connector{
		Postgres: cdcconfig.Config{
			Host:     "localhost",
			Username: "test",
			Password: "test",
			Database: "testdb",
			Publication: publication.Config{
				Name: "test_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{{
					Name:            "t",
					ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{
				Name:                        "test_slot",
				SlotActivityCheckerInterval: 1000,
			},
		},
		Redis: Redis{
			Host: "localhost",
		},
	}
}

func TestApplyDefaults(t *testing.T) {
	c := &Connector{}
	c.ApplyDefaults()

	if c.Redis.Port != 6379 {
		t.Fatalf("expected port 6379, got %d", c.Redis.Port)
	}
	if c.Redis.BatchTickerDuration != 10*time.Second {
		t.Fatalf("expected batchTickerDuration 10s, got %v", c.Redis.BatchTickerDuration)
	}
	if c.Redis.MaxBatchSize != 1000 {
		t.Fatalf("expected maxBatchSize 1000, got %d", c.Redis.MaxBatchSize)
	}
}

func TestApplyDefaults_StorageType(t *testing.T) {
	c := &Connector{
		Redis: Redis{
			TableKeyMapping: []TableKeyMapping{
				{Table: "t1", KeyColumn: "id"},
				{Table: "t2", KeyColumn: "id", StorageType: "hash"},
			},
		},
	}
	c.ApplyDefaults()

	if c.Redis.TableKeyMapping[0].StorageType != "string" {
		t.Fatalf("expected default storageType 'string', got %q", c.Redis.TableKeyMapping[0].StorageType)
	}
	if c.Redis.TableKeyMapping[1].StorageType != "hash" {
		t.Fatalf("expected storageType 'hash', got %q", c.Redis.TableKeyMapping[1].StorageType)
	}
}

func TestApplyDefaults_Schema(t *testing.T) {
	c := &Connector{
		Redis: Redis{
			TableKeyMapping: []TableKeyMapping{
				{Table: "t1", KeyColumn: "id"},
				{Table: "t2", KeyColumn: "id", Schema: "custom"},
			},
		},
	}
	c.ApplyDefaults()

	if c.Redis.TableKeyMapping[0].Schema != "public" {
		t.Fatalf("expected default schema 'public', got %q", c.Redis.TableKeyMapping[0].Schema)
	}
	if c.Redis.TableKeyMapping[1].Schema != "custom" {
		t.Fatalf("expected schema 'custom', got %q", c.Redis.TableKeyMapping[1].Schema)
	}
}

func TestApplyDefaults_DefaultTTL(t *testing.T) {
	c := &Connector{
		Redis: Redis{
			DefaultTTL: 5 * time.Minute,
			TableKeyMapping: []TableKeyMapping{
				{Table: "t1", KeyColumn: "id"},
				{Table: "t2", KeyColumn: "id", TTL: 10 * time.Minute},
			},
		},
	}
	c.ApplyDefaults()

	if c.Redis.TableKeyMapping[0].TTL != 5*time.Minute {
		t.Fatalf("expected inherited defaultTTL 5m, got %v", c.Redis.TableKeyMapping[0].TTL)
	}
	if c.Redis.TableKeyMapping[1].TTL != 10*time.Minute {
		t.Fatalf("expected explicit TTL 10m, got %v", c.Redis.TableKeyMapping[1].TTL)
	}
}

func TestValidate_NoHost(t *testing.T) {
	c := &Connector{
		Redis: Redis{},
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error for missing host")
	}
}

func TestValidate_ClusterNoHost(t *testing.T) {
	c := &Connector{
		Redis: Redis{
			Cluster: &RedisCluster{Addrs: []string{"node1:6379"}},
		},
	}
	err := c.Validate()
	if err == errRedisHostOrCluster {
		t.Fatal("cluster config should not require host")
	}
}

func TestValidate_SentinelNoHost(t *testing.T) {
	c := &Connector{
		Redis: Redis{
			Sentinel: &RedisSentinel{
				MasterName:    "mymaster",
				SentinelAddrs: []string{"sentinel1:26379"},
			},
		},
	}
	err := c.Validate()
	if err == errRedisHostOrCluster {
		t.Fatal("sentinel config should not require host")
	}
}

func TestValidate_MissingTableMapping(t *testing.T) {
	c := validBaseConnector()
	c.Redis.TableKeyMapping = []TableKeyMapping{{Table: "", KeyColumn: "id"}}
	err := c.Validate()
	if err != errTableMapping {
		t.Fatalf("expected errTableMapping, got %v", err)
	}
}

func TestValidate_MissingKeyColumn(t *testing.T) {
	c := validBaseConnector()
	c.Redis.TableKeyMapping = []TableKeyMapping{{Table: "users", KeyColumn: ""}}
	err := c.Validate()
	if err != errTableMapping {
		t.Fatalf("expected errTableMapping, got %v", err)
	}
}
