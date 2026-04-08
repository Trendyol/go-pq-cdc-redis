package config

import (
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
)

type Connector struct {
	Postgres cdcconfig.Config `json:"postgres" yaml:"postgres"`
	Redis    Redis            `json:"redis" yaml:"redis"`
}

type Redis struct {
	Host                string            `json:"host" yaml:"host"`
	Port                uint16            `json:"port" yaml:"port"`
	Username            string            `json:"username" yaml:"username"`
	Password            string            `json:"password" yaml:"password"`
	DB                  int               `json:"db" yaml:"db"`
	TableKeyMapping     []TableKeyMapping `json:"tableKeyMapping" yaml:"tableKeyMapping"`
	BatchTickerDuration time.Duration     `json:"batchTickerDuration" yaml:"batchTickerDuration"`
	MaxBatchSize        int               `json:"maxBatchSize" yaml:"maxBatchSize"`
	DefaultTTL          time.Duration     `json:"defaultTTL" yaml:"defaultTTL"`
	Sentinel            *RedisSentinel    `json:"sentinel,omitempty" yaml:"sentinel,omitempty"`
	Cluster             *RedisCluster     `json:"cluster,omitempty" yaml:"cluster,omitempty"`
}

type RedisSentinel struct {
	MasterName    string   `json:"masterName" yaml:"masterName"`
	SentinelAddrs []string `json:"sentinelAddrs" yaml:"sentinelAddrs"`
	Username      string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password      string   `json:"password,omitempty" yaml:"password,omitempty"`
}

type RedisCluster struct {
	Addrs          []string `json:"addrs" yaml:"addrs"`
	Username       string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password       string   `json:"password,omitempty" yaml:"password,omitempty"`
	RouteByLatency bool     `json:"routeByLatency,omitempty" yaml:"routeByLatency,omitempty"`
	RouteRandomly  bool     `json:"routeRandomly,omitempty" yaml:"routeRandomly,omitempty"`
	ReadOnly       bool     `json:"readOnly,omitempty" yaml:"readOnly,omitempty"`
}

type TableKeyMapping struct {
	Schema      string        `json:"schema" yaml:"schema"`
	Table       string        `json:"table" yaml:"table"`
	KeyPrefix   string        `json:"keyPrefix" yaml:"keyPrefix"`
	KeySuffix   string        `json:"keySuffix" yaml:"keySuffix"`
	KeyColumn   string        `json:"keyColumn" yaml:"keyColumn"`
	StorageType string        `json:"storageType" yaml:"storageType"`
	TTL         time.Duration `json:"ttl" yaml:"ttl"`
	HashField   string        `json:"hashField,omitempty" yaml:"hashField,omitempty"`
}

func (c *Connector) ApplyDefaults() {
	c.Postgres.SetDefault()
	if c.Redis.Port == 0 {
		c.Redis.Port = 6379
	}
	if c.Redis.BatchTickerDuration == 0 {
		c.Redis.BatchTickerDuration = 10 * time.Second
	}
	if c.Redis.MaxBatchSize <= 0 {
		c.Redis.MaxBatchSize = 1000
	}
	for i := range c.Redis.TableKeyMapping {
		if c.Redis.TableKeyMapping[i].StorageType == "" {
			c.Redis.TableKeyMapping[i].StorageType = "string"
		}
		if c.Redis.TableKeyMapping[i].Schema == "" {
			c.Redis.TableKeyMapping[i].Schema = "public"
		}
		if c.Redis.TableKeyMapping[i].TTL == 0 && c.Redis.DefaultTTL > 0 {
			c.Redis.TableKeyMapping[i].TTL = c.Redis.DefaultTTL
		}
	}
}

func (c *Connector) Validate() error {
	if err := c.Postgres.Validate(); err != nil {
		return err
	}
	r := &c.Redis
	switch {
	case r.Cluster != nil && len(r.Cluster.Addrs) > 0:
	case r.Sentinel != nil && len(r.Sentinel.SentinelAddrs) > 0:
	default:
		if r.Host == "" {
			return errRedisHostOrCluster
		}
	}
	for _, m := range r.TableKeyMapping {
		if m.Table == "" || m.KeyColumn == "" {
			return errTableMapping
		}
	}
	return nil
}
