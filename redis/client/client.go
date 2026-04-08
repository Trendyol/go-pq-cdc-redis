package client

import (
	"context"
	"fmt"

	"go-dcp-pg-redis/config"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	redis.Cmdable
	Pipeline() redis.Pipeliner
	Close() error
}

type clientWrapper struct {
	*redis.Client
}

func (c *clientWrapper) Close() error {
	return c.Client.Close()
}

type clusterWrapper struct {
	*redis.ClusterClient
}

func (c *clusterWrapper) Close() error {
	return c.ClusterClient.Close()
}

func NewRedisClient(cfg config.Redis) (RedisClient, error) {
	var cli RedisClient
	switch {
	case cfg.Cluster != nil && len(cfg.Cluster.Addrs) > 0:
		cli = newClusterClient(cfg)
	case cfg.Sentinel != nil && len(cfg.Sentinel.SentinelAddrs) > 0:
		cli = newSentinelClient(cfg)
	default:
		cli = newStandaloneClient(cfg)
	}
	if _, err := cli.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return cli, nil
}

func newStandaloneClient(cfg config.Redis) RedisClient {
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	return &clientWrapper{redis.NewClient(opts)}
}

func newSentinelClient(cfg config.Redis) RedisClient {
	opts := &redis.FailoverOptions{
		MasterName:    cfg.Sentinel.MasterName,
		SentinelAddrs: cfg.Sentinel.SentinelAddrs,
		DB:            cfg.DB,
	}
	if cfg.Sentinel.Username != "" {
		opts.Username = cfg.Sentinel.Username
	} else if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Sentinel.Password != "" {
		opts.Password = cfg.Sentinel.Password
	} else if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	return &clientWrapper{redis.NewFailoverClient(opts)}
}

func newClusterClient(cfg config.Redis) RedisClient {
	opts := &redis.ClusterOptions{Addrs: cfg.Cluster.Addrs}
	if cfg.Cluster.Username != "" {
		opts.Username = cfg.Cluster.Username
	} else if cfg.Username != "" {
		opts.Username = cfg.Username
	}
	if cfg.Cluster.Password != "" {
		opts.Password = cfg.Cluster.Password
	} else if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	opts.RouteByLatency = cfg.Cluster.RouteByLatency
	opts.RouteRandomly = cfg.Cluster.RouteRandomly
	opts.ReadOnly = cfg.Cluster.ReadOnly
	return &clusterWrapper{redis.NewClusterClient(opts)}
}
