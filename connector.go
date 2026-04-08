package pgredis

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"

	"go-dcp-pg-redis/config"
	"go-dcp-pg-redis/metric"
	"go-dcp-pg-redis/postgres"
	"go-dcp-pg-redis/redis/bulk"
)

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
	GetCDC() cdc.Connector
}

type connector struct {
	cdc    cdc.Connector
	bulk   *bulk.Bulk
	mapper Mapper
}

func (c *connector) Start(ctx context.Context) {
	go func() {
		if err := c.WaitUntilReady(ctx); err != nil {
			slog.Error("wait until ready failed", "error", err)
			return
		}
		c.bulk.StartBatchTicker()
		slog.Info("batch ticker started")
	}()
	c.cdc.Start(ctx)
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	return c.cdc.WaitUntilReady(ctx)
}

func (c *connector) Close() {
	c.cdc.Close()
	c.bulk.Close()
	slog.Info("connector closed")
}

func (c *connector) GetCDC() cdc.Connector {
	return c.cdc
}

func (c *connector) listener(ctx *replication.ListenerContext) {
	pctx, ok := postgres.ContextFromMessage(ctx.Message)
	if !ok {
		_ = ctx.Ack()
		return
	}

	actions := c.mapper(pctx)
	if len(actions) == 0 {
		_ = ctx.Ack()
		return
	}

	t := eventTime(ctx.Message)
	c.bulk.AddActions(ctx.Ack, t, actions)
}

func eventTime(msg any) time.Time {
	switch m := msg.(type) {
	case *format.Insert:
		return m.MessageTime
	case *format.Update:
		return m.MessageTime
	case *format.Delete:
		return m.MessageTime
	case *format.Snapshot:
		return m.ServerTime
	default:
		return time.Now().UTC()
	}
}

type ConnectorBuilder struct {
	mapper Mapper
	config any
}

func newConnectorConfig(cf any) (*config.Connector, error) {
	switch v := cf.(type) {
	case *config.Connector:
		return v, nil
	case config.Connector:
		return &v, nil
	case string:
		return config.ReadConnectorYAML(v)
	default:
		return nil, errors.New("invalid config type: expected *config.Connector, config.Connector, or string (yaml path)")
	}
}

func NewConnectorBuilder(config any) ConnectorBuilder {
	return ConnectorBuilder{
		config: config,
		mapper: DefaultMapper,
	}
}

func (b ConnectorBuilder) SetMapper(mapper Mapper) ConnectorBuilder {
	b.mapper = mapper
	return b
}

func (b ConnectorBuilder) Build() (Connector, error) {
	cfg, err := newConnectorConfig(b.config)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	SetTableKeyMappings(cfg.Redis.TableKeyMapping)
	printRedisConfig(cfg.Redis)

	bulkLayer, err := bulk.NewBulk(cfg)
	if err != nil {
		return nil, err
	}

	c := &connector{
		mapper: b.mapper,
		bulk:   bulkLayer,
	}

	cdcConn, err := cdc.NewConnector(context.Background(), cfg.Postgres, c.listener)
	if err != nil {
		bulkLayer.Close()
		return nil, err
	}

	collector := metric.NewCollector(bulkLayer)
	cdcConn.SetMetricCollectors(collector)

	c.cdc = cdcConn
	return c, nil
}

func printRedisConfig(cfg config.Redis) {
	cp := cfg
	cp.Password = "*****"
	if cp.Cluster != nil {
		cp.Cluster.Password = "*****"
	}
	if cp.Sentinel != nil {
		cp.Sentinel.Password = "*****"
	}
	b, _ := json.Marshal(cp)
	slog.Info("redis config", "config", string(b))
}
