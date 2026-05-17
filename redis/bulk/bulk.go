package bulk

import (
	"context"
	"go-dcp-pg-redis/config"
	"go-dcp-pg-redis/redis"
	"go-dcp-pg-redis/redis/client"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Bulk struct {
	redisClient     client.RedisClient
	responseHandler ResponseHandler
	metric          *Metric
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
	buffer          []batchEntry
	maxBatchSize    int
	ticker          *time.Ticker
	wg              sync.WaitGroup
}

type batchEntry struct {
	ack       func() error
	actions   []redis.Model
	eventTime time.Time
}

type Metric struct {
	ProcessLatencyMs            int64
	BulkRequestProcessLatencyMs int64
	CurrentBatchSize            int64
	FlushSuccessTotal           atomic.Int64
	FlushErrorTotal             atomic.Int64
	TotalProcessed              atomic.Int64
}

func NewBulk(cfg *config.Connector, responseHandler ResponseHandler) (*Bulk, error) {
	c, err := client.NewRedisClient(cfg.Redis)
	if err != nil {
		return nil, err
	}
	if responseHandler == nil {
		responseHandler = &DefaultResponseHandler{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Bulk{
		redisClient:     c,
		responseHandler: responseHandler,
		metric:          &Metric{},
		ctx:             ctx,
		cancel:          cancel,
		buffer:          make([]batchEntry, 0, cfg.Redis.MaxBatchSize),
		maxBatchSize:    cfg.Redis.MaxBatchSize,
		ticker:          time.NewTicker(cfg.Redis.BatchTickerDuration),
	}, nil
}

func (b *Bulk) GetMetric() *Metric {
	return b.metric
}

func (b *Bulk) GetRedisClient() client.RedisClient {
	return b.redisClient
}

func (b *Bulk) StartBatchTicker() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.ticker.C:
				b.mu.Lock()
				if len(b.buffer) > 0 {
					b.flushLocked()
				}
				b.mu.Unlock()
			case <-b.ctx.Done():
				return
			}
		}
	}()
}

func (b *Bulk) Close() {
	b.ticker.Stop()
	b.cancel()
	b.wg.Wait()

	b.mu.Lock()
	if len(b.buffer) > 0 {
		b.flushLocked()
	}
	b.mu.Unlock()

	if b.redisClient != nil {
		_ = b.redisClient.Close()
	}
}

func (b *Bulk) AddActions(ack func() error, eventTime time.Time, actions []redis.Model) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.buffer = append(b.buffer, batchEntry{ack: ack, actions: actions, eventTime: eventTime})
	if len(b.buffer) >= b.maxBatchSize {
		b.flushLocked()
	}
}

func (b *Bulk) flushLocked() {
	if len(b.buffer) == 0 {
		return
	}

	entries := b.buffer
	b.buffer = make([]batchEntry, 0, b.maxBatchSize)

	batchSize := len(entries)
	b.metric.ProcessLatencyMs = time.Since(entries[0].eventTime).Milliseconds()
	b.metric.CurrentBatchSize = int64(batchSize)

	started := time.Now()

	pipe := b.redisClient.Pipeline()
	for _, entry := range entries {
		for _, model := range entry.actions {
			b.addToPipe(pipe, model.Convert())
		}
	}

	if _, err := pipe.Exec(b.ctx); err != nil && err != goredis.Nil {
		b.metric.FlushErrorTotal.Add(1)
		b.responseHandler.OnError(&ResponseHandlerContext{Err: err, BatchSize: batchSize})
		return
	}

	b.metric.BulkRequestProcessLatencyMs = time.Since(started).Milliseconds()
	b.metric.FlushSuccessTotal.Add(1)
	b.metric.TotalProcessed.Add(int64(batchSize))

	b.responseHandler.OnSuccess(&ResponseHandlerContext{BatchSize: batchSize})

	lastAck := entries[len(entries)-1].ack
	if err := lastAck(); err != nil {
		slog.Error("cdc ack failed", "error", err)
	}
}

func (b *Bulk) addToPipe(pipe goredis.Pipeliner, cmd *redis.Command) {
	switch cmd.Operation {
	case "SET":
		pipe.Set(b.ctx, cmd.Key, cmd.Value, cmd.TTL)
	case "DEL":
		pipe.Del(b.ctx, cmd.Key)
	case "HSET":
		if len(cmd.Args) >= 2 {
			pipe.HSet(b.ctx, cmd.Key, cmd.Args[0], cmd.Args[1])
			if cmd.TTL > 0 {
				pipe.Expire(b.ctx, cmd.Key, cmd.TTL)
			}
		}
	case "HDEL":
		if len(cmd.Args) >= 1 {
			field, ok := cmd.Args[0].(string)
			if !ok {
				slog.Warn("hdel: field is not string, skipping", "key", cmd.Key)
				return
			}
			pipe.HDel(b.ctx, cmd.Key, field)
		}
	default:
		slog.Warn("unknown redis operation, skipping", "operation", cmd.Operation, "key", cmd.Key)
	}
}
