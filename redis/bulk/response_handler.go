package bulk

import (
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

type ResponseHandlerContext struct {
	Err       error
	BatchSize int
}

type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
}

type DefaultResponseHandler struct{}

func (d *DefaultResponseHandler) OnSuccess(_ *ResponseHandlerContext) {}

func (d *DefaultResponseHandler) OnError(ctx *ResponseHandlerContext) {
	if isFatalRedisError(ctx.Err) {
		slog.Error("permanent redis error during pipeline flush", "error", ctx.Err, "batchSize", ctx.BatchSize)
		panic(fmt.Errorf("permanent redis error: %w", ctx.Err))
	}
	slog.Error("transient redis error during pipeline flush", "error", ctx.Err, "batchSize", ctx.BatchSize)
}

func isFatalRedisError(err error) bool {
	if err == nil || err == goredis.Nil {
		return false
	}
	return true
}
