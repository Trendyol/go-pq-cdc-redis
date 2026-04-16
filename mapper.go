package pgredis

import (
	"encoding/json"
	"fmt"
	"go-dcp-pg-redis/config"
	"go-dcp-pg-redis/postgres"
	"go-dcp-pg-redis/redis"
	"log/slog"
	"sync"
)

type Mapper func(ctx postgres.Context) []redis.Model

var (
	tableMappingMu sync.RWMutex
	tableMapping   map[string]config.TableKeyMapping
)

func SetTableKeyMappings(mappings []config.TableKeyMapping) {
	cache := make(map[string]config.TableKeyMapping, len(mappings))
	for _, m := range mappings {
		schema := m.Schema
		if schema == "" {
			schema = "public"
		}
		cache[schema+"."+m.Table] = m
	}
	tableMappingMu.Lock()
	tableMapping = cache
	tableMappingMu.Unlock()
}

func findTableMapping(qualifiedTable string) (config.TableKeyMapping, bool) {
	tableMappingMu.RLock()
	defer tableMappingMu.RUnlock()
	m, ok := tableMapping[qualifiedTable]
	return m, ok
}

func DefaultMapper(ctx postgres.Context) []redis.Model {
	e := ctx.Event
	mapping, ok := findTableMapping(e.QualifiedTable)
	if !ok {
		slog.Warn("no table mapping found, skipping event", "table", e.QualifiedTable)
		return nil
	}

	switch {
	case e.IsInsert || e.IsUpdate:
		row := e.NewRow
		if row == nil {
			return nil
		}
		keyVal, err := e.KeyFromRow(mapping.KeyColumn, row)
		if err != nil {
			slog.Error("key extraction failed for insert/update", "table", e.QualifiedTable, "column", mapping.KeyColumn, "error", err)
			return nil
		}
		key := buildRedisKey(mapping, keyVal)
		return []redis.Model{buildSetCommand(mapping, key, row)}

	case e.IsDelete:
		row := e.OldRow
		if row == nil {
			return nil
		}
		keyVal, err := e.KeyFromRow(mapping.KeyColumn, row)
		if err != nil {
			slog.Error("key extraction failed for delete", "table", e.QualifiedTable, "column", mapping.KeyColumn, "error", err)
			return nil
		}
		key := buildRedisKey(mapping, keyVal)
		return []redis.Model{buildDeleteCommand(mapping, key)}

	default:
		return nil
	}
}

func buildRedisKey(m config.TableKeyMapping, id string) string {
	return m.KeyPrefix + id + m.KeySuffix
}

func buildSetCommand(m config.TableKeyMapping, key string, row map[string]any) redis.Model {
	switch m.StorageType {
	case "hash":
		field := m.HashField
		if field == "" {
			field = "value"
		}
		b, err := json.Marshal(row)
		if err != nil {
			slog.Error("json marshal failed for hash", "key", key, "error", err)
			return nil
		}
		return &redis.HSet{
			Key:   key,
			Field: field,
			Value: string(b),
			TTL:   m.TTL,
		}
	case "json":
		b, err := json.Marshal(row)
		if err != nil {
			slog.Error("json marshal failed", "key", key, "error", err)
			return nil
		}
		return &redis.Set{
			Key:   key,
			Value: string(b),
			TTL:   m.TTL,
		}
	default:
		return &redis.Set{
			Key:   key,
			Value: scalarForString(row, m),
			TTL:   m.TTL,
		}
	}
}

func scalarForString(row map[string]any, m config.TableKeyMapping) string {
	v, ok := row[m.KeyColumn]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func buildDeleteCommand(m config.TableKeyMapping, key string) redis.Model {
	switch m.StorageType {
	case "hash":
		field := m.HashField
		if field == "" {
			field = "value"
		}
		return &redis.HDel{
			Key:   key,
			Field: field,
		}
	default:
		return &redis.Del{
			Key: key,
		}
	}
}
