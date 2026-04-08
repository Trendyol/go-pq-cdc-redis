package pgredis

import (
	"testing"
	"time"

	"go-dcp-pg-redis/config"
	"go-dcp-pg-redis/postgres"
	"go-dcp-pg-redis/redis"
)

func TestSetTableKeyMappings(t *testing.T) {
	mappings := []config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "user:", KeyColumn: "id", StorageType: "json"},
		{Schema: "", Table: "orders", KeyPrefix: "order:", KeyColumn: "order_id", StorageType: "string"},
	}
	SetTableKeyMappings(mappings)

	m, ok := findTableMapping("public.users")
	if !ok {
		t.Fatal("expected to find mapping for public.users")
	}
	if m.KeyPrefix != "user:" {
		t.Fatalf("expected keyPrefix 'user:', got %q", m.KeyPrefix)
	}

	m, ok = findTableMapping("public.orders")
	if !ok {
		t.Fatal("expected to find mapping for public.orders (default schema)")
	}
	if m.KeyPrefix != "order:" {
		t.Fatalf("expected keyPrefix 'order:', got %q", m.KeyPrefix)
	}

	_, ok = findTableMapping("public.nonexistent")
	if ok {
		t.Fatal("expected no mapping for nonexistent table")
	}
}

func TestDefaultMapper_Insert_JSON(t *testing.T) {
	SetTableKeyMappings([]config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "user:", KeyColumn: "id", StorageType: "json"},
	})

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.users",
			IsInsert:       true,
			NewRow:         map[string]any{"id": 42, "name": "alice"},
		},
	}
	models := DefaultMapper(ctx)
	if len(models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(models))
	}

	cmd := models[0].Convert()
	if cmd.Operation != "SET" {
		t.Fatalf("expected SET, got %s", cmd.Operation)
	}
	if cmd.Key != "user:42" {
		t.Fatalf("expected key 'user:42', got %q", cmd.Key)
	}
}

func TestDefaultMapper_Insert_Hash(t *testing.T) {
	SetTableKeyMappings([]config.TableKeyMapping{
		{Schema: "public", Table: "items", KeyPrefix: "item:", KeyColumn: "id", StorageType: "hash", HashField: "data", TTL: 5 * time.Minute},
	})

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.items",
			IsInsert:       true,
			NewRow:         map[string]any{"id": 7, "price": 9.99},
		},
	}
	models := DefaultMapper(ctx)
	if len(models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(models))
	}
	cmd := models[0].Convert()
	if cmd.Operation != "HSET" {
		t.Fatalf("expected HSET, got %s", cmd.Operation)
	}
	if cmd.Key != "item:7" {
		t.Fatalf("expected key 'item:7', got %q", cmd.Key)
	}
	if cmd.TTL != 5*time.Minute {
		t.Fatalf("expected TTL 5m, got %v", cmd.TTL)
	}
	if len(cmd.Args) < 2 {
		t.Fatal("expected at least 2 args for HSET")
	}
	if cmd.Args[0] != "data" {
		t.Fatalf("expected field 'data', got %v", cmd.Args[0])
	}
}

func TestDefaultMapper_Delete(t *testing.T) {
	SetTableKeyMappings([]config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "user:", KeyColumn: "id", StorageType: "json"},
	})

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.users",
			IsDelete:       true,
			OldRow:         map[string]any{"id": 42, "name": "alice"},
		},
	}
	models := DefaultMapper(ctx)
	if len(models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(models))
	}
	cmd := models[0].Convert()
	if cmd.Operation != "DEL" {
		t.Fatalf("expected DEL, got %s", cmd.Operation)
	}
	if cmd.Key != "user:42" {
		t.Fatalf("expected key 'user:42', got %q", cmd.Key)
	}
}

func TestDefaultMapper_Delete_Hash(t *testing.T) {
	SetTableKeyMappings([]config.TableKeyMapping{
		{Schema: "public", Table: "items", KeyPrefix: "item:", KeyColumn: "id", StorageType: "hash", HashField: "data"},
	})

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.items",
			IsDelete:       true,
			OldRow:         map[string]any{"id": 7},
		},
	}
	models := DefaultMapper(ctx)
	if len(models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(models))
	}
	cmd := models[0].Convert()
	if cmd.Operation != "HDEL" {
		t.Fatalf("expected HDEL, got %s", cmd.Operation)
	}
	if cmd.Args[0] != "data" {
		t.Fatalf("expected field 'data', got %v", cmd.Args[0])
	}
}

func TestDefaultMapper_NoMapping(t *testing.T) {
	SetTableKeyMappings(nil)

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.unknown",
			IsInsert:       true,
			NewRow:         map[string]any{"id": 1},
		},
	}
	models := DefaultMapper(ctx)
	if models != nil {
		t.Fatalf("expected nil for unmapped table, got %v", models)
	}
}

func TestDefaultMapper_NilNewRow(t *testing.T) {
	SetTableKeyMappings([]config.TableKeyMapping{
		{Schema: "public", Table: "users", KeyPrefix: "user:", KeyColumn: "id", StorageType: "json"},
	})

	ctx := postgres.Context{
		Event: postgres.Event{
			QualifiedTable: "public.users",
			IsInsert:       true,
			NewRow:         nil,
		},
	}
	models := DefaultMapper(ctx)
	if models != nil {
		t.Fatalf("expected nil for nil new row, got %v", models)
	}
}

func TestBuildRedisKey(t *testing.T) {
	m := config.TableKeyMapping{KeyPrefix: "pre:", KeySuffix: ":suf"}
	key := buildRedisKey(m, "123")
	if key != "pre:123:suf" {
		t.Fatalf("expected 'pre:123:suf', got %q", key)
	}
}

func TestRedisModelTypes(t *testing.T) {
	set := &redis.Set{Key: "k", Value: "v", TTL: time.Second}
	cmd := set.Convert()
	if cmd.Operation != "SET" || cmd.Key != "k" || cmd.Value != "v" || cmd.TTL != time.Second {
		t.Fatal("Set.Convert mismatch")
	}

	del := &redis.Del{Key: "k"}
	cmd = del.Convert()
	if cmd.Operation != "DEL" || cmd.Key != "k" {
		t.Fatal("Del.Convert mismatch")
	}

	hset := &redis.HSet{Key: "h", Field: "f", Value: "v", TTL: time.Minute}
	cmd = hset.Convert()
	if cmd.Operation != "HSET" || cmd.Key != "h" || cmd.TTL != time.Minute || len(cmd.Args) != 2 {
		t.Fatal("HSet.Convert mismatch")
	}

	hdel := &redis.HDel{Key: "h", Field: "f"}
	cmd = hdel.Convert()
	if cmd.Operation != "HDEL" || cmd.Key != "h" || len(cmd.Args) != 1 {
		t.Fatal("HDel.Convert mismatch")
	}

	raw := &redis.Raw{Operation: "CUSTOM", Key: "r"}
	cmd = raw.Convert()
	if cmd.Operation != "CUSTOM" || cmd.Key != "r" {
		t.Fatal("Raw.Convert mismatch")
	}
}
