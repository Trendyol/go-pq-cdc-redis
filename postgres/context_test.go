package postgres

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

func TestContextFromMessage_Insert(t *testing.T) {
	msg := &format.Insert{
		MessageTime:    time.Now(),
		TableNamespace: "public",
		TableName:      "users",
		Decoded:        map[string]any{"id": 1, "name": "alice"},
	}

	ctx, ok := ContextFromMessage(msg)
	if !ok {
		t.Fatal("expected ok=true for Insert")
	}
	if !ctx.Event.IsInsert {
		t.Fatal("expected IsInsert=true")
	}
	if ctx.Event.QualifiedTable != "public.users" {
		t.Fatalf("expected 'public.users', got %q", ctx.Event.QualifiedTable)
	}
	if ctx.Event.NewRow["name"] != "alice" {
		t.Fatalf("expected name 'alice', got %v", ctx.Event.NewRow["name"])
	}
}

func TestContextFromMessage_Update(t *testing.T) {
	msg := &format.Update{
		MessageTime:    time.Now(),
		TableNamespace: "public",
		TableName:      "users",
		NewDecoded:     map[string]any{"id": 1, "name": "bob"},
		OldDecoded:     map[string]any{"id": 1, "name": "alice"},
	}

	ctx, ok := ContextFromMessage(msg)
	if !ok {
		t.Fatal("expected ok=true for Update")
	}
	if !ctx.Event.IsUpdate {
		t.Fatal("expected IsUpdate=true")
	}
	if ctx.Event.NewRow["name"] != "bob" {
		t.Fatalf("expected new name 'bob', got %v", ctx.Event.NewRow["name"])
	}
	if ctx.Event.OldRow["name"] != "alice" {
		t.Fatalf("expected old name 'alice', got %v", ctx.Event.OldRow["name"])
	}
}

func TestContextFromMessage_Delete(t *testing.T) {
	msg := &format.Delete{
		MessageTime:    time.Now(),
		TableNamespace: "public",
		TableName:      "users",
		OldDecoded:     map[string]any{"id": 1},
	}

	ctx, ok := ContextFromMessage(msg)
	if !ok {
		t.Fatal("expected ok=true for Delete")
	}
	if !ctx.Event.IsDelete {
		t.Fatal("expected IsDelete=true")
	}
	if ctx.Event.OldRow["id"] != 1 {
		t.Fatalf("expected id 1, got %v", ctx.Event.OldRow["id"])
	}
}

func TestContextFromMessage_Snapshot_Data(t *testing.T) {
	msg := &format.Snapshot{
		ServerTime: time.Now(),
		Schema:     "public",
		Table:      "users",
		EventType:  format.SnapshotEventTypeData,
		Data:       map[string]any{"id": 1, "name": "charlie"},
	}

	ctx, ok := ContextFromMessage(msg)
	if !ok {
		t.Fatal("expected ok=true for Snapshot DATA")
	}
	if !ctx.Event.IsSnapshot || !ctx.Event.IsInsert {
		t.Fatal("expected IsSnapshot=true and IsInsert=true")
	}
	if ctx.Event.QualifiedTable != "public.users" {
		t.Fatalf("expected 'public.users', got %q", ctx.Event.QualifiedTable)
	}
}

func TestContextFromMessage_Snapshot_Begin(t *testing.T) {
	msg := &format.Snapshot{
		EventType: format.SnapshotEventTypeBegin,
	}
	_, ok := ContextFromMessage(msg)
	if ok {
		t.Fatal("expected ok=false for Snapshot BEGIN")
	}
}

func TestContextFromMessage_Snapshot_End(t *testing.T) {
	msg := &format.Snapshot{
		EventType: format.SnapshotEventTypeEnd,
	}
	_, ok := ContextFromMessage(msg)
	if ok {
		t.Fatal("expected ok=false for Snapshot END")
	}
}

func TestContextFromMessage_Unknown(t *testing.T) {
	_, ok := ContextFromMessage("unknown")
	if ok {
		t.Fatal("expected ok=false for unknown message type")
	}
}

func TestCellString(t *testing.T) {
	row := map[string]any{"id": 42, "name": "alice"}

	val, err := CellString(row, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "42" {
		t.Fatalf("expected '42', got %q", val)
	}

	_, err = CellString(row, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}

	_, err = CellString(nil, "id")
	if err == nil {
		t.Fatal("expected error for nil row")
	}
}

func TestCellString_NilValue(t *testing.T) {
	row := map[string]any{"id": nil}
	_, err := CellString(row, "id")
	if err == nil {
		t.Fatal("expected error for nil column value")
	}
}

func TestKeyFromRow(t *testing.T) {
	e := Event{NewRow: map[string]any{"user_id": "abc-123"}}
	val, err := e.KeyFromRow("user_id", e.NewRow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "abc-123" {
		t.Fatalf("expected 'abc-123', got %q", val)
	}
}
