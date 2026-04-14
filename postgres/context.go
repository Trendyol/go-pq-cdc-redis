package postgres

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

type Context struct {
	Event Event
}

type Event struct {
	QualifiedTable string
	TableNamespace string
	TableName      string
	MessageTime    time.Time
	IsInsert       bool
	IsUpdate       bool
	IsDelete       bool
	IsSnapshot     bool
	NewRow         map[string]any
	OldRow         map[string]any
}

func ContextFromMessage(msg any) (Context, bool) {
	switch m := msg.(type) {
	case *format.Insert:
		return Context{
			Event: Event{
				QualifiedTable: m.TableNamespace + "." + m.TableName,
				TableNamespace: m.TableNamespace,
				TableName:      m.TableName,
				MessageTime:    m.MessageTime,
				IsInsert:       true,
				NewRow:         m.Decoded,
			},
		}, true
	case *format.Update:
		return Context{
			Event: Event{
				QualifiedTable: m.TableNamespace + "." + m.TableName,
				TableNamespace: m.TableNamespace,
				TableName:      m.TableName,
				MessageTime:    m.MessageTime,
				IsUpdate:       true,
				NewRow:         m.NewDecoded,
				OldRow:         m.OldDecoded,
			},
		}, true
	case *format.Delete:
		return Context{
			Event: Event{
				QualifiedTable: m.TableNamespace + "." + m.TableName,
				TableNamespace: m.TableNamespace,
				TableName:      m.TableName,
				MessageTime:    m.MessageTime,
				IsDelete:       true,
				OldRow:         m.OldDecoded,
			},
		}, true
	case *format.Snapshot:
		if m.EventType != format.SnapshotEventTypeData {
			return Context{}, false
		}
		return Context{
			Event: Event{
				QualifiedTable: m.Schema + "." + m.Table,
				TableNamespace: m.Schema,
				TableName:      m.Table,
				MessageTime:    m.ServerTime,
				IsSnapshot:     true,
				IsInsert:       true,
				NewRow:         m.Data,
			},
		}, true
	default:
		return Context{}, false
	}
}

func (e Event) KeyFromRow(column string, row map[string]any) (string, error) {
	return CellString(row, column)
}
