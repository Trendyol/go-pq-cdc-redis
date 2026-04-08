package postgres

import (
	"fmt"
)

// CellString satırdan tek bir sütun değerini Redis anahtar bileşeni olarak döndürür.
func CellString(row map[string]any, col string) (string, error) {
	if row == nil {
		return "", fmt.Errorf("satır nil")
	}
	v, ok := row[col]
	if !ok {
		return "", fmt.Errorf("sütun yok: %s", col)
	}
	if v == nil {
		return "", fmt.Errorf("anahtar sütunu nil: %s", col)
	}
	return fmt.Sprintf("%v", v), nil
}
