package utils

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

func NumericToFloat64(value pgtype.Numeric) float64 {
	if !value.Valid {
		return 0
	}
	f, err := value.Float64Value()
	if err == nil {
		return f.Float64
	}
	// fallback to string parse
	text, err := value.MarshalJSON()
	if err != nil {
		return 0
	}
	var out float64
	if _, err := fmt.Sscan(string(text), &out); err != nil {
		return 0
	}
	return out
}
