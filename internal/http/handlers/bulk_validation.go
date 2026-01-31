package handlers

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type bulkLegacyError struct {
	code    string
	message string
}

func (e bulkLegacyError) Error() string {
	return e.message
}

func errInvalidValue(message string) error {
	return bulkLegacyError{code: "VALIDATION_ERROR", message: message}
}

func errOperation(message string) error {
	return bulkLegacyError{code: "OPERATION_ERROR", message: message}
}

func parseFloatValue(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		parsed, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func parseBoolValue(value any) (bool, bool) {
	if value == nil {
		return false, false
	}
	if v, ok := value.(bool); ok {
		return v, true
	}
	return false, false
}

func parseIDArrayValue(value any) ([]int64, bool) {
	if value == nil {
		return nil, false
	}
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	parsed, err := parseAnyIDList(items)
	if err != nil {
		return nil, false
	}
	return parsed, true
}

func applyPriceOptions(price float64, options *bulkLegacyOptions) float64 {
	if options == nil {
		return price
	}
	if options.RoundTo != nil && *options.RoundTo > 0 {
		roundTo := *options.RoundTo
		price = math.Round(price/roundTo) * roundTo
	}
	if options.MinPrice != nil {
		price = math.Max(price, *options.MinPrice)
	}
	if options.MaxPrice != nil {
		price = math.Min(price, *options.MaxPrice)
	}
	return price
}

func parseAnyIDList(ids []any) ([]int64, error) {
	parsed := make([]int64, 0, len(ids))
	for _, raw := range ids {
		switch value := raw.(type) {
		case string:
			id, err := parseStringToInt64(strings.TrimSpace(value))
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, id)
		case float64:
			parsed = append(parsed, int64(value))
		case json.Number:
			id, err := value.Int64()
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, id)
		default:
			return nil, fmt.Errorf("invalid id type")
		}
	}
	return parsed, nil
}
