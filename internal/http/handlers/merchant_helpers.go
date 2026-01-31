package handlers

import (
	"fmt"
	"strconv"
)

func intToString(value int) string {
	return strconv.Itoa(value)
}

func parseStringToInt64(value string) (int64, error) {
	var out int64
	_, err := fmt.Sscan(value, &out)
	return out, err
}

func parseStringToInt(value string) (int, error) {
	var out int
	_, err := fmt.Sscan(value, &out)
	return out, err
}
