package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func zapError(err error) zap.Field {
	return zap.Error(err)
}

func readPathString(r *http.Request, key string) string {
	return chi.URLParam(r, key)
}

func readPathInt64(r *http.Request, key string) (int64, error) {
	value := readPathString(r, key)
	if value == "" {
		return 0, errMissingParam
	}
	var out int64
	_, err := fmt.Sscan(value, &out)
	return out, err
}

var errMissingParam = errors.New("missing param")
