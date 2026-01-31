package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

func RequestID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := readRequestIDHeader(r)
			if requestID == "" {
				requestID = generateRequestID()
			}
			r.Header.Set("X-Request-Id", requestID)
			w.Header().Set("X-Request-Id", requestID)
			next.ServeHTTP(w, r)
		})
	}
}

func readRequestIDHeader(r *http.Request) string {
	for _, key := range []string{"X-Request-Id", "X-Correlation-Id"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			return value
		}
	}
	return ""
}

func generateRequestID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}
