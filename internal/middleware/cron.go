//go:build cron

package middleware

import (
	"net/http"
	"strings"
)

func CronAuth(cronSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			secret := strings.TrimSpace(cronSecret)
			if secret == "" {
				writeAuthError(w, http.StatusForbidden, "Cron access is disabled")
				return
			}

			token := strings.TrimSpace(ParseBearerToken(r.Header.Get("Authorization")))
			if token == "" || token != secret {
				writeAuthError(w, http.StatusUnauthorized, "Invalid cron token")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func ParseBearerToken(header string) string {
	value := strings.TrimSpace(header)
	if value == "" {
		return ""
	}
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 {
		return ""
	}
	if !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}
