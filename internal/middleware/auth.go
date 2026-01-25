package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"genfity-order-services/internal/auth"

	"github.com/jackc/pgx/v5/pgxpool"
)

type contextKey string

const authContextKey contextKey = "authContext"

type AuthContext struct {
	UserID      int64
	SessionID   int64
	Role        auth.UserRole
	Email       string
	MerchantID  *int64
	IsOwner     bool
	Permissions []string
}

func WithAuthContext(ctx context.Context, authCtx *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, authCtx)
}

func GetAuthContext(ctx context.Context) (*AuthContext, bool) {
	value := ctx.Value(authContextKey)
	if value == nil {
		return nil, false
	}
	ac, ok := value.(*AuthContext)
	return ac, ok
}

func writeAuthError(w http.ResponseWriter, status int, message string) {
	writeAuthErrorDebug(w, status, message, "")
}

func writeAuthErrorDebug(w http.ResponseWriter, status int, message string, debug string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	payload := map[string]any{
		"success": false,
		"error":   "UNAUTHORIZED",
		"message": message,
	}

	if os.Getenv("APP_ENV") == "development" && strings.TrimSpace(debug) != "" {
		payload["debug"] = debug
	}

	_ = json.NewEncoder(w).Encode(payload)
}

func isMerchantLockExempt(path string, method string) bool {
	if strings.HasPrefix(path, "/api/merchant/subscription") {
		return true
	}
	if strings.HasPrefix(path, "/api/merchant/balance") {
		return true
	}
	if strings.HasPrefix(path, "/api/merchant/payment-request") {
		return true
	}
	if path == "/api/merchant/lock-status" && method == http.MethodGet {
		return true
	}
	if path == "/api/merchant/profile" && method == http.MethodGet {
		return true
	}
	return false
}

func MerchantAuth(db *pgxpool.Pool, jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := auth.ParseBearerToken(r.Header.Get("Authorization"))
			claims, err := auth.VerifyAccessToken(token, jwtSecret)
			if err != nil {
				writeAuthErrorDebug(w, http.StatusUnauthorized, "Authorization token required", err.Error())
				return
			}

			if claims.Role != auth.RoleMerchantOwner && claims.Role != auth.RoleMerchantStaff {
				writeAuthError(w, http.StatusForbidden, "Merchant access required")
				return
			}

			merchantID := int64(0)
			if claims.MerchantID == nil {
				writeAuthError(w, http.StatusUnauthorized, "Merchant not found")
				return
			}

			parsedMerchant, err := parseInt64(*claims.MerchantID)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "Merchant not found")
				return
			}
			merchantID = parsedMerchant

			userID, err := parseInt64(claims.UserID)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "Invalid token")
				return
			}
			sessionID, err := parseInt64(claims.SessionID)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "Invalid token")
				return
			}

			// Validate session + merchant link + merchant status
			var (
				role               string
				permissions        []string
				merchantActive     bool
				subscriptionStatus string
				linkActive         bool
			)

			query := `
				select u.role, mu.permissions, mu.is_active, m.is_active, coalesce(ms.status::text, '')
				from users u
				join merchant_users mu on mu.user_id = u.id and mu.merchant_id = $2
				join merchants m on m.id = mu.merchant_id
				left join merchant_subscriptions ms on ms.merchant_id = m.id
				join user_sessions us on us.id = $3 and us.user_id = u.id and us.status = 'ACTIVE' and us.expires_at > now()
				where u.id = $1
			`
			err = db.QueryRow(r.Context(), query, userID, merchantID, sessionID).Scan(&role, &permissions, &linkActive, &merchantActive, &subscriptionStatus)
			if err != nil {
				writeAuthErrorDebug(w, http.StatusUnauthorized, "Merchant access required", err.Error())
				return
			}

			if !linkActive {
				writeAuthError(w, http.StatusForbidden, "Merchant access is disabled")
				return
			}

			if !merchantActive {
				writeAuthError(w, http.StatusForbidden, "Merchant is currently disabled")
				return
			}

			if !isMerchantLockExempt(r.URL.Path, r.Method) {
				if strings.EqualFold(subscriptionStatus, "SUSPENDED") {
					writeAuthError(w, http.StatusForbidden, "Subscription is suspended. Please renew to continue.")
					return
				}
			}

			isOwner := claims.Role == auth.RoleMerchantOwner

			// Permissions for staff
			if claims.Role == auth.RoleMerchantStaff {
				perm := auth.GetPermissionForAPI(r.URL.Path, r.Method)
				if perm != nil {
					has := false
					for _, p := range permissions {
						if p == string(*perm) {
							has = true
							break
						}
					}
					if !has {
						writeAuthError(w, http.StatusForbidden, "You do not have permission to access this resource")
						return
					}
				}
			}

			authCtx := &AuthContext{
				UserID:      userID,
				SessionID:   sessionID,
				Role:        claims.Role,
				Email:       claims.Email,
				MerchantID:  &merchantID,
				IsOwner:     isOwner,
				Permissions: permissions,
			}

			ctx := WithAuthContext(r.Context(), authCtx)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func parseInt64(value string) (int64, error) {
	var out int64
	_, err := fmt.Sscan(value, &out)
	return out, err
}

// NOTE: use fmt.Sscan to avoid strconv overflow for bigints stored as string
// We keep bigints within int64 range in this service.
