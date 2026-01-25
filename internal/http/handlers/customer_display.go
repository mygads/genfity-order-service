package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var allowedDisplayModes = map[string]struct{}{
	"CART":         {},
	"ORDER_REVIEW": {},
	"THANK_YOU":    {},
	"IDLE":         {},
}

const displaySessionLookback = 24 * time.Hour

type displayStatePayload struct {
	Mode     string          `json:"mode"`
	Payload  json.RawMessage `json:"payload"`
	IsLocked *bool           `json:"isLocked"`
	Source   string          `json:"source"`
}

func (h *Handler) MerchantCustomerDisplayStateGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	query := `select mode, is_locked, payload, updated_at from customer_display_state where merchant_id = $1`
	var (
		mode         pgtype.Text
		isLocked     bool
		payloadBytes []byte
		updatedAt    pgtype.Timestamptz
	)
	if err := h.DB.QueryRow(ctx, query, *authCtx.MerchantID).Scan(&mode, &isLocked, &payloadBytes, &updatedAt); err != nil {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"mode":      "IDLE",
				"isLocked":  false,
				"payload":   nil,
				"updatedAt": nil,
			},
			"message":    "Customer display state ready",
			"statusCode": 200,
		})
		return
	}

	var payloadValue any
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &payloadValue)
	}
	var updated any
	if updatedAt.Valid {
		updated = updatedAt.Time
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"mode":      mode.String,
			"isLocked":  isLocked,
			"payload":   payloadValue,
			"updatedAt": updated,
		},
		"message":    "Customer display state retrieved",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantCustomerDisplayStatePut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body displayStatePayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_MODE", "Invalid display mode")
		return
	}

	mode := strings.ToUpper(strings.TrimSpace(body.Mode))
	if _, ok := allowedDisplayModes[mode]; !ok {
		response.Error(w, http.StatusBadRequest, "INVALID_MODE", "Invalid display mode")
		return
	}

	var (
		existingPayload []byte
		existingLocked  bool
	)
	_ = h.DB.QueryRow(ctx, "select payload, is_locked from customer_display_state where merchant_id = $1", *authCtx.MerchantID).Scan(&existingPayload, &existingLocked)

	payloadMap := map[string]any{}
	if len(existingPayload) > 0 {
		_ = json.Unmarshal(existingPayload, &payloadMap)
	}

	sessions := pruneDisplaySessions(payloadMap["sessions"])

	if body.Source == "pos" {
		sessionKey := "unknown"
		if authCtx.SessionID != 0 {
			sessionKey = fmt.Sprint(authCtx.SessionID)
		}

		existingSession, _ := sessions[sessionKey].(map[string]any)
		sessionLocked := false
		if existingSession != nil {
			if v, ok := existingSession["isLocked"].(bool); ok {
				sessionLocked = v
			}
		}

		if sessionLocked && body.IsLocked == nil {
			response.JSON(w, http.StatusOK, map[string]any{
				"success": true,
				"data": map[string]any{
					"mode":      mode,
					"isLocked":  existingLocked,
					"payload":   payloadMap,
					"updatedAt": time.Now().UTC(),
				},
				"message":    "Customer display locked",
				"statusCode": 200,
			})
			return
		}

		staffName := fetchStaffName(ctx, h.DB, authCtx.UserID)
		payloadObj := parsePayloadObject(body.Payload)
		locked := false
		if body.IsLocked != nil {
			locked = *body.IsLocked
		} else if existingSession != nil {
			if v, ok := existingSession["isLocked"].(bool); ok {
				locked = v
			}
		}

		sessions[sessionKey] = map[string]any{
			"sessionId": sessionKey,
			"userId":    fmt.Sprint(authCtx.UserID),
			"staffName": staffName,
			"mode":      mode,
			"payload":   payloadObj,
			"isLocked":  locked,
			"updatedAt": time.Now().UTC().Format(time.RFC3339),
		}

		basePayload := map[string]any{}
		if payloadObj != nil {
			if m, ok := payloadObj.(map[string]any); ok {
				basePayload = m
			}
		}
		basePayload["sessions"] = sessions

		payloadBytes, _ := json.Marshal(basePayload)
		_, err := h.DB.Exec(ctx, `
			insert into customer_display_state (merchant_id, mode, is_locked, payload, created_at, updated_at)
			values ($1, $2::"CustomerDisplayMode", $3, $4, now(), now())
			on conflict (merchant_id)
			do update set mode = excluded.mode, payload = excluded.payload, updated_at = now()
		`, *authCtx.MerchantID, mode, existingLocked, payloadBytes)
		if err != nil {
			h.Logger.Error("customer display state update failed", zap.Error(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update customer display state")
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"mode":      mode,
				"isLocked":  existingLocked,
				"payload":   basePayload,
				"updatedAt": time.Now().UTC(),
			},
			"message":    "Customer display state updated",
			"statusCode": 200,
		})
		return
	}

	payloadObj := parsePayloadObject(body.Payload)
	var payloadValue any
	if body.Payload == nil {
		payloadValue = nil
	} else if payloadObj != nil {
		payloadValue = payloadObj
	}

	if body.Payload == nil && len(sessions) > 0 {
		payloadValue = map[string]any{"sessions": sessions}
	} else if payloadMap != nil && len(sessions) > 0 {
		if m, ok := payloadValue.(map[string]any); ok {
			m["sessions"] = sessions
			payloadValue = m
		}
	}

	payloadBytes, _ := json.Marshal(payloadValue)
	isLocked := existingLocked
	if body.IsLocked != nil {
		isLocked = *body.IsLocked
	}

	_, err := h.DB.Exec(ctx, `
		insert into customer_display_state (merchant_id, mode, is_locked, payload, created_at, updated_at)
		values ($1, $2::"CustomerDisplayMode", $3, $4, now(), now())
		on conflict (merchant_id)
		do update set mode = excluded.mode, payload = excluded.payload, is_locked = excluded.is_locked, updated_at = now()
	`, *authCtx.MerchantID, mode, isLocked, payloadBytes)
	if err != nil {
		h.Logger.Error("customer display state update failed", zap.Error(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update customer display state")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"mode":      mode,
			"isLocked":  isLocked,
			"payload":   payloadValue,
			"updatedAt": time.Now().UTC(),
		},
		"message":    "Customer display state updated",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantCustomerDisplaySessions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	cutoff := time.Now().Add(-displaySessionLookback)
	query := `
		select us.id, us.created_at, us.expires_at, us.device_info,
		       u.id, u.name, u.role
		from user_sessions us
		join users u on u.id = us.user_id
		join merchant_users mu on mu.user_id = u.id and mu.merchant_id = $1
		where us.status = 'ACTIVE'
		  and us.expires_at > now()
		  and us.created_at >= $2
		  and u.role in ('MERCHANT_OWNER', 'MERCHANT_STAFF')
		  and mu.is_active = true
		order by us.created_at desc
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID, cutoff)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve customer display sessions")
		return
	}
	defer rows.Close()

	sessions := make([]map[string]any, 0)
	for rows.Next() {
		var (
			sessionID  int64
			createdAt  time.Time
			expiresAt  time.Time
			deviceInfo pgtype.Text
			userID     int64
			userName   pgtype.Text
			role       pgtype.Text
		)
		if err := rows.Scan(&sessionID, &createdAt, &expiresAt, &deviceInfo, &userID, &userName, &role); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve customer display sessions")
			return
		}

		sessions = append(sessions, map[string]any{
			"sessionId": sessionID,
			"userId":    userID,
			"staffName": userName.String,
			"role":      role.String,
			"deviceInfo": func() any {
				if deviceInfo.Valid {
					return deviceInfo.String
				}
				return nil
			}(),
			"createdAt": createdAt,
			"expiresAt": expiresAt,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"sessions": sessions,
		},
		"message":    "Customer display sessions retrieved",
		"statusCode": 200,
	})
}

func fetchStaffName(ctx context.Context, db *pgxpool.Pool, userID int64) *string {
	var name pgtype.Text
	if err := db.QueryRow(ctx, "select name from users where id = $1", userID).Scan(&name); err != nil {
		return nil
	}
	if name.Valid {
		return &name.String
	}
	return nil
}

func parsePayloadObject(payload json.RawMessage) any {
	if len(payload) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil
	}
	return value
}

func pruneDisplaySessions(value any) map[string]any {
	out := map[string]any{}
	m, ok := value.(map[string]any)
	if !ok {
		return out
	}
	cutoff := time.Now().Add(-displaySessionLookback)
	for key, raw := range m {
		session, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		updatedRaw, ok := session["updatedAt"].(string)
		if !ok || updatedRaw == "" {
			continue
		}
		updatedAt, err := time.Parse(time.RFC3339, updatedRaw)
		if err != nil {
			continue
		}
		if updatedAt.Before(cutoff) {
			continue
		}
		out[key] = session
	}
	return out
}
