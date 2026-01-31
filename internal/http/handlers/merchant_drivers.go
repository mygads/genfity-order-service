package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/auth"
	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type driverStatusRequest struct {
	IsActive *bool `json:"isActive"`
}

func (h *Handler) MerchantDriversList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	query := r.URL.Query()
	includeInactive := strings.EqualFold(query.Get("includeInactive"), "1") || strings.EqualFold(query.Get("includeInactive"), "true")
	search := strings.TrimSpace(query.Get("search"))

	driverPerm := string(auth.PermDriverDashboard)
	rows, err := h.DB.Query(ctx, `
        select mu.role, mu.is_active, mu.created_at, u.id, u.name, u.phone, u.email
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
          and u.is_active = true
          and ($2 = '' or u.name ilike '%' || $2 || '%' or u.email ilike '%' || $2 || '%')
          and (
                mu.role = 'OWNER'
             or mu.role = 'DRIVER'
             or (mu.role = 'STAFF' and mu.invitation_status = 'ACCEPTED' and mu.permissions @> ARRAY[$3]::text[])
          )
          and ($4 = true or mu.is_active = true)
        order by mu.created_at asc
    `, *authCtx.MerchantID, search, driverPerm, includeInactive)
	if err != nil {
		h.Logger.Error("merchant drivers list query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve drivers")
		return
	}
	defer rows.Close()

	drivers := make([]map[string]any, 0)
	for rows.Next() {
		var (
			role      string
			isActive  bool
			createdAt time.Time
			userID    int64
			name      string
			phone     pgtype.Text
			email     string
		)
		if err := rows.Scan(&role, &isActive, &createdAt, &userID, &name, &phone, &email); err != nil {
			h.Logger.Error("merchant drivers scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve drivers")
			return
		}

		source := "staff"
		if role == "OWNER" {
			source = "owner"
		} else if role == "DRIVER" {
			source = "driver"
		}

		drivers = append(drivers, map[string]any{
			"id":       userID,
			"name":     name,
			"phone":    nullableText(phone),
			"email":    email,
			"isActive": isActive,
			"joinedAt": createdAt,
			"source":   source,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       drivers,
		"message":    "Drivers retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDriversCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	var body struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	userID, err := parseInt64Value(strings.TrimSpace(body.UserID))
	if err != nil || userID == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Valid userId is required")
		return
	}

	driverPerm := string(auth.PermDriverDashboard)

	var (
		merchantUserID int64
		role           string
		isActive       bool
		createdAt      time.Time
		permissions    []string
		name           string
		phone          pgtype.Text
		email          string
	)
	if err := h.DB.QueryRow(ctx, `
        select mu.id, mu.role, mu.is_active, mu.created_at, mu.permissions, u.name, u.phone, u.email
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
          and mu.user_id = $2
          and mu.is_active = true
          and (
                mu.role = 'OWNER'
             or (mu.role = 'STAFF' and mu.invitation_status = 'ACCEPTED')
          )
    `, *authCtx.MerchantID, userID).Scan(&merchantUserID, &role, &isActive, &createdAt, &permissions, &name, &phone, &email); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Eligible staff member not found (must be accepted and active)")
			return
		}
		h.Logger.Error("merchant driver create lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to grant driver access")
		return
	}

	if role == "OWNER" {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"id":       userID,
				"name":     name,
				"phone":    nullableText(phone),
				"email":    email,
				"isActive": isActive,
				"joinedAt": createdAt,
				"source":   "owner",
			},
			"message":    "Owner is eligible as a driver",
			"statusCode": 200,
		})
		return
	}

	permissions = appendUniquePermission(permissions, driverPerm)
	if _, err := h.DB.Exec(ctx, `
        update merchant_users
        set permissions = $1, updated_at = now()
        where id = $2
    `, permissions, merchantUserID); err != nil {
		h.Logger.Error("merchant driver create update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to grant driver access")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":       userID,
			"name":     name,
			"phone":    nullableText(phone),
			"email":    email,
			"isActive": isActive && containsPermission(permissions, driverPerm),
			"joinedAt": createdAt,
			"source":   "staff",
		},
		"message":    "Driver access granted successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDriversUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "userId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "User ID is required")
		return
	}

	var body driverStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if body.IsActive == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isActive must be a boolean")
		return
	}

	driverPerm := string(auth.PermDriverDashboard)

	var (
		merchantUserID int64
		role           string
		isActive       bool
		createdAt      time.Time
		permissions    []string
		name           string
		phone          pgtype.Text
		email          string
	)
	if err := h.DB.QueryRow(ctx, `
        select mu.id, mu.role, mu.is_active, mu.created_at, mu.permissions, u.name, u.phone, u.email
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
          and mu.user_id = $2
          and (
                mu.role = 'OWNER'
             or mu.role = 'DRIVER'
             or (mu.role = 'STAFF' and mu.permissions @> ARRAY[$3]::text[])
          )
    `, *authCtx.MerchantID, userID, driverPerm).Scan(&merchantUserID, &role, &isActive, &createdAt, &permissions, &name, &phone, &email); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Driver not found")
			return
		}
		h.Logger.Error("merchant driver update lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update driver status")
		return
	}

	if role == "OWNER" {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner driver status cannot be changed")
		return
	}

	if role == "DRIVER" {
		if _, err := h.DB.Exec(ctx, `
            update merchant_users
            set is_active = $1, updated_at = now()
            where id = $2
        `, *body.IsActive, merchantUserID); err != nil {
			h.Logger.Error("merchant driver update failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update driver status")
			return
		}
		isActive = *body.IsActive
	} else {
		if *body.IsActive {
			permissions = appendUniquePermission(permissions, driverPerm)
		} else {
			permissions = removePermission(permissions, driverPerm)
		}
		if _, err := h.DB.Exec(ctx, `
            update merchant_users
            set permissions = $1, updated_at = now()
            where id = $2
        `, permissions, merchantUserID); err != nil {
			h.Logger.Error("merchant driver update failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update driver status")
			return
		}
		isActive = isActive && containsPermission(permissions, driverPerm)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":       userID,
			"name":     name,
			"phone":    nullableText(phone),
			"email":    email,
			"isActive": isActive,
			"joinedAt": createdAt,
		},
		"message":    "Driver status updated successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDriversDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	userID, err := readPathInt64(r, "userId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "User ID is required")
		return
	}

	driverPerm := string(auth.PermDriverDashboard)

	var (
		merchantUserID int64
		role           string
		isActive       bool
		createdAt      time.Time
		permissions    []string
		name           string
		phone          pgtype.Text
		email          string
	)
	if err := h.DB.QueryRow(ctx, `
        select mu.id, mu.role, mu.is_active, mu.created_at, mu.permissions, u.name, u.phone, u.email
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
          and mu.user_id = $2
          and (
                mu.role = 'OWNER'
             or mu.role = 'DRIVER'
             or (mu.role = 'STAFF' and mu.permissions @> ARRAY[$3]::text[])
          )
    `, *authCtx.MerchantID, userID, driverPerm).Scan(&merchantUserID, &role, &isActive, &createdAt, &permissions, &name, &phone, &email); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Driver not found")
			return
		}
		h.Logger.Error("merchant driver delete lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove driver access")
		return
	}

	if role == "OWNER" {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner cannot be removed from drivers")
		return
	}

	if role == "DRIVER" {
		if _, err := h.DB.Exec(ctx, `
            update merchant_users
            set is_active = false, updated_at = now()
            where id = $1
        `, merchantUserID); err != nil {
			h.Logger.Error("merchant driver delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove driver access")
			return
		}
		isActive = false
	} else {
		permissions = removePermission(permissions, driverPerm)
		if _, err := h.DB.Exec(ctx, `
            update merchant_users
            set permissions = $1, updated_at = now()
            where id = $2
        `, permissions, merchantUserID); err != nil {
			h.Logger.Error("merchant driver delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove driver access")
			return
		}
		isActive = isActive && containsPermission(permissions, driverPerm)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":       userID,
			"name":     name,
			"phone":    nullableText(phone),
			"email":    email,
			"isActive": isActive,
			"joinedAt": createdAt,
		},
		"message":    "Driver access removed successfully",
		"statusCode": 200,
	})
}

func appendUniquePermission(permissions []string, value string) []string {
	if containsPermission(permissions, value) {
		return permissions
	}
	return append(permissions, value)
}

func containsPermission(permissions []string, value string) bool {
	for _, perm := range permissions {
		if perm == value {
			return true
		}
	}
	return false
}

func removePermission(permissions []string, value string) []string {
	filtered := make([]string, 0, len(permissions))
	for _, perm := range permissions {
		if perm != value {
			filtered = append(filtered, perm)
		}
	}
	return filtered
}
