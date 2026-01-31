package handlers

import (
	"net/http"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"
)

func (h *Handler) MerchantUsersList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select u.id, u.name, u.email, u.role, u.is_active
        from merchant_users mu
        join users u on u.id = mu.user_id
        where mu.merchant_id = $1
    `, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("merchant users query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve users")
		return
	}
	defer rows.Close()

	users := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id       int64
			name     string
			email    string
			role     string
			isActive bool
		)
		if err := rows.Scan(&id, &name, &email, &role, &isActive); err != nil {
			h.Logger.Error("merchant users scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve users")
			return
		}

		users = append(users, map[string]any{
			"id":       id,
			"name":     name,
			"email":    email,
			"role":     role,
			"isActive": isActive,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"users": users,
		},
		"message":    "Users retrieved successfully",
		"statusCode": 200,
	})
}
