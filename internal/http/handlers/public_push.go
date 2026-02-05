package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
)

// PushSubscriptionKeys represents the keys for a push subscription
type PushSubscriptionKeys struct {
	P256dh string `json:"p256dh"`
	Auth   string `json:"auth"`
}

// PushSubscription represents a browser push subscription
type PushSubscription struct {
	Endpoint string               `json:"endpoint"`
	Keys     PushSubscriptionKeys `json:"keys"`
}

// PushSubscribeRequest represents the request body for subscribing to push
type PushSubscribeRequest struct {
	Subscription PushSubscription `json:"subscription"`
	OrderNumbers []string         `json:"orderNumbers"`
	CustomerID   *string          `json:"customerId"`
	UserAgent    string           `json:"userAgent"`
}

// PushAddOrderRequest represents the request body for adding an order to a subscription
type PushAddOrderRequest struct {
	Endpoint    string `json:"endpoint"`
	OrderNumber string `json:"orderNumber"`
}

// CustomerPushSubscriptionData represents a push subscription record
type CustomerPushSubscriptionData struct {
	ID           int64    `json:"id,string"`
	CustomerID   *int64   `json:"customerId,omitempty,string"`
	Endpoint     string   `json:"endpoint"`
	P256dhKey    string   `json:"p256dhKey"`
	AuthKey      string   `json:"authKey"`
	OrderNumbers []string `json:"orderNumbers"`
	UserAgent    *string  `json:"userAgent,omitempty"`
	IsActive     bool     `json:"isActive"`
}

// PublicPushGetVAPIDKey returns the VAPID public key for client-side subscription
// GET /api/public/push/subscribe
func (h *Handler) PublicPushGetVAPIDKey(w http.ResponseWriter, r *http.Request) {
	vapidKey := h.Config.VAPIDPublicKey
	if vapidKey == "" {
		response.Error(w, http.StatusServiceUnavailable, "NOT_CONFIGURED", "Push notifications not configured")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]string{
			"vapidPublicKey": vapidKey,
		},
	})
}

// PublicPushSubscribe creates or updates a push subscription
// POST /api/public/push/subscribe
func (h *Handler) PublicPushSubscribe(w http.ResponseWriter, r *http.Request) {
	var req PushSubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_BODY", "Invalid request body")
		return
	}

	// Validate subscription data
	if req.Subscription.Endpoint == "" || req.Subscription.Keys.P256dh == "" || req.Subscription.Keys.Auth == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid subscription data")
		return
	}

	// Check if push is configured
	if h.Config.VAPIDPublicKey == "" {
		response.Error(w, http.StatusServiceUnavailable, "NOT_CONFIGURED", "Push notifications are not configured")
		return
	}

	// Parse customer ID if provided
	var customerID *int64
	if req.CustomerID != nil && *req.CustomerID != "" {
		trimmed := strings.TrimSpace(*req.CustomerID)
		if id, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			customerID = &id
		} else {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid customerId")
			return
		}
	}

	ctx := r.Context()

	// Check if subscription already exists by endpoint
	var existingID int64
	var existingCustomerID *int64
	var existingOrderNumbers []string
	var existingUserAgent *string

	err := h.DB.QueryRow(ctx, `
		SELECT id, customer_id, order_numbers, user_agent 
		FROM customer_push_subscriptions 
		WHERE endpoint = $1
		LIMIT 1
	`, req.Subscription.Endpoint).Scan(&existingID, &existingCustomerID, &existingOrderNumbers, &existingUserAgent)

	if err == nil {
		// Update existing subscription
		orderNumbers := req.OrderNumbers
		if orderNumbers == nil {
			orderNumbers = []string{}
		}

		// Merge order numbers
		existingSet := make(map[string]bool)
		for _, on := range existingOrderNumbers {
			existingSet[on] = true
		}
		for _, on := range orderNumbers {
			existingSet[on] = true
		}
		mergedOrderNumbers := make([]string, 0, len(existingSet))
		for on := range existingSet {
			mergedOrderNumbers = append(mergedOrderNumbers, on)
		}

		// Use new customer ID if provided, otherwise keep existing
		finalCustomerID := customerID
		if finalCustomerID == nil {
			finalCustomerID = existingCustomerID
		}

		// Use new user agent if provided, otherwise keep existing
		finalUserAgent := existingUserAgent
		if req.UserAgent != "" {
			finalUserAgent = &req.UserAgent
		}

		var updated CustomerPushSubscriptionData
		err = h.DB.QueryRow(ctx, `
			UPDATE customer_push_subscriptions
			SET p256dh_key = $1,
				auth_key = $2,
				customer_id = $3,
				order_numbers = $4,
				user_agent = $5,
				is_active = true,
				updated_at = NOW()
			WHERE id = $6
			RETURNING id, customer_id, endpoint, p256dh_key, auth_key, order_numbers, user_agent, is_active
		`, req.Subscription.Keys.P256dh, req.Subscription.Keys.Auth, finalCustomerID, mergedOrderNumbers, finalUserAgent, existingID).
			Scan(&updated.ID, &updated.CustomerID, &updated.Endpoint, &updated.P256dhKey, &updated.AuthKey, &updated.OrderNumbers, &updated.UserAgent, &updated.IsActive)

		if err != nil {
			h.Logger.Error("Failed to update push subscription", zapError(err))
			response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to save subscription")
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": "Subscription updated",
			"data":    updated,
		})
		return
	}

	if err != pgx.ErrNoRows {
		h.Logger.Error("Failed to check existing push subscription", zapError(err))
		response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to save subscription")
		return
	}

	// Create new subscription
	orderNumbers := req.OrderNumbers
	if orderNumbers == nil {
		orderNumbers = []string{}
	}

	var userAgent *string
	if req.UserAgent != "" {
		userAgent = &req.UserAgent
	}

	var newSub CustomerPushSubscriptionData
	err = h.DB.QueryRow(ctx, `
		INSERT INTO customer_push_subscriptions (endpoint, p256dh_key, auth_key, customer_id, order_numbers, user_agent, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), NOW())
		RETURNING id, customer_id, endpoint, p256dh_key, auth_key, order_numbers, user_agent, is_active
	`, req.Subscription.Endpoint, req.Subscription.Keys.P256dh, req.Subscription.Keys.Auth, customerID, orderNumbers, userAgent).
		Scan(&newSub.ID, &newSub.CustomerID, &newSub.Endpoint, &newSub.P256dhKey, &newSub.AuthKey, &newSub.OrderNumbers, &newSub.UserAgent, &newSub.IsActive)

	if err != nil {
		h.Logger.Error("Failed to create push subscription", zapError(err))
		response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to save subscription")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Subscription created",
		"data":    newSub,
	})
}

// PublicPushAddOrder adds an order number to an existing subscription
// PATCH /api/public/push/subscribe
func (h *Handler) PublicPushAddOrder(w http.ResponseWriter, r *http.Request) {
	var req PushAddOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_BODY", "Invalid request body")
		return
	}

	if req.Endpoint == "" || req.OrderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Endpoint and orderNumber are required")
		return
	}

	ctx := r.Context()

	// Find existing subscription
	var subID int64
	var orderNumbers []string
	err := h.DB.QueryRow(ctx, `
		SELECT id, order_numbers 
		FROM customer_push_subscriptions 
		WHERE endpoint = $1 AND is_active = true
		LIMIT 1
	`, req.Endpoint).Scan(&subID, &orderNumbers)

	if err == pgx.ErrNoRows {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Subscription not found")
		return
	}
	if err != nil {
		h.Logger.Error("Failed to find push subscription", zapError(err))
		response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to add order")
		return
	}

	// Add order number if not already present
	found := false
	for _, on := range orderNumbers {
		if on == req.OrderNumber {
			found = true
			break
		}
	}
	if !found {
		orderNumbers = append(orderNumbers, req.OrderNumber)
	}

	// Update subscription
	var updated CustomerPushSubscriptionData
	err = h.DB.QueryRow(ctx, `
		UPDATE customer_push_subscriptions
		SET order_numbers = $1, updated_at = NOW()
		WHERE id = $2
		RETURNING id, customer_id, endpoint, p256dh_key, auth_key, order_numbers, user_agent, is_active
	`, orderNumbers, subID).
		Scan(&updated.ID, &updated.CustomerID, &updated.Endpoint, &updated.P256dhKey, &updated.AuthKey, &updated.OrderNumbers, &updated.UserAgent, &updated.IsActive)

	if err != nil {
		h.Logger.Error("Failed to add order to push subscription", zapError(err))
		response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to add order")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Order added to subscription",
		"data":    updated,
	})
}

// PublicPushUnsubscribe deactivates a push subscription
// DELETE /api/public/push/subscribe
func (h *Handler) PublicPushUnsubscribe(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpoint")
	if endpoint == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Endpoint parameter is required")
		return
	}

	ctx := r.Context()

	result, err := h.DB.Exec(ctx, `
		UPDATE customer_push_subscriptions
		SET is_active = false, updated_at = NOW()
		WHERE endpoint = $1
	`, endpoint)

	if err != nil {
		h.Logger.Error("Failed to unsubscribe push subscription", zapError(err))
		response.Error(w, http.StatusInternalServerError, "DB_ERROR", "Failed to unsubscribe")
		return
	}

	rowsAffected := result.RowsAffected()
	message := "Subscription not found"
	if rowsAffected > 0 {
		message = "Subscription deactivated"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": message,
	})
}
