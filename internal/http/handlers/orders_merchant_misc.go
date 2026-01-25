package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"
)

func (h *Handler) MerchantOrderCancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if strings.TrimSpace(body.Reason) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cancellation reason is required")
		return
	}

	if _, err := h.DB.Exec(ctx, `
		update orders
		set status = 'CANCELLED', cancelled_at = $1, cancel_reason = $2
		where id = $3 and merchant_id = $4
	`, time.Now(), body.Reason, orderID, *authCtx.MerchantID); err != nil {
		h.Logger.Error("order cancel failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to cancel order")
		return
	}

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    data,
		"message": "Order cancelled successfully",
	})
}

func (h *Handler) MerchantOrderTrackingToken(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var (
		orderNumber  string
		merchantCode string
	)
	if err := h.DB.QueryRow(ctx, `
		select o.order_number, m.code
		from orders o
		join merchants m on m.id = o.merchant_id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, *authCtx.MerchantID).Scan(&orderNumber, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	if merchantCode == "" {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	token := utils.CreateOrderTrackingToken(h.Config.OrderTrackingTokenSecret, merchantCode, orderNumber)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"trackingToken": token,
		},
		"message": "Tracking token minted successfully",
	})
}
