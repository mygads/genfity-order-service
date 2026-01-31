package handlers

import (
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) MerchantPaymentVerify(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found in context")
		return
	}

	orderNumber := strings.TrimSpace(r.URL.Query().Get("orderNumber"))
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	var (
		orderID       int64
		orderStatus   string
		totalAmount   pgtype.Numeric
		paymentMethod pgtype.Text
		paymentStatus pgtype.Text
	)
	query := `
        select o.id, o.status, o.total_amount, p.payment_method, p.status
        from orders o
        left join payments p on p.order_id = o.id
        where o.order_number = $1 and o.merchant_id = $2
        limit 1
    `
	if err := h.DB.QueryRow(ctx, query, orderNumber, *authCtx.MerchantID).Scan(&orderID, &orderStatus, &totalAmount, &paymentMethod, &paymentStatus); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
			return
		}
		h.Logger.Error("payment verify lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to verify order")
		return
	}

	payload := map[string]any{
		"id":          orderID,
		"orderNumber": orderNumber,
		"status":      orderStatus,
		"totalAmount": utils.NumericToFloat64(totalAmount),
		"payment": map[string]any{
			"paymentMethod": nullableText(paymentMethod),
			"status":        nullableText(paymentStatus),
		},
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
		"message": "Order verified successfully",
	})
}
