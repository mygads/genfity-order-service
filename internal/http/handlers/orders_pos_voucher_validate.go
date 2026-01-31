package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/internal/voucher"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type validateVoucherRequest struct {
	OrderID     any    `json:"orderId"`
	VoucherCode string `json:"voucherCode"`
}

type validateVoucherTemplateRequest struct {
	OrderID           any `json:"orderId"`
	VoucherTemplateID any `json:"voucherTemplateId"`
}

func (h *Handler) MerchantPOSValidateVoucher(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body validateVoucherRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	orderID, ok := parseNumericID(body.OrderID)
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "orderId is required")
		return
	}

	code := strings.TrimSpace(body.VoucherCode)
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "voucherCode is required")
		return
	}

	result, err := h.computeVoucherForOrder(ctx, *authCtx.MerchantID, orderID, voucher.ComputeParams{
		MerchantID:         *authCtx.MerchantID,
		Audience:           voucher.AudiencePOS,
		VoucherCode:        code,
		OrderIDForStacking: &orderID,
	})
	if err != nil {
		writeVoucherError(w, err)
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"templateId":       result.TemplateID,
			"codeId":           result.CodeID,
			"label":            result.Label,
			"discountAmount":   result.DiscountAmount,
			"eligibleSubtotal": result.EligibleSubtotal,
		},
		"message":    "Voucher valid",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantPOSValidateVoucherTemplate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body validateVoucherTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	orderID, ok := parseNumericID(body.OrderID)
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "orderId is required")
		return
	}

	templateID, ok := parseNumericID(body.VoucherTemplateID)
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "voucherTemplateId is required")
		return
	}

	result, err := h.computeVoucherForOrder(ctx, *authCtx.MerchantID, orderID, voucher.ComputeParams{
		MerchantID:         *authCtx.MerchantID,
		Audience:           voucher.AudiencePOS,
		VoucherTemplateID:  &templateID,
		OrderIDForStacking: &orderID,
	})
	if err != nil {
		writeVoucherError(w, err)
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"templateId":       result.TemplateID,
			"codeId":           result.CodeID,
			"label":            result.Label,
			"discountAmount":   result.DiscountAmount,
			"eligibleSubtotal": result.EligibleSubtotal,
		},
		"message":    "Voucher valid",
		"statusCode": 200,
	})
}

func (h *Handler) computeVoucherForOrder(ctx context.Context, merchantID int64, orderID int64, base voucher.ComputeParams) (*voucher.DiscountResult, *voucher.Error) {
	var (
		orderType string
		subtotal  pgtype.Numeric
	)
	items := make([]voucher.OrderItemInput, 0)

	rows, err := h.DB.Query(ctx, `
		select o.order_type, o.subtotal, oi.menu_id, oi.subtotal
		from orders o
		left join order_items oi on oi.order_id = o.id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, merchantID)
	if err != nil {
		return nil, voucher.ValidationError(voucher.ErrVoucherNotFound, "Order not found", nil)
	}
	defer rows.Close()

	first := true
	for rows.Next() {
		var (
			menuID       pgtype.Int8
			itemSubtotal pgtype.Numeric
		)
		if err := rows.Scan(&orderType, &subtotal, &menuID, &itemSubtotal); err != nil {
			return nil, voucher.ValidationError(voucher.ErrVoucherNotFound, "Order not found", nil)
		}
		first = false
		if menuID.Valid {
			items = append(items, voucher.OrderItemInput{
				MenuID:   menuID.Int64,
				Subtotal: utils.NumericToFloat64(itemSubtotal),
			})
		}
	}
	if first {
		return nil, voucher.ValidationError(voucher.ErrVoucherNotFound, "Order not found", nil)
	}

	merchantCurrency, merchantTimezone, err := h.getMerchantCurrencyTimezone(ctx, merchantID)
	if err != nil {
		h.Logger.Error("voucher validate merchant lookup failed", zapError(err))
		return nil, voucher.ValidationError(voucher.ErrVoucherNotFound, "Order not found", nil)
	}

	params := base
	params.OrderType = orderType
	params.Subtotal = utils.NumericToFloat64(subtotal)
	params.Items = items
	params.MerchantCurrency = merchantCurrency
	params.MerchantTimezone = merchantTimezone
	params.ExcludeOrderIDFromUsage = &orderID

	return voucher.ComputeVoucherDiscount(ctx, h.DB, params)
}

func writeVoucherError(w http.ResponseWriter, err *voucher.Error) {
	status := err.StatusCode
	if status == 0 {
		status = http.StatusBadRequest
	}
	response.JSON(w, status, map[string]any{
		"success":    false,
		"error":      string(err.Code),
		"message":    err.Message,
		"statusCode": status,
		"details":    err.Details,
	})
}

func parseNumericID(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		if v <= 0 || math.IsNaN(v) {
			return 0, false
		}
		return int64(v), true
	case int:
		if v <= 0 {
			return 0, false
		}
		return int64(v), true
	case int64:
		if v <= 0 {
			return 0, false
		}
		return v, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil || parsed <= 0 {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
