package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"genfity-order-services/internal/voucher"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type publicVoucherValidateRequest struct {
	MerchantCode string                 `json:"merchantCode"`
	VoucherCode  string                 `json:"voucherCode"`
	OrderType    string                 `json:"orderType"`
	Items        []publicVoucherItemReq `json:"items"`
}

type publicVoucherItemReq struct {
	MenuID   any     `json:"menuId"`
	Subtotal float64 `json:"subtotal"`
}

func (h *Handler) PublicVoucherValidate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body publicVoucherValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantCode := strings.TrimSpace(body.MerchantCode)
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "merchantCode is required")
		return
	}

	voucherCode := strings.ToUpper(strings.TrimSpace(body.VoucherCode))
	if voucherCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "voucherCode is required")
		return
	}

	orderType := strings.ToUpper(strings.TrimSpace(body.OrderType))
	if orderType != "DINE_IN" && orderType != "TAKEAWAY" && orderType != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "orderType is required")
		return
	}

	var (
		merchantID int64
		currency   pgtype.Text
		timezone   pgtype.Text
		isActive   bool
	)
	if err := h.DB.QueryRow(ctx, `
		select id, currency, timezone, is_active
		from merchants
		where code = $1
	`, merchantCode).Scan(&merchantID, &currency, &timezone, &isActive); err != nil || !isActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	itemInputs := make([]voucher.OrderItemInput, 0)
	var subtotal float64
	for _, item := range body.Items {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			continue
		}
		if item.Subtotal <= 0 {
			continue
		}
		subtotal += item.Subtotal
		itemInputs = append(itemInputs, voucher.OrderItemInput{MenuID: menuID, Subtotal: item.Subtotal})
	}

	params := voucher.ComputeParams{
		MerchantID:       merchantID,
		MerchantCurrency: textOrDefault(currency, "AUD"),
		MerchantTimezone: textOrDefault(timezone, "Australia/Sydney"),
		Audience:         voucher.Audience("CUSTOMER"),
		OrderType:        orderType,
		Subtotal:         subtotal,
		Items:            itemInputs,
		VoucherCode:      voucherCode,
	}

	computed, verr := voucher.ComputeVoucherDiscount(ctx, h.DB, params)
	if verr != nil {
		writeVoucherError(w, verr)
		return
	}

	data := map[string]any{
		"templateId":       computed.TemplateID,
		"codeId":           computed.CodeID,
		"label":            computed.Label,
		"discountType":     string(computed.DiscountType),
		"discountValue":    computed.DiscountValue,
		"discountAmount":   computed.DiscountAmount,
		"eligibleSubtotal": computed.EligibleSubtotal,
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       data,
		"message":    "Voucher valid",
		"statusCode": 200,
	})
}

func textOrDefault(value pgtype.Text, fallback string) string {
	if value.Valid && strings.TrimSpace(value.String) != "" {
		return value.String
	}
	return fallback
}
