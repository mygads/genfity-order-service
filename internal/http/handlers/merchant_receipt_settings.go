package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) MerchantReceiptSettingsGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		currency pgtype.Text
		receipt  []byte
	)
	if err := h.DB.QueryRow(ctx, `select currency, receipt_settings from merchants where id = $1`, *authCtx.MerchantID).Scan(&currency, &receipt); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	current := parseJSONMap(receipt)
	merged := mergeReceiptSettings(defaultReceiptSettings(), current, nil)

	fee := h.fetchCompletedOrderEmailFee(ctx, textOrDefault(currency, "IDR"))
	balance := h.fetchMerchantBalanceAmount(ctx, *authCtx.MerchantID)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"receiptSettings":        merged,
			"completedOrderEmailFee": fee,
			"currentBalance":         balance,
		},
		"statusCode": 200,
	})
}

func (h *Handler) MerchantReceiptSettingsPut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "RECEIPT_SETTINGS_INVALID", "Invalid receipt settings payload")
		return
	}

	rawSettings, ok := body["receiptSettings"].(map[string]any)
	if !ok {
		response.Error(w, http.StatusBadRequest, "RECEIPT_SETTINGS_INVALID", "Invalid receipt settings payload")
		return
	}

	patch, deleteKeys, err := parseReceiptSettingsPatch(rawSettings)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "RECEIPT_SETTINGS_INVALID", "Invalid receipt settings payload")
		return
	}

	var (
		currency pgtype.Text
		receipt  []byte
	)
	if err := h.DB.QueryRow(ctx, `select currency, receipt_settings from merchants where id = $1`, *authCtx.MerchantID).Scan(&currency, &receipt); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	current := parseJSONMap(receipt)
	merged := mergeReceiptSettings(defaultReceiptSettings(), current, patch)
	for key := range deleteKeys {
		delete(merged, key)
	}

	if enabled, ok := merged["sendCompletedOrderEmailToCustomer"].(bool); ok && enabled {
		fee := h.fetchCompletedOrderEmailFee(ctx, textOrDefault(currency, "IDR"))
		if fee <= 0 {
			response.Error(w, http.StatusBadRequest, "COMPLETED_EMAIL_FEE_NOT_CONFIGURED", "Completed-order email fee is not configured. Please contact support.")
			return
		}
		balance := h.fetchMerchantBalanceAmount(ctx, *authCtx.MerchantID)
		if balance < fee {
			response.Error(w, http.StatusBadRequest, "INSUFFICIENT_BALANCE", "Insufficient balance to enable completed-order email.")
			return
		}
	}

	payload, err := json.Marshal(merged)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update receipt settings")
		return
	}

	if _, err := h.DB.Exec(ctx, `update merchants set receipt_settings = $1, updated_at = now() where id = $2`, payload, *authCtx.MerchantID); err != nil {
		h.Logger.Error("receipt settings update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update receipt settings")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"receiptSettings": merged,
		},
		"message":    "Receipt settings updated successfully",
		"statusCode": 200,
	})
}

func defaultReceiptSettings() map[string]any {
	return map[string]any{
		"sendCompletedOrderEmailToCustomer": false,
		"paperSize":                         "80mm",
		"receiptLanguage":                   "en",
		"showLogo":                          true,
		"showMerchantName":                  true,
		"showAddress":                       true,
		"showPhone":                         true,
		"showEmail":                         false,
		"showOrderNumber":                   true,
		"showOrderType":                     true,
		"showTableNumber":                   true,
		"showDateTime":                      true,
		"showCustomerName":                  true,
		"showCustomerPhone":                 false,
		"showItemNotes":                     true,
		"showAddons":                        true,
		"showAddonPrices":                   false,
		"showUnitPrice":                     false,
		"showSubtotal":                      true,
		"showTax":                           true,
		"showServiceCharge":                 true,
		"showPackagingFee":                  true,
		"showDeliveryFee":                   true,
		"showDiscount":                      true,
		"showTotal":                         true,
		"showAmountPaid":                    true,
		"showChange":                        true,
		"showPaymentMethod":                 true,
		"showCashierName":                   true,
		"showThankYouMessage":               true,
		"showCustomFooterText":              false,
		"showFooterPhone":                   true,
		"showTrackingQRCode":                true,
	}
}

func mergeReceiptSettings(base map[string]any, current map[string]any, patch map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range current {
		merged[key] = value
	}
	for key, value := range patch {
		merged[key] = value
	}
	return merged
}

func parseReceiptSettingsPatch(raw map[string]any) (map[string]any, map[string]bool, error) {
	boolFields := map[string]bool{
		"sendCompletedOrderEmailToCustomer": true,
		"showLogo":                          true,
		"showMerchantName":                  true,
		"showAddress":                       true,
		"showPhone":                         true,
		"showEmail":                         true,
		"showOrderNumber":                   true,
		"showOrderType":                     true,
		"showTableNumber":                   true,
		"showDateTime":                      true,
		"showCustomerName":                  true,
		"showCustomerPhone":                 true,
		"showItemNotes":                     true,
		"showAddons":                        true,
		"showAddonPrices":                   true,
		"showUnitPrice":                     true,
		"showSubtotal":                      true,
		"showTax":                           true,
		"showServiceCharge":                 true,
		"showPackagingFee":                  true,
		"showDeliveryFee":                   true,
		"showDiscount":                      true,
		"showTotal":                         true,
		"showAmountPaid":                    true,
		"showChange":                        true,
		"showPaymentMethod":                 true,
		"showCashierName":                   true,
		"showThankYouMessage":               true,
		"showCustomFooterText":              true,
		"showFooterPhone":                   true,
		"showTrackingQRCode":                true,
	}

	patch := make(map[string]any)
	deleteKeys := make(map[string]bool)

	for key, value := range raw {
		switch key {
		case "paperSize":
			str, ok := value.(string)
			if !ok {
				return nil, nil, errReceiptSettingsInvalid
			}
			if str != "58mm" && str != "80mm" {
				return nil, nil, errReceiptSettingsInvalid
			}
			patch[key] = str
		case "receiptLanguage":
			str, ok := value.(string)
			if !ok {
				return nil, nil, errReceiptSettingsInvalid
			}
			str = strings.TrimSpace(str)
			if str != "en" && str != "id" {
				return nil, nil, errReceiptSettingsInvalid
			}
			patch[key] = str
		case "customThankYouMessage":
			str, ok := value.(string)
			if !ok {
				return nil, nil, errReceiptSettingsInvalid
			}
			trimmed := strings.TrimSpace(str)
			if trimmed == "" {
				deleteKeys[key] = true
				continue
			}
			if len(trimmed) > 300 {
				return nil, nil, errReceiptSettingsInvalid
			}
			patch[key] = trimmed
		case "customFooterText":
			str, ok := value.(string)
			if !ok {
				return nil, nil, errReceiptSettingsInvalid
			}
			trimmed := strings.TrimSpace(str)
			if trimmed == "" {
				deleteKeys[key] = true
				continue
			}
			if len(trimmed) > 500 {
				return nil, nil, errReceiptSettingsInvalid
			}
			patch[key] = trimmed
		default:
			if !boolFields[key] {
				return nil, nil, errReceiptSettingsInvalid
			}
			boolValue, ok := value.(bool)
			if !ok {
				return nil, nil, errReceiptSettingsInvalid
			}
			patch[key] = boolValue
		}
	}

	return patch, deleteKeys, nil
}

var errReceiptSettingsInvalid = errors.New("invalid receipt settings")
