package handlers

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"
)

type posCustomItemsPatch struct {
	Enabled       *bool    `json:"enabled"`
	MaxNameLength *int     `json:"maxNameLength"`
	MaxPrice      *float64 `json:"maxPrice"`
}

type posEditOrderPatch struct {
	Enabled *bool `json:"enabled"`
}

type posSettingsRequest struct {
	CustomItems *posCustomItemsPatch `json:"customItems"`
	EditOrder   *posEditOrderPatch   `json:"editOrder"`
}

func (h *Handler) MerchantPOSSettingsGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		currency string
		features []byte
	)
	if err := h.DB.QueryRow(ctx, `select currency, features from merchants where id = $1`, *authCtx.MerchantID).Scan(&currency, &features); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	customSettings := parsePosCustomItemsSettings(features, currency)
	editSettings := parsePosEditOrderSettings(features)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchantId":  fmt.Sprint(*authCtx.MerchantID),
			"customItems": buildPosCustomItemsResponse(customSettings),
			"editOrder":   buildPosEditOrderResponse(editSettings),
		},
		"message":    "POS settings retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantPOSSettingsPut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body posSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "POS_SETTINGS_INVALID", "Invalid POS settings payload")
		return
	}

	if body.CustomItems == nil {
		response.Error(w, http.StatusBadRequest, "POS_SETTINGS_INVALID", "Invalid POS settings payload")
		return
	}

	if body.CustomItems.MaxNameLength != nil {
		if *body.CustomItems.MaxNameLength < 10 || *body.CustomItems.MaxNameLength > 200 {
			response.Error(w, http.StatusBadRequest, "POS_SETTINGS_INVALID", "Invalid POS settings payload")
			return
		}
	}

	if body.CustomItems.MaxPrice != nil {
		if *body.CustomItems.MaxPrice <= 0 || math.IsNaN(*body.CustomItems.MaxPrice) || math.IsInf(*body.CustomItems.MaxPrice, 0) {
			response.Error(w, http.StatusBadRequest, "POS_SETTINGS_INVALID", "Invalid POS settings payload")
			return
		}
	}

	var (
		currency string
		features []byte
	)
	if err := h.DB.QueryRow(ctx, `select currency, features from merchants where id = $1`, *authCtx.MerchantID).Scan(&currency, &features); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	updated := mergePosCustomItemsFeatures(features, body.CustomItems)
	editPatch := body.EditOrder
	if editPatch == nil {
		editPatch = &posEditOrderPatch{}
	}
	updated = mergePosEditOrderFeatures(updated, editPatch)

	var (
		updatedID       int64
		updatedCurrency string
		updatedFeatures []byte
	)
	if err := h.DB.QueryRow(ctx, `
		update merchants set features = $1, updated_at = now()
		where id = $2
		returning id, currency, features
	`, updated, *authCtx.MerchantID).Scan(&updatedID, &updatedCurrency, &updatedFeatures); err != nil {
		h.Logger.Error("pos settings update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update POS settings")
		return
	}

	customSettings := parsePosCustomItemsSettings(updatedFeatures, updatedCurrency)
	editSettings := parsePosEditOrderSettings(updatedFeatures)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchantId":  fmt.Sprint(updatedID),
			"customItems": buildPosCustomItemsResponse(customSettings),
			"editOrder":   buildPosEditOrderResponse(editSettings),
		},
		"message":    "POS settings updated successfully",
		"statusCode": 200,
	})
}

func mergePosCustomItemsFeatures(features []byte, patch *posCustomItemsPatch) []byte {
	obj := parseJSONMap(features)
	pos, _ := obj["pos"].(map[string]any)
	if pos == nil {
		pos = map[string]any{}
	}
	custom, _ := pos["customItems"].(map[string]any)
	if custom == nil {
		custom = map[string]any{}
	}
	if patch != nil {
		if patch.Enabled != nil {
			custom["enabled"] = *patch.Enabled
		} else {
			delete(custom, "enabled")
		}
		if patch.MaxNameLength != nil {
			custom["maxNameLength"] = *patch.MaxNameLength
		} else {
			delete(custom, "maxNameLength")
		}
		if patch.MaxPrice != nil {
			custom["maxPrice"] = *patch.MaxPrice
		} else {
			delete(custom, "maxPrice")
		}
	}
	pos["customItems"] = custom
	obj["pos"] = pos

	payload, _ := json.Marshal(obj)
	return payload
}

func mergePosEditOrderFeatures(features []byte, patch *posEditOrderPatch) []byte {
	obj := parseJSONMap(features)
	pos, _ := obj["pos"].(map[string]any)
	if pos == nil {
		pos = map[string]any{}
	}
	editOrder, _ := pos["editOrder"].(map[string]any)
	if editOrder == nil {
		editOrder = map[string]any{}
	}
	if patch != nil {
		if patch.Enabled != nil {
			editOrder["enabled"] = *patch.Enabled
		} else {
			delete(editOrder, "enabled")
		}
	}
	pos["editOrder"] = editOrder
	obj["pos"] = pos

	payload, _ := json.Marshal(obj)
	return payload
}

func buildPosCustomItemsResponse(settings posCustomSettings) map[string]any {
	return map[string]any{
		"enabled":       settings.Enabled,
		"maxNameLength": settings.MaxNameLength,
		"maxPrice":      settings.MaxPrice,
	}
}

func buildPosEditOrderResponse(settings posEditSettings) map[string]any {
	return map[string]any{
		"enabled": settings.Enabled,
	}
}
