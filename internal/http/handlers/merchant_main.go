package handlers

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"
)

type mainMerchantPayload struct {
	Name        string
	Code        string
	Description *string
	Address     *string
	PhoneNumber *string
	Email       *string
	IsOpen      *bool
	Country     *string
	Currency    *string
	Timezone    *string
	Latitude    *float64
	Longitude   *float64
}

func (h *Handler) MerchantMainCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Authentication required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	payload, err := decodeMainMerchantPayload(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Name == "" || payload.Code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant name and code are required")
		return
	}

	code := strings.ToUpper(strings.TrimSpace(payload.Code))
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	if exists, _ := h.merchantCodeExists(ctx, code); exists {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 10; i++ {
			code = randomMerchantCode(rng, 4)
			if exists, _ := h.merchantCodeExists(ctx, code); !exists {
				break
			}
		}
		if exists, _ := h.merchantCodeExists(ctx, code); exists {
			response.Error(w, http.StatusConflict, "MERCHANT_CODE_EXISTS", "Unable to generate unique merchant code")
			return
		}
	}

	currency := "AUD"
	if payload.Currency != nil && strings.TrimSpace(*payload.Currency) != "" {
		currency = strings.TrimSpace(*payload.Currency)
	}

	timezone := "Australia/Sydney"
	if payload.Timezone != nil && strings.TrimSpace(*payload.Timezone) != "" {
		timezone = strings.TrimSpace(*payload.Timezone)
	}

	country := "Australia"
	if payload.Country != nil && strings.TrimSpace(*payload.Country) != "" {
		country = strings.TrimSpace(*payload.Country)
	}

	isOpen := true
	if payload.IsOpen != nil {
		isOpen = *payload.IsOpen
	}

	receiptSettings := defaultReceiptSettings()
	if strings.EqualFold(currency, "IDR") {
		receiptSettings["receiptLanguage"] = "id"
	} else {
		receiptSettings["receiptLanguage"] = "en"
	}
	receiptSettings["showEmail"] = true
	receiptSettings["showCustomerPhone"] = true
	receiptSettings["showAddonPrices"] = true
	receiptSettings["showUnitPrice"] = true
	receiptSettings["showCustomFooterText"] = false
	receiptSettings["customFooterText"] = nil
	receiptSettings["customThankYouMessage"] = nil
	receiptSettings["sendCompletedOrderEmailToCustomer"] = false

	features := map[string]any{
		"orderVouchers": map[string]any{
			"posDiscountsEnabled": true,
			"customerEnabled":     false,
		},
		"pos": map[string]any{
			"customItems": map[string]any{
				"enabled": false,
			},
		},
	}

	receiptJSON, _ := json.Marshal(receiptSettings)
	featuresJSON, _ := json.Marshal(features)

	var newID int64
	err = h.DB.QueryRow(ctx, `
		insert into merchants (
			code, name, description, address, phone, email, is_open, country, currency, timezone,
			latitude, longitude, branch_type, parent_merchant_id,
			enable_tax, tax_percentage, enable_service_charge, service_charge_percent,
			enable_packaging_fee, packaging_fee_amount,
			is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled, enforce_delivery_zones,
			require_table_number_for_dine_in, pos_pay_immediately, is_reservation_enabled,
			is_scheduled_order_enabled, reservation_menu_required, reservation_min_item_count,
			features, receipt_settings, is_active, created_at, updated_at
		) values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,'MAIN',null,
			false,null,false,null,
			false,null,
			true,true,false,true,
			true,true,false,
			false,false,0,
			$13,$14,true,now(),now()
		) returning id
	`,
		code,
		payload.Name,
		payload.Description,
		payload.Address,
		payload.PhoneNumber,
		stringPtrOrNil(payload.Email),
		isOpen,
		country,
		currency,
		timezone,
		payload.Latitude,
		payload.Longitude,
		featuresJSON,
		receiptJSON,
	).Scan(&newID)
	if err != nil {
		h.Logger.Error("main merchant create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create main merchant")
		return
	}

	_, _ = h.DB.Exec(ctx, `
		insert into merchant_users (merchant_id, user_id, role, is_active, permissions, invitation_status, created_at, updated_at)
		values ($1,$2,'OWNER',true,$3,'ACCEPTED',now(),now())
	`, newID, authCtx.UserID, []string{})

	merchant, err := h.fetchBranchSummary(ctx, newID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create main merchant")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchant": merchant,
		},
		"message":    "Main merchant created successfully",
		"statusCode": 201,
	})
}

func decodeMainMerchantPayload(r *http.Request) (mainMerchantPayload, error) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return mainMerchantPayload{}, err
	}

	payload := mainMerchantPayload{}
	payload.Name = readStringField(body["name"])
	payload.Code = readStringField(body["code"])
	payload.Description = readOptionalString(body["description"])
	payload.Address = readOptionalString(body["address"])
	payload.PhoneNumber = readOptionalString(body["phoneNumber"])
	payload.Email = readOptionalString(body["email"])
	payload.IsOpen = readOptionalBool(body["isOpen"])
	payload.Country = readOptionalString(body["country"])
	payload.Currency = readOptionalString(body["currency"])
	payload.Timezone = readOptionalString(body["timezone"])
	payload.Latitude = readOptionalFloat(body["latitude"])
	payload.Longitude = readOptionalFloat(body["longitude"])
	return payload, nil
}
