package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type merchantOpeningHour struct {
	ID         int64     `json:"id"`
	MerchantID int64     `json:"merchantId"`
	DayOfWeek  int       `json:"dayOfWeek"`
	OpenTime   *string   `json:"openTime"`
	CloseTime  *string   `json:"closeTime"`
	IsClosed   bool      `json:"isClosed"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

func (h *Handler) PublicMerchant(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var (
		merchantID                  int64
		code                        string
		name                        string
		email                       string
		phone                       pgtype.Text
		address                     pgtype.Text
		city                        pgtype.Text
		state                       pgtype.Text
		postalCode                  pgtype.Text
		country                     string
		logoURL                     pgtype.Text
		bannerURL                   pgtype.Text
		mapURL                      pgtype.Text
		description                 pgtype.Text
		isActive                    bool
		isOpen                      bool
		isManualOverride            bool
		isDineInEnabled             bool
		isTakeawayEnabled           bool
		requireTableNumberForDineIn bool
		dineInLabel                 pgtype.Text
		takeawayLabel               pgtype.Text
		deliveryLabel               pgtype.Text
		dineInScheduleStart         pgtype.Text
		dineInScheduleEnd           pgtype.Text
		takeawayScheduleStart       pgtype.Text
		takeawayScheduleEnd         pgtype.Text
		deliveryScheduleStart       pgtype.Text
		deliveryScheduleEnd         pgtype.Text
		totalTables                 pgtype.Int4
		isDeliveryEnabled           bool
		enforceDeliveryZones        bool
		deliveryMaxDistanceKm       pgtype.Numeric
		deliveryFeeBase             pgtype.Numeric
		deliveryFeePerKm            pgtype.Numeric
		deliveryFeeMin              pgtype.Numeric
		deliveryFeeMax              pgtype.Numeric
		enableTax                   bool
		taxPercentage               pgtype.Numeric
		enableServiceCharge         bool
		serviceChargePercent        pgtype.Numeric
		enablePackagingFee          bool
		packagingFeeAmount          pgtype.Numeric
		currency                    string
		timezone                    string
		featuresBytes               []byte
		latitude                    pgtype.Numeric
		longitude                   pgtype.Numeric
		isReservationEnabled        bool
		reservationMenuRequired     bool
		reservationMinItemCount     int32
		isScheduledOrderEnabled     bool
		isPerDayModeScheduleEnabled bool
	)

	err := h.DB.QueryRow(ctx, `
		select id, code, name, email, phone, address, city, state, postal_code, country,
		       logo_url, banner_url, map_url, description,
		       is_active, is_open, is_manual_override,
		       is_dine_in_enabled, is_takeaway_enabled, require_table_number_for_dine_in,
		       dine_in_label, takeaway_label, delivery_label,
		       dine_in_schedule_start, dine_in_schedule_end,
		       takeaway_schedule_start, takeaway_schedule_end,
		       delivery_schedule_start, delivery_schedule_end,
		       total_tables,
		       is_delivery_enabled, enforce_delivery_zones,
		       delivery_max_distance_km, delivery_fee_base, delivery_fee_per_km, delivery_fee_min, delivery_fee_max,
		       enable_tax, tax_percentage,
		       enable_service_charge, service_charge_percent,
		       enable_packaging_fee, packaging_fee_amount,
		       currency, timezone,
		       features,
		       latitude, longitude,
		       is_reservation_enabled, reservation_menu_required, reservation_min_item_count,
		       is_scheduled_order_enabled,
		       is_per_day_mode_schedule_enabled
		from merchants
		where code = $1
	`, merchantCode).Scan(
		&merchantID,
		&code,
		&name,
		&email,
		&phone,
		&address,
		&city,
		&state,
		&postalCode,
		&country,
		&logoURL,
		&bannerURL,
		&mapURL,
		&description,
		&isActive,
		&isOpen,
		&isManualOverride,
		&isDineInEnabled,
		&isTakeawayEnabled,
		&requireTableNumberForDineIn,
		&dineInLabel,
		&takeawayLabel,
		&deliveryLabel,
		&dineInScheduleStart,
		&dineInScheduleEnd,
		&takeawayScheduleStart,
		&takeawayScheduleEnd,
		&deliveryScheduleStart,
		&deliveryScheduleEnd,
		&totalTables,
		&isDeliveryEnabled,
		&enforceDeliveryZones,
		&deliveryMaxDistanceKm,
		&deliveryFeeBase,
		&deliveryFeePerKm,
		&deliveryFeeMin,
		&deliveryFeeMax,
		&enableTax,
		&taxPercentage,
		&enableServiceCharge,
		&serviceChargePercent,
		&enablePackagingFee,
		&packagingFeeAmount,
		&currency,
		&timezone,
		&featuresBytes,
		&latitude,
		&longitude,
		&isReservationEnabled,
		&reservationMenuRequired,
		&reservationMinItemCount,
		&isScheduledOrderEnabled,
		&isPerDayModeScheduleEnabled,
	)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	if !isActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_DISABLED", "Merchant is currently disabled")
		return
	}

	// Opening hours
	openingHours := make([]merchantOpeningHour, 0)
	rows, err := h.DB.Query(ctx, `
		select id, merchant_id, day_of_week, open_time, close_time, is_closed, created_at, updated_at
		from merchant_opening_hours
		where merchant_id = $1
		order by day_of_week asc
	`, merchantID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var hour merchantOpeningHour
			var openTime pgtype.Text
			var closeTime pgtype.Text
			if err := rows.Scan(
				&hour.ID,
				&hour.MerchantID,
				&hour.DayOfWeek,
				&openTime,
				&closeTime,
				&hour.IsClosed,
				&hour.CreatedAt,
				&hour.UpdatedAt,
			); err == nil {
				if openTime.Valid {
					hour.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					hour.CloseTime = &closeTime.String
				}
				openingHours = append(openingHours, hour)
			}
		}
	}

	// Subscription status
	subscriptionState, err := h.fetchSubscriptionState(ctx, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve merchant")
		return
	}

	subscriptionStatus := subscriptionState.Status
	var subscriptionSuspendReason any = nil
	if subscriptionState.SuspendReason != nil {
		subscriptionSuspendReason = *subscriptionState.SuspendReason
	}
	var subscriptionType any = nil
	if subscriptionState.Type != "" && subscriptionState.Type != "NONE" {
		subscriptionType = subscriptionState.Type
	}

	// Feature flags: customer vouchers enabled
	customerVouchersEnabled := false
	if len(featuresBytes) > 0 {
		var features map[string]any
		if err := json.Unmarshal(featuresBytes, &features); err == nil {
			if ovRaw, ok := features["orderVouchers"]; ok {
				if ov, ok := ovRaw.(map[string]any); ok {
					customerEnabledRaw, _ := ov["customerEnabled"].(bool)
					posDiscountsEnabledRaw, _ := ov["posDiscountsEnabled"].(bool)
					customerVouchersEnabled = customerEnabledRaw && posDiscountsEnabledRaw
				}
			}
		}
	}

	paymentSettings, paymentAccounts, err := h.fetchPublicPaymentConfig(ctx, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve merchant")
		return
	}

	payload := map[string]any{
		"id":                          strconv.FormatInt(merchantID, 10),
		"code":                        code,
		"name":                        name,
		"email":                       email,
		"phone":                       nullIfEmptyText(phone),
		"address":                     nullIfEmptyText(address),
		"city":                        nullIfEmptyText(city),
		"state":                       nullIfEmptyText(state),
		"postalCode":                  nullIfEmptyText(postalCode),
		"country":                     country,
		"logoUrl":                     nullIfEmptyText(logoURL),
		"bannerUrl":                   nullIfEmptyText(bannerURL),
		"mapUrl":                      nullIfEmptyText(mapURL),
		"description":                 nullIfEmptyText(description),
		"isActive":                    isActive,
		"isOpen":                      isOpen,
		"isManualOverride":            isManualOverride,
		"subscriptionStatus":          subscriptionStatus,
		"subscriptionSuspendReason":   subscriptionSuspendReason,
		"subscriptionType":            subscriptionType,
		"isDineInEnabled":             isDineInEnabled,
		"isTakeawayEnabled":           isTakeawayEnabled,
		"requireTableNumberForDineIn": requireTableNumberForDineIn,
		"dineInLabel":                 nullIfEmptyText(dineInLabel),
		"takeawayLabel":               nullIfEmptyText(takeawayLabel),
		"deliveryLabel":               nullIfEmptyText(deliveryLabel),
		"dineInScheduleStart":         nullIfEmptyText(dineInScheduleStart),
		"dineInScheduleEnd":           nullIfEmptyText(dineInScheduleEnd),
		"takeawayScheduleStart":       nullIfEmptyText(takeawayScheduleStart),
		"takeawayScheduleEnd":         nullIfEmptyText(takeawayScheduleEnd),
		"deliveryScheduleStart":       nullIfEmptyText(deliveryScheduleStart),
		"deliveryScheduleEnd":         nullIfEmptyText(deliveryScheduleEnd),
		"totalTables":                 nullIfEmptyInt32(totalTables),
		"isDeliveryEnabled":           isDeliveryEnabled,
		"enforceDeliveryZones":        enforceDeliveryZones,
		"deliveryMaxDistanceKm":       nullableNumeric(deliveryMaxDistanceKm),
		"deliveryFeeBase":             nullableNumeric(deliveryFeeBase),
		"deliveryFeePerKm":            nullableNumeric(deliveryFeePerKm),
		"deliveryFeeMin":              nullableNumeric(deliveryFeeMin),
		"deliveryFeeMax":              nullableNumeric(deliveryFeeMax),
		"enableTax":                   enableTax,
		"taxPercentage":               nullableNumeric(taxPercentage),
		"enableServiceCharge":         enableServiceCharge,
		"serviceChargePercent":        nullableNumeric(serviceChargePercent),
		"enablePackagingFee":          enablePackagingFee,
		"packagingFeeAmount":          nullableNumeric(packagingFeeAmount),
		"currency":                    currency,
		"timezone":                    timezone,
		"customerVouchersEnabled":     customerVouchersEnabled,
		"latitude":                    nullableNumeric(latitude),
		"longitude":                   nullableNumeric(longitude),
		"isReservationEnabled":        isReservationEnabled,
		"reservationMenuRequired":     reservationMenuRequired,
		"reservationMinItemCount":     reservationMinItemCount,
		"isScheduledOrderEnabled":     isScheduledOrderEnabled,
		"isPerDayModeScheduleEnabled": isPerDayModeScheduleEnabled,
		"openingHours":                openingHours,
		"paymentSettings":             paymentSettings,
		"paymentAccounts":             paymentAccounts,
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
		"message": "Merchant retrieved successfully",
	})
}

func nullIfEmptyText(value pgtype.Text) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func nullIfEmptyInt32(value pgtype.Int4) any {
	if value.Valid {
		return value.Int32
	}
	return nil
}

func nullableNumeric(value pgtype.Numeric) any {
	if value.Valid {
		return utils.NumericToFloat64(value)
	}
	return nil
}
