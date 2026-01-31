package handlers

import (
	"net/http"
	"time"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type merchantStatusOpeningHour struct {
	ID        int64   `json:"id"`
	DayOfWeek int     `json:"dayOfWeek"`
	IsClosed  bool    `json:"isClosed"`
	OpenTime  *string `json:"openTime"`
	CloseTime *string `json:"closeTime"`
}

type merchantStatusModeSchedule struct {
	ID        int64  `json:"id"`
	Mode      string `json:"mode"`
	DayOfWeek int    `json:"dayOfWeek"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	IsActive  bool   `json:"isActive"`
}

type merchantStatusSpecialHour struct {
	ID                int64     `json:"id"`
	Date              time.Time `json:"date"`
	Name              *string   `json:"name"`
	IsClosed          bool      `json:"isClosed"`
	OpenTime          *string   `json:"openTime"`
	CloseTime         *string   `json:"closeTime"`
	IsDineInEnabled   *bool     `json:"isDineInEnabled"`
	IsTakeawayEnabled *bool     `json:"isTakeawayEnabled"`
	IsDeliveryEnabled *bool     `json:"isDeliveryEnabled"`
	DineInStartTime   *string   `json:"dineInStartTime"`
	DineInEndTime     *string   `json:"dineInEndTime"`
	TakeawayStartTime *string   `json:"takeawayStartTime"`
	TakeawayEndTime   *string   `json:"takeawayEndTime"`
	DeliveryStartTime *string   `json:"deliveryStartTime"`
	DeliveryEndTime   *string   `json:"deliveryEndTime"`
}

func (h *Handler) PublicMerchantStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var (
		merchantID                  int64
		isActive                    bool
		isOpen                      bool
		isManualOverride            bool
		timezone                    string
		isPerDayModeScheduleEnabled bool
		isDineInEnabled             bool
		isTakeawayEnabled           bool
		isDeliveryEnabled           bool
		dineInLabel                 pgtype.Text
		takeawayLabel               pgtype.Text
		deliveryLabel               pgtype.Text
		dineInScheduleStart         pgtype.Text
		dineInScheduleEnd           pgtype.Text
		takeawayScheduleStart       pgtype.Text
		takeawayScheduleEnd         pgtype.Text
		deliveryScheduleStart       pgtype.Text
		deliveryScheduleEnd         pgtype.Text
	)

	err := h.DB.QueryRow(ctx, `
		select id, is_active, is_open, is_manual_override, timezone,
		       is_per_day_mode_schedule_enabled,
		       is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
		       dine_in_label, takeaway_label, delivery_label,
		       dine_in_schedule_start, dine_in_schedule_end,
		       takeaway_schedule_start, takeaway_schedule_end,
		       delivery_schedule_start, delivery_schedule_end
		from merchants
		where code = $1
	`, merchantCode).Scan(
		&merchantID,
		&isActive,
		&isOpen,
		&isManualOverride,
		&timezone,
		&isPerDayModeScheduleEnabled,
		&isDineInEnabled,
		&isTakeawayEnabled,
		&isDeliveryEnabled,
		&dineInLabel,
		&takeawayLabel,
		&deliveryLabel,
		&dineInScheduleStart,
		&dineInScheduleEnd,
		&takeawayScheduleStart,
		&takeawayScheduleEnd,
		&deliveryScheduleStart,
		&deliveryScheduleEnd,
	)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	if !isActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_DISABLED", "Merchant is currently disabled")
		return
	}

	openingHours := make([]merchantStatusOpeningHour, 0)
	hoursRows, err := h.DB.Query(ctx, `
		select id, day_of_week, is_closed, open_time, close_time
		from merchant_opening_hours
		where merchant_id = $1
		order by day_of_week asc
	`, merchantID)
	if err == nil {
		defer hoursRows.Close()
		for hoursRows.Next() {
			var hrow merchantStatusOpeningHour
			var openTime pgtype.Text
			var closeTime pgtype.Text
			if err := hoursRows.Scan(&hrow.ID, &hrow.DayOfWeek, &hrow.IsClosed, &openTime, &closeTime); err == nil {
				if openTime.Valid {
					hrow.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					hrow.CloseTime = &closeTime.String
				}
				openingHours = append(openingHours, hrow)
			}
		}
	}

	modeSchedules := make([]merchantStatusModeSchedule, 0)
	modeRows, err := h.DB.Query(ctx, `
		select id, mode, day_of_week, start_time, end_time, is_active
		from merchant_mode_schedules
		where merchant_id = $1
		order by mode asc, day_of_week asc
	`, merchantID)
	if err == nil {
		defer modeRows.Close()
		for modeRows.Next() {
			var m merchantStatusModeSchedule
			if err := modeRows.Scan(&m.ID, &m.Mode, &m.DayOfWeek, &m.StartTime, &m.EndTime, &m.IsActive); err == nil {
				modeSchedules = append(modeSchedules, m)
			}
		}
	}

	// Today's special hour
	var todaySpecial *merchantStatusSpecialHour
	dateISO := currentDateISOInTZ(timezone)
	if dateISO != "" {
		if dateValue, err := time.Parse("2006-01-02", dateISO); err == nil {
			var special merchantStatusSpecialHour
			var name pgtype.Text
			var openTime pgtype.Text
			var closeTime pgtype.Text
			var isDineIn pgtype.Bool
			var isTakeaway pgtype.Bool
			var isDelivery pgtype.Bool
			var dineInStart pgtype.Text
			var dineInEnd pgtype.Text
			var takeawayStart pgtype.Text
			var takeawayEnd pgtype.Text
			var deliveryStart pgtype.Text
			var deliveryEnd pgtype.Text
			if err := h.DB.QueryRow(ctx, `
				select id, date, name, is_closed, open_time, close_time,
				       is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
				       dine_in_start_time, dine_in_end_time,
				       takeaway_start_time, takeaway_end_time,
				       delivery_start_time, delivery_end_time
				from merchant_special_hours
				where merchant_id = $1 and date = $2
			`, merchantID, dateValue).Scan(
				&special.ID,
				&special.Date,
				&name,
				&special.IsClosed,
				&openTime,
				&closeTime,
				&isDineIn,
				&isTakeaway,
				&isDelivery,
				&dineInStart,
				&dineInEnd,
				&takeawayStart,
				&takeawayEnd,
				&deliveryStart,
				&deliveryEnd,
			); err == nil {
				if name.Valid {
					special.Name = &name.String
				}
				if openTime.Valid {
					special.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					special.CloseTime = &closeTime.String
				}
				if isDineIn.Valid {
					v := isDineIn.Bool
					special.IsDineInEnabled = &v
				}
				if isTakeaway.Valid {
					v := isTakeaway.Bool
					special.IsTakeawayEnabled = &v
				}
				if isDelivery.Valid {
					v := isDelivery.Bool
					special.IsDeliveryEnabled = &v
				}
				if dineInStart.Valid {
					special.DineInStartTime = &dineInStart.String
				}
				if dineInEnd.Valid {
					special.DineInEndTime = &dineInEnd.String
				}
				if takeawayStart.Valid {
					special.TakeawayStartTime = &takeawayStart.String
				}
				if takeawayEnd.Valid {
					special.TakeawayEndTime = &takeawayEnd.String
				}
				if deliveryStart.Valid {
					special.DeliveryStartTime = &deliveryStart.String
				}
				if deliveryEnd.Valid {
					special.DeliveryEndTime = &deliveryEnd.String
				}
				todaySpecial = &special
			}
		}
	}

	// Subscription status
	subscriptionState, err := h.fetchSubscriptionState(ctx, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve merchant status")
		return
	}
	subscriptionStatus := subscriptionState.Status

	payload := map[string]any{
		"isOpen":                      isOpen,
		"isManualOverride":            isManualOverride,
		"timezone":                    timezone,
		"isPerDayModeScheduleEnabled": isPerDayModeScheduleEnabled,
		"isDineInEnabled":             isDineInEnabled,
		"isTakeawayEnabled":           isTakeawayEnabled,
		"isDeliveryEnabled":           isDeliveryEnabled,
		"dineInLabel":                 nullIfEmptyText(dineInLabel),
		"takeawayLabel":               nullIfEmptyText(takeawayLabel),
		"deliveryLabel":               nullIfEmptyText(deliveryLabel),
		"dineInScheduleStart":         nullIfEmptyText(dineInScheduleStart),
		"dineInScheduleEnd":           nullIfEmptyText(dineInScheduleEnd),
		"takeawayScheduleStart":       nullIfEmptyText(takeawayScheduleStart),
		"takeawayScheduleEnd":         nullIfEmptyText(takeawayScheduleEnd),
		"deliveryScheduleStart":       nullIfEmptyText(deliveryScheduleStart),
		"deliveryScheduleEnd":         nullIfEmptyText(deliveryScheduleEnd),
		"openingHours":                openingHours,
		"modeSchedules":               modeSchedules,
		"todaySpecialHour":            todaySpecial,
		"subscriptionStatus":          subscriptionStatus,
		"serverTime":                  time.Now().UTC().Format(time.RFC3339),
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
	})
}

func currentDateISOInTZ(timezone string) string {
	loc, err := time.LoadLocation(timezone)
	if err != nil || timezone == "" {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	return now.Format("2006-01-02")
}
