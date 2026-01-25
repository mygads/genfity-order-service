package handlers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type availableTimesMerchant struct {
	isOpen                      bool
	isManualOverride            bool
	timezone                    string
	isPerDayModeScheduleEnabled bool
	isDineInEnabled             bool
	isTakeawayEnabled           bool
	isDeliveryEnabled           bool
	dineInScheduleStart         *string
	dineInScheduleEnd           *string
	takeawayScheduleStart       *string
	takeawayScheduleEnd         *string
	deliveryScheduleStart       *string
	deliveryScheduleEnd         *string
	openingHours                []merchantStatusOpeningHour
	modeSchedules               []merchantStatusModeSchedule
	todaySpecialHour            *merchantStatusSpecialHour
}

func (h *Handler) PublicMerchantAvailableTimes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	modeParam := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("mode")))
	if modeParam != "DINE_IN" && modeParam != "TAKEAWAY" && modeParam != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Query param \"mode\" is required (DINE_IN, TAKEAWAY, DELIVERY)")
		return
	}

	intervalRaw := r.URL.Query().Get("intervalMinutes")
	interval := 15
	if intervalRaw != "" {
		parsed, err := strconv.Atoi(intervalRaw)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "intervalMinutes must be one of: 5, 10, 15, 20, 30, 60")
			return
		}
		interval = parsed
	}

	switch interval {
	case 5, 10, 15, 20, 30, 60:
	default:
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "intervalMinutes must be one of: 5, 10, 15, 20, 30, 60")
		return
	}

	includePast := strings.EqualFold(r.URL.Query().Get("includePast"), "true")

	var (
		merchantID                  int64
		isActive                    bool
		isOpen                      bool
		isManualOverride            bool
		isScheduledOrderEnabled     bool
		isPerDayModeScheduleEnabled bool
		isDineInEnabled             bool
		isTakeawayEnabled           bool
		isDeliveryEnabled           bool
		timezone                    string
		dineInScheduleStart         pgtype.Text
		dineInScheduleEnd           pgtype.Text
		takeawayScheduleStart       pgtype.Text
		takeawayScheduleEnd         pgtype.Text
		deliveryScheduleStart       pgtype.Text
		deliveryScheduleEnd         pgtype.Text
		latitude                    pgtype.Numeric
		longitude                   pgtype.Numeric
	)

	err := h.DB.QueryRow(ctx, `
		select id, is_active, is_open, is_manual_override, is_scheduled_order_enabled,
		       is_per_day_mode_schedule_enabled,
		       is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
		       timezone,
		       dine_in_schedule_start, dine_in_schedule_end,
		       takeaway_schedule_start, takeaway_schedule_end,
		       delivery_schedule_start, delivery_schedule_end,
		       latitude, longitude
		from merchants
		where code = $1
	`, merchantCode).Scan(
		&merchantID,
		&isActive,
		&isOpen,
		&isManualOverride,
		&isScheduledOrderEnabled,
		&isPerDayModeScheduleEnabled,
		&isDineInEnabled,
		&isTakeawayEnabled,
		&isDeliveryEnabled,
		&timezone,
		&dineInScheduleStart,
		&dineInScheduleEnd,
		&takeawayScheduleStart,
		&takeawayScheduleEnd,
		&deliveryScheduleStart,
		&deliveryScheduleEnd,
		&latitude,
		&longitude,
	)
	if err != nil || !isActive {
		response.Error(w, http.StatusBadRequest, "MERCHANT_INACTIVE", "Merchant is currently not accepting orders")
		return
	}

	if !isScheduledOrderEnabled {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"timezone":        timezoneOrDefault(timezone),
				"now":             nil,
				"mode":            modeParam,
				"intervalMinutes": interval,
				"slots":           []string{},
				"disabledReason":  "Scheduled orders are not enabled for this merchant.",
			},
			"statusCode": 200,
		})
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

	dateISO := currentDateISOInTZ(timezone)
	var todaySpecial *merchantStatusSpecialHour
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

	merchant := availableTimesMerchant{
		isOpen:                      isOpen,
		isManualOverride:            isManualOverride,
		timezone:                    timezoneOrDefault(timezone),
		isPerDayModeScheduleEnabled: isPerDayModeScheduleEnabled,
		isDineInEnabled:             isDineInEnabled,
		isTakeawayEnabled:           isTakeawayEnabled,
		isDeliveryEnabled:           isDeliveryEnabled,
		openingHours:                openingHours,
		modeSchedules:               modeSchedules,
		todaySpecialHour:            todaySpecial,
		dineInScheduleStart:         textPtr(dineInScheduleStart),
		dineInScheduleEnd:           textPtr(dineInScheduleEnd),
		takeawayScheduleStart:       textPtr(takeawayScheduleStart),
		takeawayScheduleEnd:         textPtr(takeawayScheduleEnd),
		deliveryScheduleStart:       textPtr(deliveryScheduleStart),
		deliveryScheduleEnd:         textPtr(deliveryScheduleEnd),
	}

	nowHHMM := currentTimeHHMMInTZ(merchant.timezone)
	allSlots := generateSlots(interval)
	valid := make([]string, 0)
	for _, hhmm := range allSlots {
		if !includePast && nowHHMM != "" && hhmm < nowHHMM {
			continue
		}

		if !isStoreOpenWithSpecialHoursAtTime(merchant, hhmm) {
			continue
		}
		if !isModeAvailableWithSchedulesAtTime(modeParam, merchant, hhmm) {
			continue
		}
		if modeParam == "DELIVERY" {
			if !isDeliveryEnabled {
				continue
			}
			if !latitude.Valid || !longitude.Valid {
				continue
			}
		}
		valid = append(valid, hhmm)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"timezone":        merchant.timezone,
			"date":            dateISO,
			"now":             nowHHMM,
			"mode":            modeParam,
			"intervalMinutes": interval,
			"slots":           valid,
		},
		"statusCode": 200,
	})
}

func timezoneOrDefault(timezone string) string {
	if timezone == "" {
		return "Australia/Sydney"
	}
	return timezone
}

func textPtr(value pgtype.Text) *string {
	if value.Valid {
		return &value.String
	}
	return nil
}

func currentTimeHHMMInTZ(timezone string) string {
	loc, err := time.LoadLocation(timezone)
	if err != nil || timezone == "" {
		loc = time.UTC
	}
	return time.Now().In(loc).Format("15:04")
}

func dayOfWeekInTZ(timezone string) int {
	loc, err := time.LoadLocation(timezone)
	if err != nil || timezone == "" {
		loc = time.UTC
	}
	return int(time.Now().In(loc).Weekday())
}

func isStoreOpenByScheduleAtTime(merchant availableTimesMerchant, timeHHMM string) bool {
	if len(merchant.openingHours) == 0 {
		return true
	}
	day := dayOfWeekInTZ(merchant.timezone)
	var today *merchantStatusOpeningHour
	for i := range merchant.openingHours {
		if merchant.openingHours[i].DayOfWeek == day {
			today = &merchant.openingHours[i]
			break
		}
	}
	if today == nil || today.IsClosed {
		return false
	}
	if today.OpenTime != nil && today.CloseTime != nil {
		return timeHHMM >= *today.OpenTime && timeHHMM <= *today.CloseTime
	}
	return true
}

func isStoreOpenWithSpecialHoursAtTime(merchant availableTimesMerchant, timeHHMM string) bool {
	if merchant.isManualOverride {
		return merchant.isOpen
	}

	if merchant.todaySpecialHour != nil {
		special := merchant.todaySpecialHour
		if special.IsClosed {
			return false
		}
		if special.OpenTime != nil && special.CloseTime != nil {
			return timeHHMM >= *special.OpenTime && timeHHMM <= *special.CloseTime
		}
	}

	return isStoreOpenByScheduleAtTime(merchant, timeHHMM)
}

func isModeAvailableWithSchedulesAtTime(modeType string, merchant availableTimesMerchant, timeHHMM string) bool {
	if merchant.isManualOverride && !merchant.isOpen {
		return false
	}

	isEnabled := false
	switch modeType {
	case "DINE_IN":
		isEnabled = merchant.isDineInEnabled
	case "TAKEAWAY":
		isEnabled = merchant.isTakeawayEnabled
	case "DELIVERY":
		isEnabled = merchant.isDeliveryEnabled
	}
	if !isEnabled {
		return false
	}

	if merchant.isManualOverride && merchant.isOpen {
		return true
	}

	if merchant.todaySpecialHour != nil {
		special := merchant.todaySpecialHour
		var modeEnabled *bool
		var start *string
		var end *string
		switch modeType {
		case "DINE_IN":
			modeEnabled = special.IsDineInEnabled
			start = special.DineInStartTime
			end = special.DineInEndTime
		case "TAKEAWAY":
			modeEnabled = special.IsTakeawayEnabled
			start = special.TakeawayStartTime
			end = special.TakeawayEndTime
		case "DELIVERY":
			modeEnabled = special.IsDeliveryEnabled
			start = special.DeliveryStartTime
			end = special.DeliveryEndTime
		}
		if modeEnabled != nil && !*modeEnabled {
			return false
		}
		if start != nil && end != nil {
			return timeHHMM >= *start && timeHHMM <= *end
		}
	}

	if merchant.isPerDayModeScheduleEnabled && len(merchant.modeSchedules) > 0 {
		day := dayOfWeekInTZ(merchant.timezone)
		for _, schedule := range merchant.modeSchedules {
			if schedule.Mode == modeType && schedule.DayOfWeek == day {
				if !schedule.IsActive {
					return false
				}
				return timeHHMM >= schedule.StartTime && timeHHMM <= schedule.EndTime
			}
		}
	}

	var globalStart *string
	var globalEnd *string
	switch modeType {
	case "DINE_IN":
		globalStart = merchant.dineInScheduleStart
		globalEnd = merchant.dineInScheduleEnd
	case "TAKEAWAY":
		globalStart = merchant.takeawayScheduleStart
		globalEnd = merchant.takeawayScheduleEnd
	case "DELIVERY":
		globalStart = merchant.deliveryScheduleStart
		globalEnd = merchant.deliveryScheduleEnd
	}
	if globalStart != nil && globalEnd != nil {
		return timeHHMM >= *globalStart && timeHHMM <= *globalEnd
	}

	return true
}

func generateSlots(intervalMinutes int) []string {
	if intervalMinutes <= 0 {
		intervalMinutes = 15
	}
	slots := make([]string, 0)
	for minutes := 0; minutes < 24*60; minutes += intervalMinutes {
		h := minutes / 60
		m := minutes % 60
		slots = append(slots, twoDigit(h)+":"+twoDigit(m))
	}
	return slots
}

func twoDigit(value int) string {
	if value < 10 {
		return "0" + strconv.Itoa(value)
	}
	return strconv.Itoa(value)
}
