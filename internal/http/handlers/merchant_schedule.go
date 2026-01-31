package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type openingHourInput struct {
	DayOfWeek int    `json:"dayOfWeek"`
	OpenTime  string `json:"openTime"`
	CloseTime string `json:"closeTime"`
	IsClosed  bool   `json:"isClosed"`
}

type modeScheduleInput struct {
	Mode      string `json:"mode"`
	DayOfWeek int    `json:"dayOfWeek"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	IsActive  *bool  `json:"isActive"`
}

type specialHourPayload struct {
	Date              string  `json:"date"`
	Name              *string `json:"name"`
	IsClosed          *bool   `json:"isClosed"`
	OpenTime          *string `json:"openTime"`
	CloseTime         *string `json:"closeTime"`
	IsDineInEnabled   *bool   `json:"isDineInEnabled"`
	IsTakeawayEnabled *bool   `json:"isTakeawayEnabled"`
	IsDeliveryEnabled *bool   `json:"isDeliveryEnabled"`
	DineInStartTime   *string `json:"dineInStartTime"`
	DineInEndTime     *string `json:"dineInEndTime"`
	TakeawayStartTime *string `json:"takeawayStartTime"`
	TakeawayEndTime   *string `json:"takeawayEndTime"`
	DeliveryStartTime *string `json:"deliveryStartTime"`
	DeliveryEndTime   *string `json:"deliveryEndTime"`
}

var timeHHMMPatternSchedule = regexp.MustCompile(`^(?:[01]\d|2[0-3]):[0-5]\d$`)

func (h *Handler) MerchantOpeningHoursPut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var merchantExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from merchants where id = $1)", *authCtx.MerchantID).Scan(&merchantExists); err != nil || !merchantExists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	var payload struct {
		OpeningHours []openingHourInput `json:"openingHours"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if payload.OpeningHours == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid opening hours data")
		return
	}

	for _, hour := range payload.OpeningHours {
		if hour.DayOfWeek < 0 || hour.DayOfWeek > 6 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid day of week")
			return
		}
		if !hour.IsClosed {
			if strings.TrimSpace(hour.OpenTime) == "" || strings.TrimSpace(hour.CloseTime) == "" {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Open time and close time are required when not closed")
				return
			}
			if !isValidTimeHHMM(hour.OpenTime) || !isValidTimeHHMM(hour.CloseTime) {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid time format. Expected HH:MM")
				return
			}
		}
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update opening hours")
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "delete from merchant_opening_hours where merchant_id = $1", *authCtx.MerchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update opening hours")
		return
	}

	for _, hour := range payload.OpeningHours {
		openTime := hour.OpenTime
		closeTime := hour.CloseTime
		if hour.IsClosed {
			openTime = "00:00"
			closeTime = "00:00"
		}
		if _, err := tx.Exec(ctx, `
			insert into merchant_opening_hours (merchant_id, day_of_week, open_time, close_time, is_closed, updated_at)
			values ($1, $2, $3, $4, $5, now())
		`, *authCtx.MerchantID, hour.DayOfWeek, openTime, closeTime, hour.IsClosed); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update opening hours")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update opening hours")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"openingHours": payload.OpeningHours,
		},
		"message":    "Opening hours updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantSpecialHoursGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	fromDate := strings.TrimSpace(r.URL.Query().Get("from"))
	toDate := strings.TrimSpace(r.URL.Query().Get("to"))

	args := []any{*authCtx.MerchantID}
	conditions := []string{"merchant_id = $1"}

	if fromDate != "" {
		parsed, err := parseScheduleDateInput(fromDate)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid from date")
			return
		}
		args = append(args, normalizeDate(parsed))
		conditions = append(conditions, fmt.Sprintf("date >= $%d", len(args)))
	}

	if toDate != "" {
		parsed, err := parseScheduleDateInput(toDate)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid to date")
			return
		}
		args = append(args, normalizeDate(parsed))
		conditions = append(conditions, fmt.Sprintf("date <= $%d", len(args)))
	}

	query := fmt.Sprintf(`
        select id, merchant_id, date, name, is_closed, open_time, close_time,
               is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
               dine_in_start_time, dine_in_end_time,
               takeaway_start_time, takeaway_end_time,
               delivery_start_time, delivery_end_time,
               created_at, updated_at
        from merchant_special_hours
        where %s
        order by date asc
    `, strings.Join(conditions, " and "))

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch special hours")
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id            int64
			merchantID    int64
			date          time.Time
			name          pgtype.Text
			isClosed      bool
			openTime      pgtype.Text
			closeTime     pgtype.Text
			isDineIn      pgtype.Bool
			isTakeaway    pgtype.Bool
			isDelivery    pgtype.Bool
			dineInStart   pgtype.Text
			dineInEnd     pgtype.Text
			takeawayStart pgtype.Text
			takeawayEnd   pgtype.Text
			deliveryStart pgtype.Text
			deliveryEnd   pgtype.Text
			createdAt     time.Time
			updatedAt     time.Time
		)
		if err := rows.Scan(
			&id,
			&merchantID,
			&date,
			&name,
			&isClosed,
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
			&createdAt,
			&updatedAt,
		); err == nil {
			items = append(items, buildSpecialHourPayload(
				id,
				merchantID,
				date,
				name,
				isClosed,
				openTime,
				closeTime,
				isDineIn,
				isTakeaway,
				isDelivery,
				dineInStart,
				dineInEnd,
				takeawayStart,
				takeawayEnd,
				deliveryStart,
				deliveryEnd,
				createdAt,
				updatedAt,
			))
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    items,
	})
}

func (h *Handler) MerchantSpecialHoursPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload specialHourPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(payload.Date) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Date is required")
		return
	}

	parsedDate, err := parseScheduleDateInput(payload.Date)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid date")
		return
	}
	normalizedDate := normalizeDate(parsedDate)

	isClosed := false
	if payload.IsClosed != nil {
		isClosed = *payload.IsClosed
	}

	if err := validateSpecialHourTimes(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	var (
		id            int64
		merchantID    int64
		date          time.Time
		name          pgtype.Text
		openTime      pgtype.Text
		closeTime     pgtype.Text
		isDineIn      pgtype.Bool
		isTakeaway    pgtype.Bool
		isDelivery    pgtype.Bool
		dineInStart   pgtype.Text
		dineInEnd     pgtype.Text
		takeawayStart pgtype.Text
		takeawayEnd   pgtype.Text
		deliveryStart pgtype.Text
		deliveryEnd   pgtype.Text
		createdAt     time.Time
		updatedAt     time.Time
	)

	if err := h.DB.QueryRow(ctx, `
		insert into merchant_special_hours (
			merchant_id, date, name, is_closed, open_time, close_time,
			is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
			dine_in_start_time, dine_in_end_time,
			takeaway_start_time, takeaway_end_time,
			delivery_start_time, delivery_end_time, updated_at
		)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now())
        on conflict (merchant_id, date)
        do update set
            name = excluded.name,
            is_closed = excluded.is_closed,
            open_time = excluded.open_time,
            close_time = excluded.close_time,
            is_dine_in_enabled = excluded.is_dine_in_enabled,
            is_takeaway_enabled = excluded.is_takeaway_enabled,
            is_delivery_enabled = excluded.is_delivery_enabled,
            dine_in_start_time = excluded.dine_in_start_time,
            dine_in_end_time = excluded.dine_in_end_time,
            takeaway_start_time = excluded.takeaway_start_time,
            takeaway_end_time = excluded.takeaway_end_time,
            delivery_start_time = excluded.delivery_start_time,
            delivery_end_time = excluded.delivery_end_time,
            updated_at = now()
        returning id, merchant_id, date, name, is_closed, open_time, close_time,
                  is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
                  dine_in_start_time, dine_in_end_time,
                  takeaway_start_time, takeaway_end_time,
                  delivery_start_time, delivery_end_time,
                  created_at, updated_at
    `,
		*authCtx.MerchantID,
		normalizedDate,
		payload.Name,
		isClosed,
		payload.OpenTime,
		payload.CloseTime,
		payload.IsDineInEnabled,
		payload.IsTakeawayEnabled,
		payload.IsDeliveryEnabled,
		payload.DineInStartTime,
		payload.DineInEndTime,
		payload.TakeawayStartTime,
		payload.TakeawayEndTime,
		payload.DeliveryStartTime,
		payload.DeliveryEndTime,
	).Scan(
		&id,
		&merchantID,
		&date,
		&name,
		&isClosed,
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
		&createdAt,
		&updatedAt,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save special hours")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": buildSpecialHourPayload(
			id,
			merchantID,
			date,
			name,
			isClosed,
			openTime,
			closeTime,
			isDineIn,
			isTakeaway,
			isDelivery,
			dineInStart,
			dineInEnd,
			takeawayStart,
			takeawayEnd,
			deliveryStart,
			deliveryEnd,
			createdAt,
			updatedAt,
		),
		"message": "Special hours saved successfully",
	})
}

func (h *Handler) MerchantSpecialHoursDetailGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	specialHourID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special hour id")
		return
	}

	payload, err := h.fetchSpecialHour(ctx, *authCtx.MerchantID, specialHourID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special hour not found")
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch special hour")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
	})
}

func (h *Handler) MerchantSpecialHoursDetailPut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	specialHourID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special hour id")
		return
	}

	existing, err := h.fetchSpecialHour(ctx, *authCtx.MerchantID, specialHourID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special hour not found")
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special hour")
		return
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	updates := make(map[string]any)

	if value, ok := body["name"]; ok {
		updates["name"] = normalizeOptionalString(value)
	}
	if value, ok := body["isClosed"]; ok {
		boolVal, ok := value.(bool)
		if !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isClosed must be a boolean")
			return
		}
		updates["is_closed"] = boolVal
	}
	if value, ok := body["openTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["open_time"] = normalized
	}
	if value, ok := body["closeTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["close_time"] = normalized
	}
	if value, ok := body["isDineInEnabled"]; ok {
		parsed, ok := normalizeOptionalBool(value)
		if !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isDineInEnabled must be a boolean")
			return
		}
		updates["is_dine_in_enabled"] = parsed
	}
	if value, ok := body["isTakeawayEnabled"]; ok {
		parsed, ok := normalizeOptionalBool(value)
		if !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isTakeawayEnabled must be a boolean")
			return
		}
		updates["is_takeaway_enabled"] = parsed
	}
	if value, ok := body["isDeliveryEnabled"]; ok {
		parsed, ok := normalizeOptionalBool(value)
		if !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isDeliveryEnabled must be a boolean")
			return
		}
		updates["is_delivery_enabled"] = parsed
	}
	if value, ok := body["dineInStartTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["dine_in_start_time"] = normalized
	}
	if value, ok := body["dineInEndTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["dine_in_end_time"] = normalized
	}
	if value, ok := body["takeawayStartTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["takeaway_start_time"] = normalized
	}
	if value, ok := body["takeawayEndTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["takeaway_end_time"] = normalized
	}
	if value, ok := body["deliveryStartTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["delivery_start_time"] = normalized
	}
	if value, ok := body["deliveryEndTime"]; ok {
		normalized, err := normalizeAndValidateTime(value)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
		updates["delivery_end_time"] = normalized
	}

	if len(updates) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    existing,
			"message": "Special hour updated successfully",
		})
		return
	}

	columns := make([]string, 0, len(updates)+1)
	args := make([]any, 0, len(updates)+2)
	idx := 1
	keys := make([]string, 0, len(updates))
	for key := range updates {
		keys = append(keys, key)
	}
	sortStrings(keys)
	for _, key := range keys {
		columns = append(columns, fmt.Sprintf("%s = $%d", key, idx))
		args = append(args, updates[key])
		idx++
	}
	columns = append(columns, "updated_at = now()")
	args = append(args, specialHourID)
	args = append(args, *authCtx.MerchantID)

	query := "update merchant_special_hours set " + strings.Join(columns, ", ") + fmt.Sprintf(" where id = $%d and merchant_id = $%d", idx, idx+1)

	var (
		id            int64
		merchantID    int64
		date          time.Time
		name          pgtype.Text
		isClosed      bool
		openTime      pgtype.Text
		closeTime     pgtype.Text
		isDineIn      pgtype.Bool
		isTakeaway    pgtype.Bool
		isDelivery    pgtype.Bool
		dineInStart   pgtype.Text
		dineInEnd     pgtype.Text
		takeawayStart pgtype.Text
		takeawayEnd   pgtype.Text
		deliveryStart pgtype.Text
		deliveryEnd   pgtype.Text
		createdAt     time.Time
		updatedAt     time.Time
	)
	if err := h.DB.QueryRow(ctx, query+" returning id, merchant_id, date, name, is_closed, open_time, close_time, is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled, dine_in_start_time, dine_in_end_time, takeaway_start_time, takeaway_end_time, delivery_start_time, delivery_end_time, created_at, updated_at", args...).Scan(
		&id,
		&merchantID,
		&date,
		&name,
		&isClosed,
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
		&createdAt,
		&updatedAt,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special hour")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": buildSpecialHourPayload(
			id,
			merchantID,
			date,
			name,
			isClosed,
			openTime,
			closeTime,
			isDineIn,
			isTakeaway,
			isDelivery,
			dineInStart,
			dineInEnd,
			takeawayStart,
			takeawayEnd,
			deliveryStart,
			deliveryEnd,
			createdAt,
			updatedAt,
		),
		"message": "Special hour updated successfully",
	})
}

func (h *Handler) MerchantSpecialHoursDetailDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	specialHourID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special hour id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from merchant_special_hours where id = $1 and merchant_id = $2)", specialHourID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special hour not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from merchant_special_hours where id = $1", specialHourID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete special hour")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Special hour deleted successfully",
	})
}

func (h *Handler) MerchantModeSchedulesGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var enabled bool
	if err := h.DB.QueryRow(ctx, "select is_per_day_mode_schedule_enabled from merchants where id = $1", *authCtx.MerchantID).Scan(&enabled); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch mode schedules")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select id, merchant_id, mode, day_of_week, start_time, end_time, is_active, created_at, updated_at
        from merchant_mode_schedules
        where merchant_id = $1
        order by mode asc, day_of_week asc
    `, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch mode schedules")
		return
	}
	defer rows.Close()

	schedules := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id         int64
			merchantID int64
			mode       string
			dayOfWeek  int
			startTime  string
			endTime    string
			isActive   bool
			createdAt  time.Time
			updatedAt  time.Time
		)
		if err := rows.Scan(&id, &merchantID, &mode, &dayOfWeek, &startTime, &endTime, &isActive, &createdAt, &updatedAt); err == nil {
			schedules = append(schedules, map[string]any{
				"id":         fmt.Sprint(id),
				"merchantId": fmt.Sprint(merchantID),
				"mode":       mode,
				"dayOfWeek":  dayOfWeek,
				"startTime":  startTime,
				"endTime":    endTime,
				"isActive":   isActive,
				"createdAt":  createdAt,
				"updatedAt":  updatedAt,
			})
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"enabled":   enabled,
			"schedules": schedules,
		},
	})
}

func (h *Handler) MerchantModeSchedulesPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload struct {
		Schedules []modeScheduleInput `json:"schedules"`
		Enabled   *bool               `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Enabled != nil && (*payload.Enabled != true && *payload.Enabled != false) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Enabled must be a boolean")
		return
	}

	if payload.Schedules == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Schedules must be an array")
		return
	}

	for _, schedule := range payload.Schedules {
		if schedule.Mode == "" || (schedule.Mode != "DINE_IN" && schedule.Mode != "TAKEAWAY" && schedule.Mode != "DELIVERY") {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid mode")
			return
		}
		if schedule.DayOfWeek < 0 || schedule.DayOfWeek > 6 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid day of week (0-6)")
			return
		}
		if strings.TrimSpace(schedule.StartTime) == "" || strings.TrimSpace(schedule.EndTime) == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Start and end time required")
			return
		}
		if !isValidTimeHHMM(schedule.StartTime) || !isValidTimeHHMM(schedule.EndTime) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid time format. Expected HH:MM")
			return
		}
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save mode schedules")
		return
	}
	defer tx.Rollback(ctx)

	var enabledValue any = nil
	if payload.Enabled != nil {
		var updated bool
		if err := tx.QueryRow(ctx, "update merchants set is_per_day_mode_schedule_enabled = $1 where id = $2 returning is_per_day_mode_schedule_enabled", *payload.Enabled, *authCtx.MerchantID).Scan(&updated); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save mode schedules")
			return
		}
		enabledValue = updated
	}

	schedules := make([]map[string]any, 0)
	for _, schedule := range payload.Schedules {
		isActive := true
		if schedule.IsActive != nil {
			isActive = *schedule.IsActive
		}
		var (
			id         int64
			merchantID int64
			mode       string
			dayOfWeek  int
			startTime  string
			endTime    string
			active     bool
			createdAt  time.Time
			updatedAt  time.Time
		)
		if err := tx.QueryRow(ctx, `
		insert into merchant_mode_schedules (merchant_id, mode, day_of_week, start_time, end_time, is_active, updated_at)
		values ($1, $2, $3, $4, $5, $6, now())
            on conflict (merchant_id, mode, day_of_week)
            do update set start_time = excluded.start_time, end_time = excluded.end_time, is_active = excluded.is_active, updated_at = now()
            returning id, merchant_id, mode, day_of_week, start_time, end_time, is_active, created_at, updated_at
        `, *authCtx.MerchantID, schedule.Mode, schedule.DayOfWeek, schedule.StartTime, schedule.EndTime, isActive).Scan(
			&id,
			&merchantID,
			&mode,
			&dayOfWeek,
			&startTime,
			&endTime,
			&active,
			&createdAt,
			&updatedAt,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save mode schedules")
			return
		}
		schedules = append(schedules, map[string]any{
			"id":         fmt.Sprint(id),
			"merchantId": fmt.Sprint(merchantID),
			"mode":       mode,
			"dayOfWeek":  dayOfWeek,
			"startTime":  startTime,
			"endTime":    endTime,
			"isActive":   active,
			"createdAt":  createdAt,
			"updatedAt":  updatedAt,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save mode schedules")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"enabled":   enabledValue,
			"schedules": schedules,
		},
		"message": "Mode schedules updated successfully",
	})
}

func (h *Handler) MerchantModeSchedulesDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	mode := strings.TrimSpace(r.URL.Query().Get("mode"))
	dayOfWeekRaw := strings.TrimSpace(r.URL.Query().Get("dayOfWeek"))
	disableIfEmpty := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("disableIfEmpty")), "true")

	if mode == "" || dayOfWeekRaw == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Mode and dayOfWeek required")
		return
	}

	dayOfWeek, err := strconv.Atoi(dayOfWeekRaw)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Mode and dayOfWeek required")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete mode schedule")
		return
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
        delete from merchant_mode_schedules
        where merchant_id = $1 and mode = $2 and day_of_week = $3
    `, *authCtx.MerchantID, mode, dayOfWeek)
	if err != nil || result.RowsAffected() == 0 {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete mode schedule")
		return
	}

	disabled := false
	if disableIfEmpty {
		var remaining int
		if err := tx.QueryRow(ctx, "select count(*) from merchant_mode_schedules where merchant_id = $1", *authCtx.MerchantID).Scan(&remaining); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete mode schedule")
			return
		}
		if remaining == 0 {
			if _, err := tx.Exec(ctx, "update merchants set is_per_day_mode_schedule_enabled = false where id = $1", *authCtx.MerchantID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete mode schedule")
				return
			}
			disabled = true
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete mode schedule")
		return
	}

	message := "Mode schedule deleted successfully"
	if disabled {
		message = "Mode schedule deleted successfully and per-day scheduling disabled"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"disabled": disabled,
		},
		"message": message,
	})
}

func (h *Handler) MerchantToggleOpen(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	var payload struct {
		IsOpen           *bool `json:"isOpen"`
		IsManualOverride *bool `json:"isManualOverride"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.IsOpen != nil && (*payload.IsOpen != true && *payload.IsOpen != false) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isOpen must be a boolean value")
		return
	}
	if payload.IsManualOverride != nil && (*payload.IsManualOverride != true && *payload.IsManualOverride != false) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isManualOverride must be a boolean value")
		return
	}

	updateColumns := []string{"updated_at = now()"}
	args := make([]any, 0)
	idx := 1

	if payload.IsManualOverride != nil && !*payload.IsManualOverride {
		updateColumns = append(updateColumns, fmt.Sprintf("is_manual_override = $%d", idx))
		args = append(args, false)
		idx++
		updateColumns = append(updateColumns, fmt.Sprintf("is_open = $%d", idx))
		args = append(args, true)
		idx++
	} else if payload.IsOpen != nil {
		updateColumns = append(updateColumns, fmt.Sprintf("is_open = $%d", idx))
		args = append(args, *payload.IsOpen)
		idx++
		updateColumns = append(updateColumns, fmt.Sprintf("is_manual_override = $%d", idx))
		args = append(args, true)
		idx++
	}

	args = append(args, *authCtx.MerchantID)
	query := "update merchants set " + strings.Join(updateColumns, ", ") + fmt.Sprintf(" where id = $%d", idx)
	if _, err := h.DB.Exec(ctx, query, args...); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle store open status")
		return
	}

	var (
		merchantID       int64
		merchantCode     string
		isOpen           bool
		isManualOverride bool
	)
	if err := h.DB.QueryRow(ctx, "select id, code, is_open, is_manual_override from merchants where id = $1", *authCtx.MerchantID).Scan(&merchantID, &merchantCode, &isOpen, &isManualOverride); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle store open status")
		return
	}

	openingHours := h.fetchOpeningHoursSimple(ctx, *authCtx.MerchantID)
	merchant := map[string]any{
		"id":               fmt.Sprint(merchantID),
		"code":             merchantCode,
		"isOpen":           isOpen,
		"isManualOverride": isManualOverride,
		"openingHours":     openingHours,
	}

	message := "Store manually closed"
	if payload.IsManualOverride != nil && !*payload.IsManualOverride {
		message = "Store switched to auto mode (following schedule)"
	} else if payload.IsOpen != nil && *payload.IsOpen {
		message = "Store manually opened"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       merchant,
		"message":    message,
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) fetchSpecialHour(ctx context.Context, merchantID int64, specialHourID int64) (map[string]any, error) {
	var (
		id              int64
		merchantIDValue int64
		date            time.Time
		name            pgtype.Text
		isClosed        bool
		openTime        pgtype.Text
		closeTime       pgtype.Text
		isDineIn        pgtype.Bool
		isTakeaway      pgtype.Bool
		isDelivery      pgtype.Bool
		dineInStart     pgtype.Text
		dineInEnd       pgtype.Text
		takeawayStart   pgtype.Text
		takeawayEnd     pgtype.Text
		deliveryStart   pgtype.Text
		deliveryEnd     pgtype.Text
		createdAt       time.Time
		updatedAt       time.Time
	)
	if err := h.DB.QueryRow(ctx, `
        select id, merchant_id, date, name, is_closed, open_time, close_time,
               is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
               dine_in_start_time, dine_in_end_time,
               takeaway_start_time, takeaway_end_time,
               delivery_start_time, delivery_end_time,
               created_at, updated_at
        from merchant_special_hours
        where id = $1 and merchant_id = $2
    `, specialHourID, merchantID).Scan(
		&id,
		&merchantIDValue,
		&date,
		&name,
		&isClosed,
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
		&createdAt,
		&updatedAt,
	); err != nil {
		return nil, err
	}

	return buildSpecialHourPayload(
		id,
		merchantIDValue,
		date,
		name,
		isClosed,
		openTime,
		closeTime,
		isDineIn,
		isTakeaway,
		isDelivery,
		dineInStart,
		dineInEnd,
		takeawayStart,
		takeawayEnd,
		deliveryStart,
		deliveryEnd,
		createdAt,
		updatedAt,
	), nil
}

func buildSpecialHourPayload(
	id int64,
	merchantID int64,
	date time.Time,
	name pgtype.Text,
	isClosed bool,
	openTime pgtype.Text,
	closeTime pgtype.Text,
	isDineIn pgtype.Bool,
	isTakeaway pgtype.Bool,
	isDelivery pgtype.Bool,
	dineInStart pgtype.Text,
	dineInEnd pgtype.Text,
	takeawayStart pgtype.Text,
	takeawayEnd pgtype.Text,
	deliveryStart pgtype.Text,
	deliveryEnd pgtype.Text,
	createdAt time.Time,
	updatedAt time.Time,
) map[string]any {
	return map[string]any{
		"id":                fmt.Sprint(id),
		"merchantId":        fmt.Sprint(merchantID),
		"date":              date,
		"name":              nullIfEmptyText(name),
		"isClosed":          isClosed,
		"openTime":          nullIfEmptyText(openTime),
		"closeTime":         nullIfEmptyText(closeTime),
		"isDineInEnabled":   nullableBool(isDineIn),
		"isTakeawayEnabled": nullableBool(isTakeaway),
		"isDeliveryEnabled": nullableBool(isDelivery),
		"dineInStartTime":   nullIfEmptyText(dineInStart),
		"dineInEndTime":     nullIfEmptyText(dineInEnd),
		"takeawayStartTime": nullIfEmptyText(takeawayStart),
		"takeawayEndTime":   nullIfEmptyText(takeawayEnd),
		"deliveryStartTime": nullIfEmptyText(deliveryStart),
		"deliveryEndTime":   nullIfEmptyText(deliveryEnd),
		"createdAt":         createdAt,
		"updatedAt":         updatedAt,
	}
}

func (h *Handler) fetchOpeningHoursSimple(ctx context.Context, merchantID int64) []map[string]any {
	rows, err := h.DB.Query(ctx, `
        select day_of_week, open_time, close_time, is_closed
        from merchant_opening_hours
        where merchant_id = $1
        order by day_of_week asc
    `, merchantID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	hours := make([]map[string]any, 0)
	for rows.Next() {
		var (
			dayOfWeek int
			openTime  pgtype.Text
			closeTime pgtype.Text
			isClosed  bool
		)
		if err := rows.Scan(&dayOfWeek, &openTime, &closeTime, &isClosed); err == nil {
			hours = append(hours, map[string]any{
				"dayOfWeek": dayOfWeek,
				"openTime":  nullIfEmptyText(openTime),
				"closeTime": nullIfEmptyText(closeTime),
				"isClosed":  isClosed,
			})
		}
	}
	return hours
}

func nullableBool(value pgtype.Bool) any {
	if value.Valid {
		return value.Bool
	}
	return nil
}

func normalizeDate(value time.Time) time.Time {
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func parseScheduleDateInput(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, errors.New("date is required")
	}
	if parsed, err := time.Parse("2006-01-02", trimmed); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return parsed, nil
	}
	return time.Time{}, errors.New("invalid date format")
}

func normalizeOptionalString(value any) any {
	if value == nil {
		return nil
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func isValidTimeHHMM(value string) bool {
	return timeHHMMPatternSchedule.MatchString(strings.TrimSpace(value))
}

func normalizeAndValidateTime(value any) (any, error) {
	normalized := normalizeOptionalString(value)
	if normalized == nil {
		return nil, nil
	}
	text := strings.TrimSpace(fmt.Sprint(normalized))
	if text == "" {
		return normalized, nil
	}
	if !isValidTimeHHMM(text) {
		return nil, errors.New("Invalid time format. Expected HH:MM")
	}
	return normalized, nil
}

func validateSpecialHourTimes(payload *specialHourPayload) error {
	fields := []*string{
		payload.OpenTime,
		payload.CloseTime,
		payload.DineInStartTime,
		payload.DineInEndTime,
		payload.TakeawayStartTime,
		payload.TakeawayEndTime,
		payload.DeliveryStartTime,
		payload.DeliveryEndTime,
	}
	for _, field := range fields {
		if field == nil {
			continue
		}
		if strings.TrimSpace(*field) == "" {
			continue
		}
		if !isValidTimeHHMM(*field) {
			return errors.New("Invalid time format. Expected HH:MM")
		}
	}
	return nil
}

func normalizeOptionalBool(value any) (any, bool) {
	if value == nil {
		return nil, true
	}
	if b, ok := value.(bool); ok {
		return b, true
	}
	return nil, false
}

func sortStrings(values []string) {
	if len(values) <= 1 {
		return
	}
	for i := 0; i < len(values)-1; i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}
