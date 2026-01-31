package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type specialPricePayload struct {
	Name           string
	MenuBookID     int64
	StartDate      time.Time
	EndDate        time.Time
	ApplicableDays []int32
	IsAllDay       bool
	StartTime      *string
	EndTime        *string
	PriceItems     []specialPriceItemPayload
}

type specialPriceItemPayload struct {
	MenuID     int64
	PromoPrice float64
}

var timeHHMMPattern = regexp.MustCompile(`^(?:[01]\d|2[0-3]):[0-5]\d$`)

func (h *Handler) MerchantSpecialPricesList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, `select exists(select 1 from merchants where id = $1)`, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select sp.id, sp.merchant_id, sp.menu_book_id, sp.name, sp.start_date, sp.end_date,
		       sp.applicable_days, sp.is_all_day, sp.start_time, sp.end_time, sp.is_active,
		       sp.created_at, sp.updated_at,
		       mb.name,
		       (select count(*) from menu_book_items mbi where mbi.menu_book_id = sp.menu_book_id) as menu_book_items_count,
		       (select count(*) from special_price_items spi where spi.special_price_id = sp.id) as price_items_count
		from special_prices sp
		join menu_books mb on mb.id = sp.menu_book_id
		where sp.merchant_id = $1
		order by sp.created_at desc
	`, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("special prices list failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve special prices")
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id                int64
			merchantID        int64
			menuBookID        int64
			name              string
			startDate         time.Time
			endDate           time.Time
			applicableDays    []int32
			isAllDay          bool
			startTime         pgtype.Text
			endTime           pgtype.Text
			isActive          bool
			createdAt         time.Time
			updatedAt         time.Time
			menuBookName      string
			menuBookItemCount int64
			priceItemCount    int64
		)
		if err := rows.Scan(
			&id, &merchantID, &menuBookID, &name, &startDate, &endDate,
			&applicableDays, &isAllDay, &startTime, &endTime, &isActive,
			&createdAt, &updatedAt, &menuBookName, &menuBookItemCount, &priceItemCount,
		); err != nil {
			continue
		}

		items = append(items, map[string]any{
			"id":             int64ToString(id),
			"merchantId":     int64ToString(merchantID),
			"menuBookId":     int64ToString(menuBookID),
			"name":           name,
			"startDate":      startDate,
			"endDate":        endDate,
			"applicableDays": int32ArrayValue(applicableDays),
			"isAllDay":       isAllDay,
			"startTime":      textOrNilSpecialPrice(startTime),
			"endTime":        textOrNilSpecialPrice(endTime),
			"isActive":       isActive,
			"createdAt":      createdAt,
			"updatedAt":      updatedAt,
			"menuBook": map[string]any{
				"id":   int64ToString(menuBookID),
				"name": menuBookName,
				"_count": map[string]any{
					"items": menuBookItemCount,
				},
			},
			"_count": map[string]any{
				"priceItems": priceItemCount,
			},
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    items,
	})
}

func (h *Handler) MerchantSpecialPricesCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	payload, err := decodeSpecialPricePayload(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	if payload.Name == "" || payload.MenuBookID == 0 || payload.StartDate.IsZero() || payload.EndDate.IsZero() {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Missing required fields")
		return
	}

	if err := validateSpecialPriceTimes(payload.IsAllDay, payload.StartTime, payload.EndTime); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	var menuBookExists bool
	if err := h.DB.QueryRow(ctx, `
		select exists(select 1 from menu_books where id = $1 and merchant_id = $2)
	`, payload.MenuBookID, *authCtx.MerchantID).Scan(&menuBookExists); err != nil || !menuBookExists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu book not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create special price")
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if len(payload.ApplicableDays) == 0 {
		payload.ApplicableDays = []int32{0, 1, 2, 3, 4, 5, 6}
	}

	var newID int64
	err = tx.QueryRow(ctx, `
		insert into special_prices (
			merchant_id, menu_book_id, name, start_date, end_date, applicable_days,
			is_all_day, start_time, end_time, is_active, created_at, updated_at
		) values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,true,now(),now()
		) returning id
	`,
		*authCtx.MerchantID,
		payload.MenuBookID,
		payload.Name,
		payload.StartDate,
		payload.EndDate,
		payload.ApplicableDays,
		payload.IsAllDay,
		payload.StartTime,
		payload.EndTime,
	).Scan(&newID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create special price")
		return
	}

	for _, item := range payload.PriceItems {
		if _, err = tx.Exec(ctx, `
			insert into special_price_items (special_price_id, menu_id, promo_price, created_at, updated_at)
			values ($1,$2,$3,now(),now())
		`, newID, item.MenuID, item.PromoPrice); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create special price")
			return
		}
	}

	if err = tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create special price")
		return
	}

	created, err := h.fetchSpecialPriceDetail(ctx, newID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create special price")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data":    created,
		"message": "Special price created successfully",
	})
}

func (h *Handler) MerchantSpecialPricesDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	priceID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special price id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, `select exists(select 1 from merchants where id = $1)`, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	detail, err := h.fetchSpecialPriceDetailByMerchant(ctx, priceID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special price not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    detail,
	})
}

func (h *Handler) MerchantSpecialPricesUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	priceID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special price id")
		return
	}

	var (
		existingMenuBookID int64
		existingName       string
		existingStartDate  time.Time
		existingEndDate    time.Time
		existingDays       []int32
		existingIsAllDay   bool
		existingStartTime  pgtype.Text
		existingEndTime    pgtype.Text
		existingIsActive   bool
	)

	if err := h.DB.QueryRow(ctx, `
		select menu_book_id, name, start_date, end_date, applicable_days, is_all_day, start_time, end_time, is_active
		from special_prices
		where id = $1 and merchant_id = $2
	`, priceID, *authCtx.MerchantID).Scan(
		&existingMenuBookID,
		&existingName,
		&existingStartDate,
		&existingEndDate,
		&existingDays,
		&existingIsAllDay,
		&existingStartTime,
		&existingEndTime,
		&existingIsActive,
	); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special price not found")
		return
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := existingName
	if value, ok := body["name"]; ok {
		name = strings.TrimSpace(readStringField(value))
		if name == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Name is required")
			return
		}
	}

	menuBookID := existingMenuBookID
	if value, ok := body["menuBookId"]; ok {
		parsed, ok := parseNumericID(value)
		if !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menuBookId")
			return
		}
		menuBookID = parsed
	}

	startDate := existingStartDate
	if value, ok := body["startDate"]; ok {
		parsed := parseSpecialPriceDateValue(value)
		if parsed.IsZero() {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid startDate")
			return
		}
		startDate = parsed
	}

	endDate := existingEndDate
	if value, ok := body["endDate"]; ok {
		parsed := parseSpecialPriceDateValue(value)
		if parsed.IsZero() {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid endDate")
			return
		}
		endDate = parsed
	}

	applicableDays := int32ArrayValue(existingDays)
	if value, ok := body["applicableDays"]; ok {
		parsed := parseInt32Slice(value)
		if parsed == nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid applicableDays")
			return
		}
		applicableDays = parsed
	}

	isAllDay := existingIsAllDay
	if value, ok := body["isAllDay"].(bool); ok {
		isAllDay = value
	}

	var startTime *string
	if existingStartTime.Valid {
		value := strings.TrimSpace(existingStartTime.String)
		if value != "" {
			startTime = &value
		}
	}
	if value, ok := body["startTime"]; ok {
		if value == nil {
			startTime = nil
		} else if str, ok := value.(string); ok {
			trimmed := strings.TrimSpace(str)
			if trimmed == "" {
				startTime = nil
			} else {
				startTime = &trimmed
			}
		} else {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid startTime")
			return
		}
	}

	var endTime *string
	if existingEndTime.Valid {
		value := strings.TrimSpace(existingEndTime.String)
		if value != "" {
			endTime = &value
		}
	}
	if value, ok := body["endTime"]; ok {
		if value == nil {
			endTime = nil
		} else if str, ok := value.(string); ok {
			trimmed := strings.TrimSpace(str)
			if trimmed == "" {
				endTime = nil
			} else {
				endTime = &trimmed
			}
		} else {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid endTime")
			return
		}
	}

	isActive := existingIsActive
	if value, ok := body["isActive"].(bool); ok {
		isActive = value
	}

	if err := validateSpecialPriceTimes(isAllDay, startTime, endTime); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	if menuBookID != existingMenuBookID {
		var menuBookExists bool
		if err := h.DB.QueryRow(ctx, `
			select exists(select 1 from menu_books where id = $1 and merchant_id = $2)
		`, menuBookID, *authCtx.MerchantID).Scan(&menuBookExists); err != nil || !menuBookExists {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu book not found")
			return
		}
	}

	priceItemsProvided := false
	var priceItems []specialPriceItemPayload
	if value, ok := body["priceItems"]; ok {
		priceItemsProvided = true
		priceItems = parseSpecialPriceItems(value)
	}

	startTimeValue := any(nil)
	endTimeValue := any(nil)
	if !isAllDay {
		startTimeValue = startTime
		endTimeValue = endTime
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, `
		update special_prices
		set name = $1, menu_book_id = $2, start_date = $3, end_date = $4, applicable_days = $5,
		    is_all_day = $6, start_time = $7, end_time = $8, is_active = $9, updated_at = now()
		where id = $10 and merchant_id = $11
	`,
		name,
		menuBookID,
		startDate,
		endDate,
		applicableDays,
		isAllDay,
		startTimeValue,
		endTimeValue,
		isActive,
		priceID,
		*authCtx.MerchantID,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
		return
	}

	if priceItemsProvided {
		if _, err = tx.Exec(ctx, `delete from special_price_items where special_price_id = $1`, priceID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
			return
		}
		for _, item := range priceItems {
			if _, err = tx.Exec(ctx, `
				insert into special_price_items (special_price_id, menu_id, promo_price, created_at, updated_at)
				values ($1,$2,$3,now(),now())
			`, priceID, item.MenuID, item.PromoPrice); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
				return
			}
		}
	}

	if err = tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
		return
	}

	updated, err := h.fetchSpecialPriceDetailByMerchant(ctx, priceID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update special price")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    updated,
		"message": "Special price updated successfully",
	})
}

func (h *Handler) MerchantSpecialPricesDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	priceID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid special price id")
		return
	}

	result, err := h.DB.Exec(ctx, `delete from special_prices where id = $1 and merchant_id = $2`, priceID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete special price")
		return
	}
	if result.RowsAffected() == 0 {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Special price not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Special price deleted successfully",
	})
}

func (h *Handler) fetchSpecialPriceDetail(ctx context.Context, specialPriceID int64) (map[string]any, error) {
	var (
		id             int64
		merchantID     int64
		menuBookID     int64
		name           string
		startDate      time.Time
		endDate        time.Time
		applicableDays []int32
		isAllDay       bool
		startTime      pgtype.Text
		endTime        pgtype.Text
		isActive       bool
		createdAt      time.Time
		updatedAt      time.Time
		menuBookName   string
	)

	if err := h.DB.QueryRow(ctx, `
		select sp.id, sp.merchant_id, sp.menu_book_id, sp.name, sp.start_date, sp.end_date,
		       sp.applicable_days, sp.is_all_day, sp.start_time, sp.end_time, sp.is_active,
		       sp.created_at, sp.updated_at, mb.name
		from special_prices sp
		join menu_books mb on mb.id = sp.menu_book_id
		where sp.id = $1
	`, specialPriceID).Scan(
		&id, &merchantID, &menuBookID, &name, &startDate, &endDate,
		&applicableDays, &isAllDay, &startTime, &endTime, &isActive,
		&createdAt, &updatedAt, &menuBookName,
	); err != nil {
		return nil, err
	}

	priceItems := make([]map[string]any, 0)
	itemRows, err := h.DB.Query(ctx, `
		select spi.id, spi.menu_id, spi.promo_price, m.name, m.price
		from special_price_items spi
		join menus m on m.id = spi.menu_id
		where spi.special_price_id = $1
	`, specialPriceID)
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	defer func() {
		if itemRows != nil {
			itemRows.Close()
		}
	}()

	for itemRows.Next() {
		var (
			itemID     int64
			menuID     int64
			promoPrice pgtype.Numeric
			menuName   string
			menuPrice  pgtype.Numeric
		)
		if err := itemRows.Scan(&itemID, &menuID, &promoPrice, &menuName, &menuPrice); err != nil {
			continue
		}
		priceItems = append(priceItems, map[string]any{
			"id":         int64ToString(itemID),
			"menuId":     int64ToString(menuID),
			"promoPrice": utils.NumericToFloat64(promoPrice),
			"menu": map[string]any{
				"id":    int64ToString(menuID),
				"name":  menuName,
				"price": utils.NumericToFloat64(menuPrice),
			},
		})
	}

	return map[string]any{
		"id":             int64ToString(id),
		"merchantId":     int64ToString(merchantID),
		"menuBookId":     int64ToString(menuBookID),
		"name":           name,
		"startDate":      startDate,
		"endDate":        endDate,
		"applicableDays": int32ArrayValue(applicableDays),
		"isAllDay":       isAllDay,
		"startTime":      textOrNilSpecialPrice(startTime),
		"endTime":        textOrNilSpecialPrice(endTime),
		"isActive":       isActive,
		"createdAt":      createdAt,
		"updatedAt":      updatedAt,
		"menuBook": map[string]any{
			"id":   int64ToString(menuBookID),
			"name": menuBookName,
		},
		"priceItems": priceItems,
	}, nil
}

func (h *Handler) fetchSpecialPriceDetailByMerchant(ctx context.Context, specialPriceID int64, merchantID int64) (map[string]any, error) {
	var exists bool
	if err := h.DB.QueryRow(ctx, `
		select exists(select 1 from special_prices where id = $1 and merchant_id = $2)
	`, specialPriceID, merchantID).Scan(&exists); err != nil || !exists {
		return nil, pgx.ErrNoRows
	}

	return h.fetchSpecialPriceDetail(ctx, specialPriceID)
}

func decodeSpecialPricePayload(r *http.Request) (specialPricePayload, error) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return specialPricePayload{}, err
	}

	payload := specialPricePayload{}
	payload.Name = strings.TrimSpace(readStringField(body["name"]))
	if menuBookID, ok := parseNumericID(body["menuBookId"]); ok {
		payload.MenuBookID = menuBookID
	}

	payload.StartDate = parseSpecialPriceDateValue(body["startDate"])
	payload.EndDate = parseSpecialPriceDateValue(body["endDate"])

	payload.IsAllDay = true
	if value, ok := body["isAllDay"].(bool); ok {
		payload.IsAllDay = value
	}

	if !payload.IsAllDay {
		if value, ok := body["startTime"].(string); ok && strings.TrimSpace(value) != "" {
			trimmed := strings.TrimSpace(value)
			payload.StartTime = &trimmed
		}
		if value, ok := body["endTime"].(string); ok && strings.TrimSpace(value) != "" {
			trimmed := strings.TrimSpace(value)
			payload.EndTime = &trimmed
		}
	}

	payload.ApplicableDays = parseInt32Slice(body["applicableDays"])
	payload.PriceItems = parseSpecialPriceItems(body["priceItems"])

	return payload, nil
}

func parseSpecialPriceDateValue(value any) time.Time {
	if value == nil {
		return time.Time{}
	}
	switch v := value.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return time.Time{}
		}
		if parsed, err := time.Parse("2006-01-02", trimmed); err == nil {
			return parsed
		}
		if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
			return parsed
		}
	case float64:
		return time.Unix(int64(v/1000), 0).UTC()
	}
	return time.Time{}
}

func parseInt32Slice(value any) []int32 {
	if value == nil {
		return nil
	}
	slice, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]int32, 0, len(slice))
	for _, item := range slice {
		if parsed, ok := parseNumericID(item); ok {
			out = append(out, int32(parsed))
		}
	}
	return out
}

func parseSpecialPriceItems(value any) []specialPriceItemPayload {
	if value == nil {
		return nil
	}
	slice, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]specialPriceItemPayload, 0)
	for _, item := range slice {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		menuID, ok := parseNumericID(row["menuId"])
		if !ok {
			continue
		}
		promo := 0.0
		if value, ok := row["promoPrice"].(float64); ok {
			promo = value
		}
		out = append(out, specialPriceItemPayload{MenuID: menuID, PromoPrice: promo})
	}
	return out
}

func validateSpecialPriceTimes(isAllDay bool, startTime *string, endTime *string) error {
	if !isAllDay {
		if startTime == nil || endTime == nil {
			return errors.New("startTime and endTime are required when isAllDay is false")
		}
	}
	if startTime != nil && !timeHHMMPattern.MatchString(*startTime) {
		return errors.New("Invalid time format. Expected HH:MM")
	}
	if endTime != nil && !timeHHMMPattern.MatchString(*endTime) {
		return errors.New("Invalid time format. Expected HH:MM")
	}
	return nil
}

func int32ArrayValue(value []int32) []int32 {
	if len(value) == 0 {
		return []int32{}
	}
	out := make([]int32, len(value))
	copy(out, value)
	return out
}

func textOrNilSpecialPrice(value pgtype.Text) any {
	if value.Valid {
		return value.String
	}
	return nil
}
