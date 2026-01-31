package handlers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
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
	"github.com/jackc/pgx/v5/pgxpool"
)

const voucherCodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

var voucherCodeRegex = regexp.MustCompile(`^[A-Z0-9]+$`)

func (h *Handler) MerchantOrderVoucherTemplateGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}

	var (
		id             int64
		name           string
		description    pgtype.Text
		audience       string
		discountType   string
		discountValue  pgtype.Numeric
		maxDiscount    pgtype.Numeric
		minOrder       pgtype.Numeric
		maxUsesTotal   pgtype.Int4
		maxUsesCust    pgtype.Int4
		maxUsesOrder   int32
		totalCap       pgtype.Numeric
		allowedTypes   []string
		daysOfWeek     []int32
		startTime      pgtype.Text
		endTime        pgtype.Text
		includeAll     bool
		reportCategory pgtype.Text
		validFrom      pgtype.Timestamptz
		validUntil     pgtype.Timestamptz
		isActive       bool
		requiresLogin  bool
		createdAt      time.Time
		updatedAt      time.Time
		codesCount     int64
		discountsCount int64
	)

	query := `
        select t.id, t.name, t.description, t.audience, t.discount_type, t.discount_value,
               t.max_discount_amount, t.min_order_amount, t.max_uses_total, t.max_uses_per_customer,
               t.max_uses_per_order, t.total_discount_cap, t.allowed_order_types, t.days_of_week,
               t.start_time, t.end_time, t.include_all_items, t.report_category, t.valid_from, t.valid_until,
               t.is_active, t.requires_customer_login, t.created_at, t.updated_at,
               (select count(*) from order_voucher_codes c where c.template_id = t.id) as codes_count,
               (select count(*) from order_discounts d where d.voucher_template_id = t.id) as discounts_count
        from order_voucher_templates t
        where t.id = $1 and t.merchant_id = $2
    `
	if err := h.DB.QueryRow(ctx, query, templateID, *authCtx.MerchantID).Scan(
		&id, &name, &description, &audience, &discountType, &discountValue, &maxDiscount, &minOrder, &maxUsesTotal,
		&maxUsesCust, &maxUsesOrder, &totalCap, &allowedTypes, &daysOfWeek, &startTime, &endTime, &includeAll,
		&reportCategory, &validFrom, &validUntil, &isActive, &requiresLogin, &createdAt, &updatedAt, &codesCount, &discountsCount,
	); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Template not found")
			return
		}
		h.Logger.Error("order voucher template lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher template")
		return
	}

	menuScopes, err := h.fetchVoucherMenuScopes(ctx, id)
	if err != nil {
		h.Logger.Error("order voucher menu scopes fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher template")
		return
	}
	categoryScopes, err := h.fetchVoucherCategoryScopes(ctx, id)
	if err != nil {
		h.Logger.Error("order voucher category scopes fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher template")
		return
	}

	payload := map[string]any{
		"id":                    id,
		"name":                  name,
		"audience":              audience,
		"discountType":          discountType,
		"discountValue":         utils.NumericToFloat64(discountValue),
		"includeAllItems":       includeAll,
		"isActive":              isActive,
		"requiresCustomerLogin": requiresLogin,
		"createdAt":             createdAt,
		"updatedAt":             updatedAt,
		"menuScopes":            menuScopes,
		"categoryScopes":        categoryScopes,
		"_count": map[string]any{
			"codes":          codesCount,
			"orderDiscounts": discountsCount,
		},
	}

	if description.Valid {
		payload["description"] = description.String
	}
	if maxDiscount.Valid {
		payload["maxDiscountAmount"] = utils.NumericToFloat64(maxDiscount)
	}
	if minOrder.Valid {
		payload["minOrderAmount"] = utils.NumericToFloat64(minOrder)
	}
	if maxUsesTotal.Valid {
		payload["maxUsesTotal"] = maxUsesTotal.Int32
	}
	if maxUsesCust.Valid {
		payload["maxUsesPerCustomer"] = maxUsesCust.Int32
	}
	payload["maxUsesPerOrder"] = maxUsesOrder
	if totalCap.Valid {
		payload["totalDiscountCap"] = utils.NumericToFloat64(totalCap)
	}
	if allowedTypes != nil {
		items := make([]string, len(allowedTypes))
		copy(items, allowedTypes)
		payload["allowedOrderTypes"] = items
	}
	if daysOfWeek != nil {
		items := make([]int32, len(daysOfWeek))
		copy(items, daysOfWeek)
		payload["daysOfWeek"] = items
	}
	if startTime.Valid {
		payload["startTime"] = startTime.String
	}
	if endTime.Valid {
		payload["endTime"] = endTime.String
	}
	if reportCategory.Valid {
		payload["reportCategory"] = reportCategory.String
	}
	if validFrom.Valid {
		payload["validFrom"] = validFrom.Time
	}
	if validUntil.Valid {
		payload["validUntil"] = validUntil.Time
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
	})
}

func (h *Handler) fetchVoucherTemplateBase(ctx context.Context, templateID int64, merchantID int64) (map[string]any, error) {
	var (
		id             int64
		name           string
		description    pgtype.Text
		audience       string
		discountType   string
		discountValue  pgtype.Numeric
		maxDiscount    pgtype.Numeric
		minOrder       pgtype.Numeric
		maxUsesTotal   pgtype.Int4
		maxUsesCust    pgtype.Int4
		maxUsesOrder   int32
		totalCap       pgtype.Numeric
		allowedTypes   []string
		daysOfWeek     []int32
		startTime      pgtype.Text
		endTime        pgtype.Text
		includeAll     bool
		reportCategory pgtype.Text
		validFrom      pgtype.Timestamptz
		validUntil     pgtype.Timestamptz
		isActive       bool
		requiresLogin  bool
		createdAt      time.Time
		updatedAt      time.Time
	)

	query := `
        select id, name, description, audience, discount_type, discount_value,
               max_discount_amount, min_order_amount, max_uses_total, max_uses_per_customer,
               max_uses_per_order, total_discount_cap, allowed_order_types, days_of_week,
               start_time, end_time, include_all_items, report_category, valid_from, valid_until,
               is_active, requires_customer_login, created_at, updated_at
        from order_voucher_templates
        where id = $1 and merchant_id = $2
    `
	if err := h.DB.QueryRow(ctx, query, templateID, merchantID).Scan(
		&id, &name, &description, &audience, &discountType, &discountValue, &maxDiscount, &minOrder, &maxUsesTotal,
		&maxUsesCust, &maxUsesOrder, &totalCap, &allowedTypes, &daysOfWeek, &startTime, &endTime, &includeAll,
		&reportCategory, &validFrom, &validUntil, &isActive, &requiresLogin, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}

	payload := map[string]any{
		"id":                    id,
		"name":                  name,
		"audience":              audience,
		"discountType":          discountType,
		"discountValue":         utils.NumericToFloat64(discountValue),
		"includeAllItems":       includeAll,
		"isActive":              isActive,
		"requiresCustomerLogin": requiresLogin,
		"createdAt":             createdAt,
		"updatedAt":             updatedAt,
	}

	if description.Valid {
		payload["description"] = description.String
	}
	if maxDiscount.Valid {
		payload["maxDiscountAmount"] = utils.NumericToFloat64(maxDiscount)
	}
	if minOrder.Valid {
		payload["minOrderAmount"] = utils.NumericToFloat64(minOrder)
	}
	if maxUsesTotal.Valid {
		payload["maxUsesTotal"] = maxUsesTotal.Int32
	}
	if maxUsesCust.Valid {
		payload["maxUsesPerCustomer"] = maxUsesCust.Int32
	}
	payload["maxUsesPerOrder"] = maxUsesOrder
	if totalCap.Valid {
		payload["totalDiscountCap"] = utils.NumericToFloat64(totalCap)
	}
	if allowedTypes != nil {
		items := make([]string, len(allowedTypes))
		copy(items, allowedTypes)
		payload["allowedOrderTypes"] = items
	}
	if daysOfWeek != nil {
		items := make([]int32, len(daysOfWeek))
		copy(items, daysOfWeek)
		payload["daysOfWeek"] = items
	}
	if startTime.Valid {
		payload["startTime"] = startTime.String
	}
	if endTime.Valid {
		payload["endTime"] = endTime.String
	}
	if reportCategory.Valid {
		payload["reportCategory"] = reportCategory.String
	}
	if validFrom.Valid {
		payload["validFrom"] = validFrom.Time
	}
	if validUntil.Valid {
		payload["validUntil"] = validUntil.Time
	}
	return payload, nil
}
func (h *Handler) MerchantOrderVoucherTemplateUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from order_voucher_templates where id = $1 and merchant_id = $2)", templateID, *authCtx.MerchantID).Scan(&exists); err != nil {
		h.Logger.Error("order voucher template lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
		return
	}
	if !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Template not found")
		return
	}

	payload := map[string]json.RawMessage{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	updates := make([]string, 0)
	args := make([]any, 0)
	addUpdate := func(column string, value any) {
		updates = append(updates, fmt.Sprintf("%s = $%d", column, len(args)+1))
		args = append(args, value)
	}

	audience, audienceProvided, err := readStringFieldRaw(payload, "audience")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "audience must be a string")
		return
	}
	if audienceProvided {
		audience = strings.ToUpper(strings.TrimSpace(audience))
		if audience != "POS" && audience != "CUSTOMER" && audience != "BOTH" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid audience")
			return
		}
		addUpdate("audience", audience)
	}

	name, nameProvided, err := readStringFieldRaw(payload, "name")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "name must be a string")
		return
	}
	if nameProvided {
		addUpdate("name", strings.TrimSpace(name))
	}

	description, descriptionProvided, err := readNullableStringFieldRaw(payload, "description")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "description must be a string or null")
		return
	}
	if descriptionProvided {
		addUpdate("description", description)
	}

	discountType, discountTypeProvided, err := readStringFieldRaw(payload, "discountType")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "discountType must be a string")
		return
	}
	if discountTypeProvided {
		discountType = strings.ToUpper(strings.TrimSpace(discountType))
		if discountType != "PERCENTAGE" && discountType != "FIXED_AMOUNT" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid discountType")
			return
		}
		addUpdate("discount_type", discountType)
	}

	discountValue, discountValueProvided, err := readFloatFieldRaw(payload, "discountValue")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "discountValue must be a number")
		return
	}
	if discountValueProvided {
		if discountValue <= 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Discount value must be greater than 0")
			return
		}
		if discountTypeProvided && discountType == "PERCENTAGE" && discountValue > 100 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Percentage discount cannot exceed 100")
			return
		}
		addUpdate("discount_value", discountValue)
	}

	isActive, isActiveProvided, err := readBoolFieldRaw(payload, "isActive")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isActive must be a boolean")
		return
	}
	if isActiveProvided {
		addUpdate("is_active", isActive)
	}

	maxDiscount, maxDiscountProvided, err := readNullableFloatFieldRaw(payload, "maxDiscountAmount")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "maxDiscountAmount must be a number or null")
		return
	}
	if maxDiscountProvided {
		addUpdate("max_discount_amount", maxDiscount)
	}

	minOrder, minOrderProvided, err := readNullableFloatFieldRaw(payload, "minOrderAmount")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "minOrderAmount must be a number or null")
		return
	}
	if minOrderProvided {
		if minOrder != nil && *minOrder < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "minOrderAmount must be >= 0")
			return
		}
		addUpdate("min_order_amount", minOrder)
	}

	maxUsesTotal, maxUsesTotalProvided, err := readNullableIntFieldRaw(payload, "maxUsesTotal")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "maxUsesTotal must be a number or null")
		return
	}
	if maxUsesTotalProvided {
		if maxUsesTotal != nil && *maxUsesTotal < 1 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "maxUsesTotal must be >= 1")
			return
		}
		addUpdate("max_uses_total", maxUsesTotal)
	}

	maxUsesCustomer, maxUsesCustomerProvided, err := readNullableIntFieldRaw(payload, "maxUsesPerCustomer")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "maxUsesPerCustomer must be a number or null")
		return
	}
	if maxUsesCustomerProvided {
		if maxUsesCustomer != nil && *maxUsesCustomer < 1 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "maxUsesPerCustomer must be >= 1")
			return
		}
		addUpdate("max_uses_per_customer", maxUsesCustomer)
	}

	addUpdate("max_uses_per_order", 1)

	totalCap, totalCapProvided, err := readNullableFloatFieldRaw(payload, "totalDiscountCap")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "totalDiscountCap must be a number or null")
		return
	}
	if totalCapProvided {
		if totalCap != nil && *totalCap < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "totalDiscountCap must be >= 0")
			return
		}
		addUpdate("total_discount_cap", totalCap)
	}

	allowedTypes, allowedTypesProvided, err := readStringArrayFieldRaw(payload, "allowedOrderTypes")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "allowedOrderTypes must be an array of strings")
		return
	}
	if allowedTypesProvided {
		addUpdate("allowed_order_types", cleanOrderTypes(allowedTypes))
	}

	daysOfWeek, daysOfWeekProvided, err := readIntArrayFieldRaw(payload, "daysOfWeek")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "daysOfWeek must be an array of numbers")
		return
	}
	if daysOfWeekProvided {
		addUpdate("days_of_week", cleanDaysOfWeek(daysOfWeek))
	}

	startTime, startProvided, err := readNullableStringFieldRaw(payload, "startTime")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "startTime must be a string or null")
		return
	}
	endTime, endProvided, err := readNullableStringFieldRaw(payload, "endTime")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "endTime must be a string or null")
		return
	}

	if (startProvided && !endProvided) || (!startProvided && endProvided) {
		if (startProvided && startTime == nil && !endProvided) || (endProvided && endTime == nil && !startProvided) {
			// allow clearing one side
		} else {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "startTime and endTime must both be provided")
			return
		}
	}
	if startProvided && startTime != nil {
		trimmed := strings.TrimSpace(*startTime)
		if trimmed != "" && !isValidTimeString(trimmed) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid startTime (expected HH:MM)")
			return
		}
		addUpdate("start_time", nullableString(trimmed))
	}
	if endProvided && endTime != nil {
		trimmed := strings.TrimSpace(*endTime)
		if trimmed != "" && !isValidTimeString(trimmed) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid endTime (expected HH:MM)")
			return
		}
		addUpdate("end_time", nullableString(trimmed))
	}
	if startProvided && startTime == nil {
		addUpdate("start_time", nil)
	}
	if endProvided && endTime == nil {
		addUpdate("end_time", nil)
	}

	includeAllItems, includeAllProvided, err := readBoolFieldRaw(payload, "includeAllItems")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "includeAllItems must be a boolean")
		return
	}
	if includeAllProvided {
		addUpdate("include_all_items", includeAllItems)
	}

	reportCategory, reportProvided, err := readNullableStringFieldRaw(payload, "reportCategory")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "reportCategory must be a string or null")
		return
	}
	if reportProvided {
		if reportCategory != nil {
			trimmed := strings.TrimSpace(*reportCategory)
			if trimmed == "" {
				reportCategory = nil
			} else {
				reportCategory = &trimmed
			}
		}
		addUpdate("report_category", reportCategory)
	}

	validFrom, validFromProvided, err := readNullableTimeFieldRaw(payload, "validFrom")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "validFrom is invalid")
		return
	}
	if validFromProvided {
		addUpdate("valid_from", validFrom)
	}
	validUntil, validUntilProvided, err := readNullableTimeFieldRaw(payload, "validUntil")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "validUntil is invalid")
		return
	}
	if validUntilProvided {
		addUpdate("valid_until", validUntil)
	}

	requiresLogin, requiresProvided, err := readBoolFieldRaw(payload, "requiresCustomerLogin")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "requiresCustomerLogin must be a boolean")
		return
	}
	if requiresProvided {
		addUpdate("requires_customer_login", requiresLogin)
	}

	shouldUpdateScopes := includeAllProvided || payload["scopedMenuIds"] != nil || payload["scopedCategoryIds"] != nil

	menuIDs := []int64{}
	categoryIDs := []int64{}
	if includeAllProvided && !includeAllItems {
		menuIDs = parseIDArray(parseRawArray(payload["scopedMenuIds"]))
		categoryIDs = parseIDArray(parseRawArray(payload["scopedCategoryIds"]))
	}

	if includeAllProvided && !includeAllItems {
		if len(menuIDs) > 0 {
			var count int64
			if err := h.DB.QueryRow(ctx, "select count(*) from menus where merchant_id = $1 and deleted_at is null and id = any($2)", *authCtx.MerchantID, menuIDs).Scan(&count); err != nil {
				h.Logger.Error("order voucher menu validation failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
				return
			}
			if count != int64(len(menuIDs)) {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid scopedMenuIds")
				return
			}
		}
		if len(categoryIDs) > 0 {
			var count int64
			if err := h.DB.QueryRow(ctx, "select count(*) from menu_categories where merchant_id = $1 and deleted_at is null and id = any($2)", *authCtx.MerchantID, categoryIDs).Scan(&count); err != nil {
				h.Logger.Error("order voucher category validation failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
				return
			}
			if count != int64(len(categoryIDs)) {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid scopedCategoryIds")
				return
			}
		}
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		h.Logger.Error("order voucher update begin failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if len(updates) > 0 {
		args = append(args, templateID, *authCtx.MerchantID)
		query := "update order_voucher_templates set " + strings.Join(updates, ", ") + " where id = $" + intToString(len(args)-1) + " and merchant_id = $" + intToString(len(args))
		if _, err := tx.Exec(ctx, query, args...); err != nil {
			h.Logger.Error("order voucher update failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
			return
		}
	}

	if shouldUpdateScopes {
		if _, err := tx.Exec(ctx, "delete from order_voucher_template_menus where template_id = $1", templateID); err != nil {
			h.Logger.Error("order voucher scope delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
			return
		}
		if _, err := tx.Exec(ctx, "delete from order_voucher_template_categories where template_id = $1", templateID); err != nil {
			h.Logger.Error("order voucher scope delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
			return
		}
		if includeAllProvided && !includeAllItems {
			if err := insertVoucherScopes(ctx, tx, templateID, menuIDs, categoryIDs); err != nil {
				h.Logger.Error("order voucher scope insert failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
				return
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("order voucher update commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
		return
	}

	updated, err := h.fetchVoucherTemplateBase(ctx, templateID, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("order voucher template fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher template")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    updated,
	})
}

func (h *Handler) MerchantOrderVoucherTemplateCodesList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}

	if !h.ensureVoucherTemplate(ctx, templateID, *authCtx.MerchantID, w) {
		return
	}

	take := clampInt(parseQueryInt(r, "take", 200), 1, 500)

	rows, err := h.DB.Query(ctx, `
        select c.id, c.code, c.is_active, c.created_at, c.valid_from, c.valid_until,
               (select count(*) from order_discounts od where od.voucher_code_id = c.id) as used_count
        from order_voucher_codes c
        where c.merchant_id = $1 and c.template_id = $2
        order by c.created_at desc
        limit $3
    `, *authCtx.MerchantID, templateID, take)
	if err != nil {
		h.Logger.Error("order voucher codes query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher codes")
		return
	}
	defer rows.Close()

	codes := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id         int64
			code       string
			isActive   bool
			createdAt  time.Time
			validFrom  pgtype.Timestamptz
			validUntil pgtype.Timestamptz
			usedCount  int64
		)
		if err := rows.Scan(&id, &code, &isActive, &createdAt, &validFrom, &validUntil, &usedCount); err != nil {
			h.Logger.Error("order voucher codes scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher codes")
			return
		}
		entry := map[string]any{
			"id":        id,
			"code":      code,
			"isActive":  isActive,
			"createdAt": createdAt,
			"usedCount": usedCount,
		}
		if validFrom.Valid {
			entry["validFrom"] = validFrom.Time
		}
		if validUntil.Valid {
			entry["validUntil"] = validUntil.Time
		}
		codes = append(codes, entry)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    codes,
	})
}

func (h *Handler) MerchantOrderVoucherTemplateCodesCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}

	if !h.ensureVoucherTemplate(ctx, templateID, *authCtx.MerchantID, w) {
		return
	}

	payload := map[string]any{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	manualCodes := normalizeManualCodes(payload["manualCodes"], payload["manualCode"])
	if len(manualCodes) > 0 {
		if len(manualCodes) > 500 {
			manualCodes = manualCodes[:500]
		}
		invalid := findInvalidCode(manualCodes)
		if invalid != "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Invalid code: %s. Codes must be 3-32 chars and contain only A-Z, 0-9.", invalid))
			return
		}

		existing, err := fetchExistingVoucherCodes(ctx, h.DB, *authCtx.MerchantID, manualCodes)
		if err != nil {
			h.Logger.Error("order voucher existing codes lookup failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
			return
		}

		toCreate := make([]string, 0)
		for _, code := range manualCodes {
			if _, ok := existing[code]; !ok {
				toCreate = append(toCreate, code)
			}
		}
		for _, code := range toCreate {
			if _, err := h.DB.Exec(ctx, `
                insert into order_voucher_codes (merchant_id, template_id, code, is_active)
                values ($1, $2, $3, true)
                on conflict do nothing
            `, *authCtx.MerchantID, templateID, code); err != nil {
				h.Logger.Error("order voucher code insert failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
				return
			}
		}

		codes, err := fetchVoucherCodesByValue(ctx, h.DB, *authCtx.MerchantID, templateID, manualCodes)
		if err != nil {
			h.Logger.Error("order voucher codes fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
			return
		}

		skipped := make([]string, 0)
		for _, code := range manualCodes {
			if _, ok := existing[code]; ok {
				skipped = append(skipped, code)
			}
		}
		message := "Voucher codes added"
		if len(skipped) > 0 {
			message = "Some codes already exist and were skipped."
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    codes,
			"message": message,
		})
		return
	}

	count := clampInt(parseMapInt(payload["count"], 1), 1, 500)
	length := clampInt(parseMapInt(payload["length"], 8), 6, 16)

	createdCodes := make([]string, 0)
	attempts := 0
	for len(createdCodes) < count && attempts < 6 {
		attempts++
		remaining := count - len(createdCodes)
		batchSize := remaining
		if batchSize > 200 {
			batchSize = 200
		}
		batch := make(map[string]struct{})
		for len(batch) < batchSize {
			batch[generateVoucherCode(length)] = struct{}{}
		}
		batchList := make([]string, 0, len(batch))
		for code := range batch {
			batchList = append(batchList, code)
		}
		for _, code := range batchList {
			if _, err := h.DB.Exec(ctx, `
                insert into order_voucher_codes (merchant_id, template_id, code, is_active)
                values ($1, $2, $3, true)
                on conflict do nothing
            `, *authCtx.MerchantID, templateID, code); err != nil {
				h.Logger.Error("order voucher code insert failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
				return
			}
		}

		inserted, err := fetchVoucherCodesByValue(ctx, h.DB, *authCtx.MerchantID, templateID, batchList)
		if err != nil {
			h.Logger.Error("order voucher codes fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
			return
		}
		for _, entry := range inserted {
			createdCodes = append(createdCodes, entry["code"].(string))
		}
	}

	codes, err := fetchVoucherCodesByValue(ctx, h.DB, *authCtx.MerchantID, templateID, createdCodes)
	if err != nil {
		h.Logger.Error("order voucher codes fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher codes")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    codes,
		"message": "Voucher codes added",
	})
}

func (h *Handler) MerchantOrderVoucherTemplateCodeUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}
	codeID, ok := parseVoucherCodeID(w, r)
	if !ok {
		return
	}

	var body struct {
		IsActive *bool `json:"isActive"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if body.IsActive == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isActive must be a boolean")
		return
	}

	res, err := h.DB.Exec(ctx, `
        update order_voucher_codes
        set is_active = $1
        where id = $2 and merchant_id = $3 and template_id = $4
    `, *body.IsActive, codeID, *authCtx.MerchantID, templateID)
	if err != nil {
		h.Logger.Error("order voucher code update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher code")
		return
	}
	if res.RowsAffected() == 0 {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Code not found")
		return
	}

	code, err := fetchVoucherCode(ctx, h.DB, *authCtx.MerchantID, templateID, codeID)
	if err != nil {
		h.Logger.Error("order voucher code fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update voucher code")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    code,
	})
}

func (h *Handler) MerchantOrderVoucherTemplateCodeDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}
	codeID, ok := parseVoucherCodeID(w, r)
	if !ok {
		return
	}

	var usedCount int64
	if err := h.DB.QueryRow(ctx, `
        select count(*) from order_discounts where voucher_code_id = $1
    `, codeID).Scan(&usedCount); err != nil {
		h.Logger.Error("order voucher code usage lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete voucher code")
		return
	}
	if usedCount > 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot delete a code that has already been used. Please deactivate it instead.")
		return
	}

	res, err := h.DB.Exec(ctx, `
        delete from order_voucher_codes
        where id = $1 and merchant_id = $2 and template_id = $3
    `, codeID, *authCtx.MerchantID, templateID)
	if err != nil {
		h.Logger.Error("order voucher code delete failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete voucher code")
		return
	}
	if res.RowsAffected() == 0 {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Code not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
	})
}

func (h *Handler) MerchantOrderVoucherTemplateUsage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	templateID, ok := parseVoucherTemplateID(w, r)
	if !ok {
		return
	}

	if !h.ensureVoucherTemplate(ctx, templateID, *authCtx.MerchantID, w) {
		return
	}

	query := r.URL.Query()
	take := clampInt(parseQueryInt(r, "take", 50), 1, 200)
	cursor := parseQueryInt64(query.Get("cursor"))
	startDate := time.Time{}
	if parsed := parseOptionalTimePtr(query.Get("startDate")); parsed != nil {
		startDate = *parsed
	}
	endDate := time.Time{}
	if parsed := parseOptionalTimePtr(query.Get("endDate")); parsed != nil {
		endDate = *parsed
	}

	source := strings.ToUpper(strings.TrimSpace(query.Get("source")))
	if source != "POS_VOUCHER" && source != "CUSTOMER_VOUCHER" && source != "MANUAL" {
		source = ""
	}

	codeQuery := strings.TrimSpace(query.Get("code"))

	filters := make([]string, 0)
	args := []any{*authCtx.MerchantID, templateID}
	filters = append(filters, "od.merchant_id = $1", "od.voucher_template_id = $2")
	argPos := 3

	if source != "" {
		filters = append(filters, "od.source = $"+intToString(argPos))
		args = append(args, source)
		argPos++
	}
	if codeQuery != "" {
		filters = append(filters, "vc.code ilike $"+intToString(argPos))
		args = append(args, "%"+codeQuery+"%")
		argPos++
	}
	if !startDate.IsZero() || !endDate.IsZero() {
		if !startDate.IsZero() {
			filters = append(filters, "o.placed_at >= $"+intToString(argPos))
			args = append(args, startDate)
			argPos++
		}
		if !endDate.IsZero() {
			filters = append(filters, "o.placed_at <= $"+intToString(argPos))
			args = append(args, endDate)
			argPos++
		}
	}
	if cursor != nil {
		filters = append(filters, "od.id < $"+intToString(argPos))
		args = append(args, *cursor)
		argPos++
	}

	querySQL := `
        select od.id, od.created_at, od.source, od.label, od.discount_amount,
               vc.id, vc.code,
               u.id, u.name, u.email,
               c.id, c.name, c.phone,
               o.id, o.order_number, o.order_type, o.status, o.placed_at, o.subtotal, o.discount_amount, o.total_amount,
               oc.id, oc.name, oc.phone
        from order_discounts od
        join orders o on o.id = od.order_id
        left join order_voucher_codes vc on vc.id = od.voucher_code_id
        left join users u on u.id = od.applied_by_user_id
        left join customers c on c.id = od.applied_by_customer_id
        left join customers oc on oc.id = o.customer_id
        where ` + strings.Join(filters, " and ") + `
        order by od.created_at desc, od.id desc
        limit $` + intToString(argPos)
	args = append(args, take+1)

	rows, err := h.DB.Query(ctx, querySQL, args...)
	if err != nil {
		h.Logger.Error("order voucher usage query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher usage")
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id             int64
			createdAt      time.Time
			sourceValue    string
			label          string
			discountAmount pgtype.Numeric
			voucherID      pgtype.Int8
			voucherCode    pgtype.Text
			userID         pgtype.Int8
			userName       pgtype.Text
			userEmail      pgtype.Text
			customerID     pgtype.Int8
			customerName   pgtype.Text
			customerPhone  pgtype.Text
			orderID        int64
			orderNumber    string
			orderType      string
			orderStatus    string
			orderPlacedAt  time.Time
			orderSubtotal  pgtype.Numeric
			orderDiscount  pgtype.Numeric
			orderTotal     pgtype.Numeric
			orderCustID    pgtype.Int8
			orderCustName  pgtype.Text
			orderCustPhone pgtype.Text
		)
		if err := rows.Scan(
			&id, &createdAt, &sourceValue, &label, &discountAmount,
			&voucherID, &voucherCode,
			&userID, &userName, &userEmail,
			&customerID, &customerName, &customerPhone,
			&orderID, &orderNumber, &orderType, &orderStatus, &orderPlacedAt, &orderSubtotal, &orderDiscount, &orderTotal,
			&orderCustID, &orderCustName, &orderCustPhone,
		); err != nil {
			h.Logger.Error("order voucher usage scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher usage")
			return
		}

		item := map[string]any{
			"id":             id,
			"createdAt":      createdAt,
			"source":         sourceValue,
			"label":          label,
			"discountAmount": utils.NumericToFloat64(discountAmount),
			"order": map[string]any{
				"id":             orderID,
				"orderNumber":    orderNumber,
				"orderType":      orderType,
				"status":         orderStatus,
				"placedAt":       orderPlacedAt,
				"subtotal":       utils.NumericToFloat64(orderSubtotal),
				"discountAmount": utils.NumericToFloat64(orderDiscount),
				"totalAmount":    utils.NumericToFloat64(orderTotal),
				"customer": map[string]any{
					"id":    nullableInt64(orderCustID),
					"name":  nullableText(orderCustName),
					"phone": nullableText(orderCustPhone),
				},
			},
		}

		if voucherID.Valid {
			item["voucherCode"] = map[string]any{
				"id":   voucherID.Int64,
				"code": nullableText(voucherCode),
			}
		}
		if userID.Valid {
			item["appliedByUser"] = map[string]any{
				"id":    userID.Int64,
				"name":  nullableText(userName),
				"email": nullableText(userEmail),
			}
		}
		if customerID.Valid {
			item["appliedByCustomer"] = map[string]any{
				"id":    customerID.Int64,
				"name":  nullableText(customerName),
				"phone": nullableText(customerPhone),
			}
		}

		items = append(items, item)
	}

	hasMore := len(items) > take
	nextCursor := any(nil)
	if hasMore {
		items = items[:take]
		if len(items) > 0 {
			nextCursor = items[len(items)-1]["id"]
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"items":      items,
			"nextCursor": nextCursor,
		},
	})
}

func (h *Handler) ensureVoucherTemplate(ctx context.Context, templateID int64, merchantID int64, w http.ResponseWriter) bool {
	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from order_voucher_templates where id = $1 and merchant_id = $2)", templateID, merchantID).Scan(&exists); err != nil {
		h.Logger.Error("order voucher template lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher template")
		return false
	}
	if !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Template not found")
		return false
	}
	return true
}

func parseVoucherTemplateID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	idParam := chi.URLParam(r, "id")
	id, err := parseStringToInt64(idParam)
	if err != nil || id <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid template id")
		return 0, false
	}
	return id, true
}

func parseVoucherCodeID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	idParam := chi.URLParam(r, "codeId")
	id, err := parseStringToInt64(idParam)
	if err != nil || id <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid code id")
		return 0, false
	}
	return id, true
}

func (h *Handler) fetchVoucherMenuScopes(ctx context.Context, templateID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select mt.menu_id, m.name
        from order_voucher_template_menus mt
        join menus m on m.id = mt.menu_id
        where mt.template_id = $1
        order by mt.created_at asc
    `, templateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	scopes := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		scopes = append(scopes, map[string]any{
			"menuId": id,
			"menu": map[string]any{
				"id":   id,
				"name": name,
			},
		})
	}
	return scopes, nil
}

func (h *Handler) fetchVoucherCategoryScopes(ctx context.Context, templateID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select ct.category_id, c.name
        from order_voucher_template_categories ct
        join menu_categories c on c.id = ct.category_id
        where ct.template_id = $1
        order by ct.created_at asc
    `, templateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	scopes := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		scopes = append(scopes, map[string]any{
			"categoryId": id,
			"category": map[string]any{
				"id":   id,
				"name": name,
			},
		})
	}
	return scopes, nil
}

func readStringFieldRaw(payload map[string]json.RawMessage, key string) (string, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return "", false, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", true, err
	}
	return value, true, nil
}

func readNullableStringFieldRaw(payload map[string]json.RawMessage, key string) (*string, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, true, err
	}
	if value == nil {
		return nil, true, nil
	}
	trimmed := strings.TrimSpace(*value)
	return &trimmed, true, nil
}

func readBoolFieldRaw(payload map[string]json.RawMessage, key string) (bool, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return false, false, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, true, err
	}
	return value, true, nil
}

func readFloatFieldRaw(payload map[string]json.RawMessage, key string) (float64, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return 0, false, nil
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, true, err
	}
	return value, true, nil
}

func readNullableFloatFieldRaw(payload map[string]json.RawMessage, key string) (*float64, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var value *float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, true, err
	}
	return value, true, nil
}

func readNullableIntFieldRaw(payload map[string]json.RawMessage, key string) (*int, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var value *float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, true, err
	}
	if value == nil {
		return nil, true, nil
	}
	intValue := int(*value)
	return &intValue, true, nil
}

func readStringArrayFieldRaw(payload map[string]json.RawMessage, key string) ([]string, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, true, err
	}
	return values, true, nil
}

func readIntArrayFieldRaw(payload map[string]json.RawMessage, key string) ([]int, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var values []int
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, true, err
	}
	return values, true, nil
}

func readNullableTimeFieldRaw(payload map[string]json.RawMessage, key string) (*time.Time, bool, error) {
	raw, ok := payload[key]
	if !ok {
		return nil, false, nil
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, true, err
	}
	if value == nil {
		return nil, true, nil
	}
	parsed, okParse := parseOptionalTime(value)
	if !okParse {
		return nil, true, fmt.Errorf("invalid time")
	}
	return parsed, true, nil
}

func parseRawArray(raw json.RawMessage) []any {
	if len(raw) == 0 {
		return nil
	}
	var value []any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}

func parseQueryInt(r *http.Request, key string, fallback int) int {
	raw := r.URL.Query().Get(key)
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := parseStringToInt(raw)
	if err != nil {
		return fallback
	}
	return value
}

func parseQueryInt64(value string) *int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	parsed, err := parseStringToInt64(trimmed)
	if err != nil {
		return nil
	}
	return &parsed
}

func parseMapInt(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return fallback
	}
}

func clampInt(value int, min int, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func normalizeManualCodes(values ...any) []string {
	raw := make([]string, 0)
	for _, value := range values {
		switch v := value.(type) {
		case []any:
			for _, entry := range v {
				if s, ok := entry.(string); ok {
					raw = append(raw, s)
				}
			}
		case []string:
			raw = append(raw, v...)
		case string:
			raw = append(raw, v)
		}
	}

	exploded := make([]string, 0)
	for _, item := range raw {
		parts := strings.FieldsFunc(item, func(r rune) bool {
			return r == ' ' || r == ',' || r == ';' || r == '\n' || r == '\t'
		})
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				exploded = append(exploded, trimmed)
			}
		}
	}

	cleaned := make([]string, 0)
	seen := make(map[string]struct{})
	for _, item := range exploded {
		value := strings.ToUpper(strings.TrimSpace(item))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		cleaned = append(cleaned, value)
	}
	return cleaned
}

func findInvalidCode(codes []string) string {
	for _, code := range codes {
		if len(code) < 3 || len(code) > 32 {
			return code
		}
		if !voucherCodeRegex.MatchString(code) {
			return code
		}
	}
	return ""
}

func generateVoucherCode(length int) string {
	bytes := make([]byte, length)
	_, _ = rand.Read(bytes)
	out := strings.Builder{}
	out.Grow(length)
	for i := 0; i < length; i++ {
		out.WriteByte(voucherCodeAlphabet[int(bytes[i])%len(voucherCodeAlphabet)])
	}
	return out.String()
}

func fetchExistingVoucherCodes(ctx context.Context, db *pgxpool.Pool, merchantID int64, codes []string) (map[string]struct{}, error) {
	if len(codes) == 0 {
		return map[string]struct{}{}, nil
	}
	rows, err := db.Query(ctx, "select code from order_voucher_codes where merchant_id = $1 and code = any($2)", merchantID, codes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := make(map[string]struct{})
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, err
		}
		set[code] = struct{}{}
	}
	return set, nil
}

func fetchVoucherCodesByValue(ctx context.Context, db *pgxpool.Pool, merchantID int64, templateID int64, codes []string) ([]map[string]any, error) {
	if len(codes) == 0 {
		return []map[string]any{}, nil
	}
	rows, err := db.Query(ctx, `
        select c.id, c.code, c.is_active, c.created_at, c.valid_from, c.valid_until,
               (select count(*) from order_discounts od where od.voucher_code_id = c.id) as used_count
        from order_voucher_codes c
        where c.merchant_id = $1 and c.template_id = $2 and c.code = any($3)
        order by c.created_at desc
    `, merchantID, templateID, codes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id         int64
			code       string
			isActive   bool
			createdAt  time.Time
			validFrom  pgtype.Timestamptz
			validUntil pgtype.Timestamptz
			usedCount  int64
		)
		if err := rows.Scan(&id, &code, &isActive, &createdAt, &validFrom, &validUntil, &usedCount); err != nil {
			return nil, err
		}
		entry := map[string]any{
			"id":        id,
			"code":      code,
			"isActive":  isActive,
			"createdAt": createdAt,
			"usedCount": usedCount,
		}
		if validFrom.Valid {
			entry["validFrom"] = validFrom.Time
		}
		if validUntil.Valid {
			entry["validUntil"] = validUntil.Time
		}
		result = append(result, entry)
	}
	return result, nil
}

func fetchVoucherCode(ctx context.Context, db *pgxpool.Pool, merchantID int64, templateID int64, codeID int64) (map[string]any, error) {
	var (
		id         int64
		code       string
		isActive   bool
		createdAt  time.Time
		validFrom  pgtype.Timestamptz
		validUntil pgtype.Timestamptz
		usedCount  int64
	)
	query := `
        select c.id, c.code, c.is_active, c.created_at, c.valid_from, c.valid_until,
               (select count(*) from order_discounts od where od.voucher_code_id = c.id) as used_count
        from order_voucher_codes c
        where c.merchant_id = $1 and c.template_id = $2 and c.id = $3
    `
	if err := db.QueryRow(ctx, query, merchantID, templateID, codeID).Scan(&id, &code, &isActive, &createdAt, &validFrom, &validUntil, &usedCount); err != nil {
		return nil, err
	}
	entry := map[string]any{
		"id":        id,
		"code":      code,
		"isActive":  isActive,
		"createdAt": createdAt,
		"usedCount": usedCount,
	}
	if validFrom.Valid {
		entry["validFrom"] = validFrom.Time
	}
	if validUntil.Valid {
		entry["validUntil"] = validUntil.Time
	}
	return entry, nil
}

func nullableInt64(value pgtype.Int8) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}
