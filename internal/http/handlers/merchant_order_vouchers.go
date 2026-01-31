package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type orderVoucherTemplateRequest struct {
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	Audience              string   `json:"audience"`
	DiscountType          string   `json:"discountType"`
	DiscountValue         float64  `json:"discountValue"`
	MaxDiscountAmount     *float64 `json:"maxDiscountAmount"`
	MinOrderAmount        *float64 `json:"minOrderAmount"`
	MaxUsesTotal          *int     `json:"maxUsesTotal"`
	MaxUsesPerCustomer    *int     `json:"maxUsesPerCustomer"`
	MaxUsesPerOrder       *int     `json:"maxUsesPerOrder"`
	TotalDiscountCap      *float64 `json:"totalDiscountCap"`
	AllowedOrderTypes     []string `json:"allowedOrderTypes"`
	DaysOfWeek            []int    `json:"daysOfWeek"`
	StartTime             *string  `json:"startTime"`
	EndTime               *string  `json:"endTime"`
	IncludeAllItems       *bool    `json:"includeAllItems"`
	ScopedMenuIds         []any    `json:"scopedMenuIds"`
	ScopedCategoryIds     []any    `json:"scopedCategoryIds"`
	ReportCategory        *string  `json:"reportCategory"`
	ValidFrom             *string  `json:"validFrom"`
	ValidUntil            *string  `json:"validUntil"`
	IsActive              *bool    `json:"isActive"`
	RequiresCustomerLogin *bool    `json:"requiresCustomerLogin"`
}

type orderVoucherSettingsRequest struct {
	CustomerVouchersEnabled *bool `json:"customerVouchersEnabled"`
	PosDiscountsEnabled     *bool `json:"posDiscountsEnabled"`
}

func (h *Handler) MerchantOrderVoucherTemplatesList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select t.id, t.name, t.description, t.audience, t.discount_type, t.discount_value,
               t.max_discount_amount, t.min_order_amount, t.max_uses_total, t.max_uses_per_customer,
               t.max_uses_per_order, t.total_discount_cap, t.allowed_order_types, t.days_of_week,
               t.start_time, t.end_time, t.include_all_items, t.report_category, t.valid_from, t.valid_until,
               t.is_active, t.requires_customer_login, t.created_at, t.updated_at,
               (select count(*) from order_voucher_codes c where c.template_id = t.id) as codes_count,
               (select count(*) from order_discounts d where d.voucher_template_id = t.id) as discounts_count
        from order_voucher_templates t
        where t.merchant_id = $1
        order by t.created_at desc
    `, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("order voucher templates query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher templates")
		return
	}
	defer rows.Close()

	templates := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id              int64
			name            string
			description     pgtype.Text
			audience        string
			discountType    string
			discountValue   pgtype.Numeric
			maxDiscount     pgtype.Numeric
			minOrder        pgtype.Numeric
			maxUsesTotal    pgtype.Int4
			maxUsesCustomer pgtype.Int4
			maxUsesOrder    int32
			totalCap        pgtype.Numeric
			allowedTypes    []string
			daysOfWeek      []int32
			startTime       pgtype.Text
			endTime         pgtype.Text
			includeAll      bool
			reportCategory  pgtype.Text
			validFrom       pgtype.Timestamptz
			validUntil      pgtype.Timestamptz
			isActive        bool
			requiresLogin   bool
			createdAt       time.Time
			updatedAt       time.Time
			codesCount      int64
			discountsCount  int64
		)
		if err := rows.Scan(&id, &name, &description, &audience, &discountType, &discountValue, &maxDiscount, &minOrder, &maxUsesTotal, &maxUsesCustomer, &maxUsesOrder, &totalCap, &allowedTypes, &daysOfWeek, &startTime, &endTime, &includeAll, &reportCategory, &validFrom, &validUntil, &isActive, &requiresLogin, &createdAt, &updatedAt, &codesCount, &discountsCount); err != nil {
			h.Logger.Error("order voucher templates scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher templates")
			return
		}

		template := map[string]any{
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
			"_count": map[string]any{
				"codes":          codesCount,
				"orderDiscounts": discountsCount,
			},
		}

		if description.Valid {
			template["description"] = description.String
		}
		if maxDiscount.Valid {
			template["maxDiscountAmount"] = utils.NumericToFloat64(maxDiscount)
		}
		if minOrder.Valid {
			template["minOrderAmount"] = utils.NumericToFloat64(minOrder)
		}
		if maxUsesTotal.Valid {
			template["maxUsesTotal"] = maxUsesTotal.Int32
		}
		if maxUsesCustomer.Valid {
			template["maxUsesPerCustomer"] = maxUsesCustomer.Int32
		}
		template["maxUsesPerOrder"] = maxUsesOrder
		if totalCap.Valid {
			template["totalDiscountCap"] = utils.NumericToFloat64(totalCap)
		}
		if allowedTypes != nil {
			items := make([]string, len(allowedTypes))
			copy(items, allowedTypes)
			template["allowedOrderTypes"] = items
		}
		if daysOfWeek != nil {
			items := make([]int32, len(daysOfWeek))
			copy(items, daysOfWeek)
			template["daysOfWeek"] = items
		}
		if startTime.Valid {
			template["startTime"] = startTime.String
		}
		if endTime.Valid {
			template["endTime"] = endTime.String
		}
		if reportCategory.Valid {
			template["reportCategory"] = reportCategory.String
		}
		if validFrom.Valid {
			template["validFrom"] = validFrom.Time
		}
		if validUntil.Valid {
			template["validUntil"] = validUntil.Time
		}

		templates = append(templates, template)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    templates,
	})
}

func (h *Handler) MerchantOrderVoucherTemplatesCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body orderVoucherTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(body.Name)
	audience := strings.ToUpper(strings.TrimSpace(body.Audience))
	discountType := strings.ToUpper(strings.TrimSpace(body.DiscountType))
	if name == "" || audience == "" || discountType == "" || !isFiniteFloat(body.DiscountValue) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Missing or invalid required fields")
		return
	}
	if body.DiscountValue <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Discount value must be greater than 0")
		return
	}
	if discountType == "PERCENTAGE" && body.DiscountValue > 100 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Percentage discount cannot exceed 100")
		return
	}

	includeAll := true
	if body.IncludeAllItems != nil {
		includeAll = *body.IncludeAllItems
	}

	startTime, startValid := normalizeTime(body.StartTime)
	endTime, endValid := normalizeTime(body.EndTime)
	if !startValid || !endValid {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "startTime or endTime is invalid")
		return
	}
	if (startTime != nil && endTime == nil) || (startTime == nil && endTime != nil) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "startTime and endTime must both be provided")
		return
	}

	validFrom, validFromValid := parseOptionalTime(body.ValidFrom)
	validUntil, validUntilValid := parseOptionalTime(body.ValidUntil)
	if !validFromValid || !validUntilValid {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "validFrom or validUntil is invalid")
		return
	}

	menuIDs := parseIDArray(body.ScopedMenuIds)
	categoryIDs := parseIDArray(body.ScopedCategoryIds)

	isActive := true
	if body.IsActive != nil {
		isActive = *body.IsActive
	}
	requiresLogin := false
	if body.RequiresCustomerLogin != nil {
		requiresLogin = *body.RequiresCustomerLogin
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		h.Logger.Error("order voucher create begin failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher template")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var templateID int64
	if err := tx.QueryRow(ctx, `
        insert into order_voucher_templates (
            merchant_id, name, description, audience, discount_type, discount_value, max_discount_amount,
            min_order_amount, max_uses_total, max_uses_per_customer, max_uses_per_order, total_discount_cap,
            allowed_order_types, days_of_week, start_time, end_time, include_all_items, report_category,
            valid_from, valid_until, is_active, requires_customer_login
        ) values ($1, $2, $3, $4, $5, $6, $7,
                 $8, $9, $10, $11, $12,
                 $13, $14, $15, $16, $17, $18,
                 $19, $20, $21, $22)
        returning id
    `,
		*authCtx.MerchantID,
		name,
		nullableString(body.Description),
		audience,
		discountType,
		body.DiscountValue,
		body.MaxDiscountAmount,
		body.MinOrderAmount,
		body.MaxUsesTotal,
		body.MaxUsesPerCustomer,
		valueOrDefault(body.MaxUsesPerOrder, 1),
		body.TotalDiscountCap,
		cleanOrderTypes(body.AllowedOrderTypes),
		cleanDaysOfWeek(body.DaysOfWeek),
		startTime,
		endTime,
		includeAll,
		nullableString(ptrToString(body.ReportCategory)),
		validFrom,
		validUntil,
		isActive,
		requiresLogin,
	).Scan(&templateID); err != nil {
		h.Logger.Error("order voucher insert failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher template")
		return
	}

	if !includeAll {
		if err := insertVoucherScopes(ctx, tx, templateID, menuIDs, categoryIDs); err != nil {
			h.Logger.Error("order voucher scope insert failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher template")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("order voucher create commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create voucher template")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":            templateID,
			"name":          name,
			"audience":      audience,
			"discountType":  discountType,
			"discountValue": body.DiscountValue,
		},
		"message": "Voucher template created",
	})
}

func (h *Handler) MerchantOrderVoucherSettingsGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var features []byte
	if err := h.DB.QueryRow(ctx, "select features from merchants where id = $1", *authCtx.MerchantID).Scan(&features); err != nil {
		h.Logger.Error("order voucher settings lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve settings")
		return
	}

	posEnabled := readPosDiscountsEnabled(features)
	customerEnabled := readCustomerVouchersEnabled(features)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"customerVouchersEnabled": posEnabled && customerEnabled,
			"posDiscountsEnabled":     posEnabled,
		},
		"statusCode": 200,
	})
}

func (h *Handler) MerchantOrderVoucherSettingsUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body orderVoucherSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if body.CustomerVouchersEnabled == nil && body.PosDiscountsEnabled == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "At least one of customerVouchersEnabled or posDiscountsEnabled is required")
		return
	}

	var features []byte
	if err := h.DB.QueryRow(ctx, "select features from merchants where id = $1", *authCtx.MerchantID).Scan(&features); err != nil {
		h.Logger.Error("order voucher settings lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update settings")
		return
	}

	currentCustomerEnabled := readCustomerVouchersEnabled(features)
	currentPosEnabled := readPosDiscountsEnabled(features)
	intendedPosEnabled := currentPosEnabled
	if body.PosDiscountsEnabled != nil {
		intendedPosEnabled = *body.PosDiscountsEnabled
	}

	if body.CustomerVouchersEnabled != nil && *body.CustomerVouchersEnabled && !intendedPosEnabled {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "POS discounts must be enabled before enabling customer vouchers")
		return
	}

	updated := writeOrderVoucherFeatures(features, body.CustomerVouchersEnabled, body.PosDiscountsEnabled)
	if !intendedPosEnabled && currentCustomerEnabled {
		updated = writeOrderVoucherFeatures(updated, boolPtr(false), nil)
	}

	if _, err := h.DB.Exec(ctx, "update merchants set features = $1, updated_at = now() where id = $2", updated, *authCtx.MerchantID); err != nil {
		h.Logger.Error("order voucher settings update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update settings")
		return
	}

	posEnabled := intendedPosEnabled
	customerEnabled := currentCustomerEnabled
	if body.CustomerVouchersEnabled != nil {
		customerEnabled = *body.CustomerVouchersEnabled
	}
	if !posEnabled {
		customerEnabled = false
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"customerVouchersEnabled": posEnabled && customerEnabled,
			"posDiscountsEnabled":     posEnabled,
		},
		"statusCode": 200,
	})
}

func (h *Handler) MerchantOrderVoucherAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var timezone pgtype.Text
	if err := h.DB.QueryRow(ctx, "select timezone from merchants where id = $1", *authCtx.MerchantID).Scan(&timezone); err != nil {
		h.Logger.Error("order voucher analytics merchant lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
		return
	}

	period := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("period")))
	startDate := parseOptionalTimePtr(r.URL.Query().Get("startDate"))
	endDate := parseOptionalTimePtr(r.URL.Query().Get("endDate"))
	merchantTimezone := textValueOr(timezone, "Asia/Jakarta")

	start, end := resolveVoucherRange(period, startDate, endDate, merchantTimezone)

	var summaryCount int64
	var summarySum pgtype.Numeric
	summaryQuery := `
	    select count(*), coalesce(sum(od.discount_amount), 0)
        from order_discounts od
        join orders o on o.id = od.order_id
        where od.merchant_id = $1
          and od.voucher_template_id is not null
          and o.placed_at >= $2 and o.placed_at <= $3
    `
	if err := h.DB.QueryRow(ctx, summaryQuery, *authCtx.MerchantID, start, end).Scan(&summaryCount, &summarySum); err != nil {
		h.Logger.Error("order voucher summary query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
		return
	}

	bySource := make([]map[string]any, 0)
	rows, err := h.DB.Query(ctx, `
	    select source, count(*) as uses, coalesce(sum(od.discount_amount), 0) as total
        from order_discounts od
        join orders o on o.id = od.order_id
        where od.merchant_id = $1
          and od.voucher_template_id is not null
          and o.placed_at >= $2 and o.placed_at <= $3
        group by source
        order by total desc
    `, *authCtx.MerchantID, start, end)
	if err != nil {
		h.Logger.Error("order voucher by source failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
		return
	}
	for rows.Next() {
		var source string
		var uses int64
		var total pgtype.Numeric
		if err := rows.Scan(&source, &uses, &total); err != nil {
			rows.Close()
			h.Logger.Error("order voucher by source scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
			return
		}
		bySource = append(bySource, map[string]any{
			"source": source,
			"_count": map[string]any{"id": uses},
			"_sum":   map[string]any{"discountAmount": utils.NumericToFloat64(total)},
		})
	}
	rows.Close()

	templateRows := make([]struct {
		TemplateID int64
		Uses       int64
		Total      pgtype.Numeric
	}, 0)
	rows, err = h.DB.Query(ctx, `
	    select voucher_template_id, count(*) as uses, coalesce(sum(od.discount_amount), 0) as total
        from order_discounts od
        join orders o on o.id = od.order_id
        where od.merchant_id = $1
          and od.voucher_template_id is not null
          and o.placed_at >= $2 and o.placed_at <= $3
        group by voucher_template_id
        order by total desc
        limit 50
    `, *authCtx.MerchantID, start, end)
	if err != nil {
		h.Logger.Error("order voucher by template failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
		return
	}
	for rows.Next() {
		var id int64
		var uses int64
		var total pgtype.Numeric
		if err := rows.Scan(&id, &uses, &total); err != nil {
			rows.Close()
			h.Logger.Error("order voucher by template scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
			return
		}
		templateRows = append(templateRows, struct {
			TemplateID int64
			Uses       int64
			Total      pgtype.Numeric
		}{TemplateID: id, Uses: uses, Total: total})
	}
	rows.Close()

	templateInfo := make(map[int64]map[string]any)
	if len(templateRows) > 0 {
		ids := make([]int64, 0, len(templateRows))
		for _, row := range templateRows {
			ids = append(ids, row.TemplateID)
		}

		rows, err = h.DB.Query(ctx, `
            select id, name, audience, report_category
            from order_voucher_templates
            where merchant_id = $1 and id = any($2)
        `, *authCtx.MerchantID, ids)
		if err != nil {
			h.Logger.Error("order voucher templates fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
			return
		}
		for rows.Next() {
			var (
				id             int64
				name           string
				audience       string
				reportCategory pgtype.Text
			)
			if err := rows.Scan(&id, &name, &audience, &reportCategory); err != nil {
				rows.Close()
				h.Logger.Error("order voucher templates scan failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve voucher analytics")
				return
			}
			entry := map[string]any{
				"name":     name,
				"audience": audience,
			}
			if reportCategory.Valid {
				entry["reportCategory"] = reportCategory.String
			}
			templateInfo[id] = entry
		}
		rows.Close()
	}

	byTemplate := make([]map[string]any, 0)
	reportCategoryAgg := make(map[string]map[string]float64)
	for _, row := range templateRows {
		info := templateInfo[row.TemplateID]
		category := "uncategorized"
		if info != nil {
			if raw, ok := info["reportCategory"].(string); ok && raw != "" {
				category = raw
			}
		}
		byTemplate = append(byTemplate, map[string]any{
			"templateId":          row.TemplateID,
			"templateName":        safeMapString(info, "name"),
			"audience":            safeMapString(info, "audience"),
			"reportCategory":      safeMapString(info, "reportCategory"),
			"uses":                row.Uses,
			"totalDiscountAmount": utils.NumericToFloat64(row.Total),
		})

		entry := reportCategoryAgg[category]
		if entry == nil {
			entry = map[string]float64{"uses": 0, "total": 0}
		}
		entry["uses"] += float64(row.Uses)
		entry["total"] += utils.NumericToFloat64(row.Total)
		reportCategoryAgg[category] = entry
	}

	byReportCategory := make([]map[string]any, 0)
	for category, agg := range reportCategoryAgg {
		byReportCategory = append(byReportCategory, map[string]any{
			"reportCategory":      category,
			"uses":                int64(agg["uses"]),
			"totalDiscountAmount": agg["total"],
		})
	}
	sort.Slice(byReportCategory, func(i, j int) bool {
		return byReportCategory[i]["totalDiscountAmount"].(float64) > byReportCategory[j]["totalDiscountAmount"].(float64)
	})

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"summary": map[string]any{
				"uses":                summaryCount,
				"totalDiscountAmount": utils.NumericToFloat64(summarySum),
			},
			"bySource":         bySource,
			"byTemplate":       byTemplate,
			"byReportCategory": byReportCategory,
			"meta": map[string]any{
				"period":    period,
				"timezone":  merchantTimezone,
				"startDate": start.Format(time.RFC3339),
				"endDate":   end.Format(time.RFC3339),
			},
		},
	})
}

func readPosDiscountsEnabled(features []byte) bool {
	obj := parseJSONMap(features)
	orderVouchers, ok := obj["orderVouchers"].(map[string]any)
	if !ok {
		return false
	}
	value, ok := orderVouchers["posDiscountsEnabled"]
	if !ok {
		return false
	}
	return value == true
}

func readCustomerVouchersEnabled(features []byte) bool {
	obj := parseJSONMap(features)
	orderVouchers, ok := obj["orderVouchers"].(map[string]any)
	if !ok {
		return false
	}
	value, ok := orderVouchers["customerEnabled"]
	if !ok {
		return false
	}
	return value == true
}

func writeOrderVoucherFeatures(features []byte, customerEnabled *bool, posEnabled *bool) []byte {
	obj := parseJSONMap(features)
	orderVouchers, ok := obj["orderVouchers"].(map[string]any)
	if !ok {
		orderVouchers = map[string]any{}
	}
	if customerEnabled != nil {
		orderVouchers["customerEnabled"] = *customerEnabled
	}
	if posEnabled != nil {
		orderVouchers["posDiscountsEnabled"] = *posEnabled
	}
	obj["orderVouchers"] = orderVouchers

	bytes, _ := json.Marshal(obj)
	return bytes
}

func parseJSONMap(features []byte) map[string]any {
	if len(features) == 0 {
		return map[string]any{}
	}
	var obj map[string]any
	if err := json.Unmarshal(features, &obj); err != nil {
		return map[string]any{}
	}
	return obj
}

func normalizeTime(value *string) (*string, bool) {
	if value == nil {
		return nil, true
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil, true
	}
	if !isValidTimeString(trimmed) {
		return nil, false
	}
	return &trimmed, true
}

func isValidTimeString(value string) bool {
	if len(value) != 5 {
		return false
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return false
	}
	hh, err := parseStringToInt(parts[0])
	if err != nil || hh < 0 || hh > 23 {
		return false
	}
	mm, err := parseStringToInt(parts[1])
	if err != nil || mm < 0 || mm > 59 {
		return false
	}
	return true
}

func parseOptionalTime(value *string) (*time.Time, bool) {
	if value == nil {
		return nil, true
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		parsed, err = time.Parse("2006-01-02", trimmed)
		if err != nil {
			return nil, false
		}
	}
	return &parsed, true
}

func parseOptionalTimePtr(value string) *time.Time {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		parsed, err = time.Parse("2006-01-02", value)
		if err != nil {
			return nil
		}
	}
	return &parsed
}

func resolveVoucherRange(period string, startDate *time.Time, endDate *time.Time, timezone string) (time.Time, time.Time) {
	location := resolveLocation(timezone)
	now := time.Now().In(location)
	if period == "custom" && startDate != nil && endDate != nil {
		return *startDate, *endDate
	}
	if period == "today" {
		start := startOfDayInTz(now, location)
		return start, now
	}
	if period == "week" {
		startToday := startOfDayInTz(now, location)
		start := startToday.AddDate(0, 0, -7)
		return start, now
	}
	if period == "year" {
		start := startOfYearInTz(now, location)
		return start, now
	}
	start := startOfMonthInTz(now, location)
	return start, now
}

func startOfDayInTz(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

func startOfMonthInTz(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location)
}

func startOfYearInTz(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), 1, 1, 0, 0, 0, 0, location)
}

func insertVoucherScopes(ctx context.Context, tx pgx.Tx, templateID int64, menuIDs []int64, categoryIDs []int64) error {
	for _, menuID := range menuIDs {
		if _, err := tx.Exec(ctx, `
            insert into order_voucher_template_menus (template_id, menu_id)
            values ($1, $2)
            on conflict do nothing
        `, templateID, menuID); err != nil {
			return err
		}
	}
	for _, categoryID := range categoryIDs {
		if _, err := tx.Exec(ctx, `
            insert into order_voucher_template_categories (template_id, category_id)
            values ($1, $2)
            on conflict do nothing
        `, templateID, categoryID); err != nil {
			return err
		}
	}
	return nil
}

func cleanOrderTypes(values []string) []string {
	allowed := map[string]struct{}{"DINE_IN": {}, "TAKEAWAY": {}, "DELIVERY": {}}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, raw := range values {
		value := strings.ToUpper(strings.TrimSpace(raw))
		if value == "" {
			continue
		}
		if _, ok := allowed[value]; !ok {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func cleanDaysOfWeek(values []int) []int32 {
	out := make([]int32, 0)
	seen := make(map[int32]struct{})
	for _, value := range values {
		if value < 0 || value > 6 {
			continue
		}
		candidate := int32(value)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out
}

func isFiniteFloat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func valueOrDefault(value *int, fallback int) int {
	if value == nil || *value <= 0 {
		return fallback
	}
	return *value
}

func ptrToString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func safeMapString(values map[string]any, key string) any {
	if values == nil {
		return nil
	}
	if value, ok := values[key]; ok {
		return value
	}
	return nil
}

func textValueOr(value pgtype.Text, fallback string) string {
	if value.Valid {
		return value.String
	}
	return fallback
}

func boolPtr(value bool) *bool {
	return &value
}
