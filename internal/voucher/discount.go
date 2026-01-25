package voucher

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Audience string

const (
	AudiencePOS Audience = "POS"
)

type DiscountType string

const (
	DiscountPercentage DiscountType = "PERCENTAGE"
	DiscountFixed      DiscountType = "FIXED_AMOUNT"
)

type OrderItemInput struct {
	MenuID   int64
	Subtotal float64
}

type DiscountResult struct {
	TemplateID       int64
	CodeID           *int64
	Label            string
	DiscountType     DiscountType
	DiscountValue    float64
	DiscountAmount   float64
	EligibleSubtotal float64
}

type voucherTemplate struct {
	ID                    int64
	Name                  string
	Audience              string
	DiscountType          DiscountType
	DiscountValue         float64
	MaxDiscountAmount     *float64
	MinOrderAmount        *float64
	MaxUsesTotal          *int32
	MaxUsesPerCustomer    *int32
	MaxUsesPerOrder       int32
	TotalDiscountCap      *float64
	RequiresCustomerLogin bool
	AllowedOrderTypes     []string
	ValidFrom             *time.Time
	ValidUntil            *time.Time
	DaysOfWeek            []int
	StartTime             *string
	EndTime               *string
	IncludeAllItems       bool
	ReportCategory        *string
	IsActive              bool
	MenuScopes            []int64
	CategoryScopes        []int64
}

type voucherCode struct {
	ID                 int64
	Code               string
	IsActive           bool
	MaxUsesTotal       *int32
	MaxUsesPerCustomer *int32
	ValidFrom          *time.Time
	ValidUntil         *time.Time
}

type ComputeParams struct {
	MerchantID              int64
	MerchantCurrency        string
	MerchantTimezone        string
	Audience                Audience
	OrderType               string
	Subtotal                float64
	Items                   []OrderItemInput
	VoucherCode             string
	VoucherTemplateID       *int64
	CustomerID              *int64
	OrderIDForStacking      *int64
	ExcludeOrderIDFromUsage *int64
}

func ComputeVoucherDiscount(ctx context.Context, db *pgxpool.Pool, params ComputeParams) (*DiscountResult, *Error) {
	resolvedTemplate, resolvedCode, err := resolveVoucher(ctx, db, params.MerchantID, params.Audience, params.VoucherCode, params.VoucherTemplateID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	if err := assertTimeAndChannel(resolvedTemplate, now, params.MerchantTimezone, params.OrderType); err != nil {
		return nil, err
	}

	if resolvedCode != nil {
		if !resolvedCode.IsActive {
			return nil, ValidationError(ErrVoucherInactive, "Voucher is inactive", nil)
		}
		if resolvedCode.ValidFrom != nil && now.Before(*resolvedCode.ValidFrom) {
			return nil, ValidationError(ErrVoucherNotActiveYet, "Voucher is not active yet", map[string]any{"validFrom": *resolvedCode.ValidFrom})
		}
		if resolvedCode.ValidUntil != nil && now.After(*resolvedCode.ValidUntil) {
			return nil, ValidationError(ErrVoucherExpired, "Voucher has expired", map[string]any{"validUntil": *resolvedCode.ValidUntil})
		}
	}

	if resolvedTemplate.RequiresCustomerLogin && params.CustomerID == nil {
		return nil, ValidationError(ErrVoucherRequiresLogin, "Customer login is required to use this voucher", nil)
	}

	if resolvedTemplate.MinOrderAmount != nil && params.Subtotal < *resolvedTemplate.MinOrderAmount {
		return nil, ValidationError(ErrVoucherMinOrderNotMet, "Order does not meet minimum amount", map[string]any{
			"minOrderAmount": *resolvedTemplate.MinOrderAmount,
			"subtotal":       params.Subtotal,
			"currency":       params.MerchantCurrency,
		})
	}

	if err := assertUsageLimits(ctx, db, params.MerchantID, resolvedTemplate, resolvedCode, params.CustomerID, params.ExcludeOrderIDFromUsage); err != nil {
		return nil, err
	}

	if params.OrderIDForStacking != nil {
		if err := assertStacking(ctx, db, params.MerchantID, *params.OrderIDForStacking); err != nil {
			return nil, err
		}
	}

	eligibleSubtotal, err := computeEligibleSubtotal(ctx, db, resolvedTemplate, params.Items)
	if err != nil {
		return nil, err
	}
	if eligibleSubtotal <= 0 {
		return nil, ValidationError(ErrVoucherNotApplicableItems, "Voucher is not applicable to selected items", map[string]any{
			"includeAllItems":  resolvedTemplate.IncludeAllItems,
			"scopedMenus":      len(resolvedTemplate.MenuScopes),
			"scopedCategories": len(resolvedTemplate.CategoryScopes),
		})
	}

	discountValue := resolvedTemplate.DiscountValue
	var discountAmount float64
	if resolvedTemplate.DiscountType == DiscountPercentage {
		pct := math.Max(0, math.Min(discountValue, 100))
		discountAmount = eligibleSubtotal * (pct / 100)
		if resolvedTemplate.MaxDiscountAmount != nil {
			discountAmount = math.Min(discountAmount, *resolvedTemplate.MaxDiscountAmount)
		}
	} else {
		discountAmount = math.Min(discountValue, eligibleSubtotal)
	}

	discountAmount = roundCurrency(math.Max(0, discountAmount), params.MerchantCurrency)
	eligibleSubtotal = roundCurrency(eligibleSubtotal, params.MerchantCurrency)

	if discountAmount <= 0 {
		return nil, ValidationError(ErrVoucherDiscountZero, "Voucher discount is zero", nil)
	}

	result := &DiscountResult{
		TemplateID:       resolvedTemplate.ID,
		Label:            resolvedTemplate.Name,
		DiscountType:     resolvedTemplate.DiscountType,
		DiscountValue:    discountValue,
		DiscountAmount:   discountAmount,
		EligibleSubtotal: eligibleSubtotal,
	}
	if resolvedCode != nil {
		codeID := resolvedCode.ID
		result.CodeID = &codeID
	}

	return result, nil
}

func resolveVoucher(ctx context.Context, db *pgxpool.Pool, merchantID int64, audience Audience, voucherCode string, voucherTemplateID *int64) (*voucherTemplate, *voucherCode, *Error) {
	code := strings.TrimSpace(strings.ToUpper(voucherCode))
	if code != "" {
		return resolveVoucherByCode(ctx, db, merchantID, audience, code)
	}

	if voucherTemplateID == nil {
		return nil, nil, ValidationError(ErrVoucherTemplateRequired, "Voucher template is required", nil)
	}

	tpl, err := loadVoucherTemplate(ctx, db, merchantID, *voucherTemplateID)
	if err != nil {
		return nil, nil, err
	}
	if !tpl.IsActive {
		return nil, nil, ValidationError(ErrVoucherInactive, "Voucher is inactive", nil)
	}
	if !audienceApplicable(tpl.Audience, audience) {
		return nil, nil, ValidationError(ErrVoucherNotApplicable, "Voucher is not applicable", nil)
	}

	return tpl, nil, nil
}

func resolveVoucherByCode(ctx context.Context, db *pgxpool.Pool, merchantID int64, audience Audience, code string) (*voucherTemplate, *voucherCode, *Error) {
	row := db.QueryRow(ctx, `
		select c.id, c.code, c.is_active, c.max_uses_total, c.max_uses_per_customer, c.valid_from, c.valid_until,
		       t.id, t.name, t.audience, t.discount_type, t.discount_value, t.max_discount_amount, t.min_order_amount,
		       t.max_uses_total, t.max_uses_per_customer, t.max_uses_per_order, t.total_discount_cap,
		       t.requires_customer_login, t.allowed_order_types, t.valid_from, t.valid_until, t.days_of_week,
		       t.start_time, t.end_time, t.include_all_items, t.report_category, t.is_active
		from order_voucher_codes c
		join order_voucher_templates t on t.id = c.template_id
		where c.merchant_id = $1 and c.code = $2
	`, merchantID, code)

	var (
		codeID             int64
		codeValue          string
		codeActive         bool
		codeMaxUses        pgtype.Int4
		codeMaxPerCustomer pgtype.Int4
		codeValidFrom      pgtype.Timestamptz
		codeValidUntil     pgtype.Timestamptz
		tplID              int64
		tplName            string
		tplAudience        string
		discountType       string
		discountValue      pgtype.Numeric
		maxDiscount        pgtype.Numeric
		minOrder           pgtype.Numeric
		maxUsesTotal       pgtype.Int4
		maxUsesPerCustomer pgtype.Int4
		maxUsesPerOrder    int32
		totalDiscountCap   pgtype.Numeric
		requiresLogin      bool
		allowedOrderTypes  []string
		tplValidFrom       pgtype.Timestamptz
		tplValidUntil      pgtype.Timestamptz
		daysOfWeek         []int32
		startTime          pgtype.Text
		endTime            pgtype.Text
		includeAll         bool
		reportCategory     pgtype.Text
		tplActive          bool
	)

	if err := row.Scan(
		&codeID, &codeValue, &codeActive, &codeMaxUses, &codeMaxPerCustomer, &codeValidFrom, &codeValidUntil,
		&tplID, &tplName, &tplAudience, &discountType, &discountValue, &maxDiscount, &minOrder,
		&maxUsesTotal, &maxUsesPerCustomer, &maxUsesPerOrder, &totalDiscountCap,
		&requiresLogin, &allowedOrderTypes, &tplValidFrom, &tplValidUntil, &daysOfWeek,
		&startTime, &endTime, &includeAll, &reportCategory, &tplActive,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil, ValidationError(ErrVoucherNotFound, "Invalid voucher code", nil)
		}
		return nil, nil, ValidationError(ErrVoucherNotFound, "Invalid voucher code", nil)
	}

	menuScopes, categoryScopes, err := loadVoucherScopes(ctx, db, tplID)
	if err != nil {
		return nil, nil, err
	}

	if !tplActive {
		return nil, nil, ValidationError(ErrVoucherInactive, "Voucher is inactive", nil)
	}
	if !audienceApplicable(tplAudience, audience) {
		return nil, nil, ValidationError(ErrVoucherNotApplicable, "Voucher is not applicable", nil)
	}

	return &voucherTemplate{
			ID:                    tplID,
			Name:                  tplName,
			Audience:              tplAudience,
			DiscountType:          DiscountType(discountType),
			DiscountValue:         numericToFloat(discountValue),
			MaxDiscountAmount:     optionalNumeric(maxDiscount),
			MinOrderAmount:        optionalNumeric(minOrder),
			MaxUsesTotal:          optionalInt(codeOrTemplateMax(codeMaxUses, maxUsesTotal)),
			MaxUsesPerCustomer:    optionalInt(maxUsesPerCustomer),
			MaxUsesPerOrder:       maxUsesPerOrder,
			TotalDiscountCap:      optionalNumeric(totalDiscountCap),
			RequiresCustomerLogin: requiresLogin,
			AllowedOrderTypes:     textArrayToSlice(allowedOrderTypes),
			ValidFrom:             optionalTime(tplValidFrom),
			ValidUntil:            optionalTime(tplValidUntil),
			DaysOfWeek:            intArrayToSlice(daysOfWeek),
			StartTime:             optionalText(startTime),
			EndTime:               optionalText(endTime),
			IncludeAllItems:       includeAll,
			ReportCategory:        optionalText(reportCategory),
			IsActive:              tplActive,
			MenuScopes:            menuScopes,
			CategoryScopes:        categoryScopes,
		}, &voucherCode{
			ID:                 codeID,
			Code:               codeValue,
			IsActive:           codeActive,
			MaxUsesTotal:       optionalInt(codeMaxUses),
			MaxUsesPerCustomer: optionalInt(codeMaxPerCustomer),
			ValidFrom:          optionalTime(codeValidFrom),
			ValidUntil:         optionalTime(codeValidUntil),
		}, nil
}

func loadVoucherTemplate(ctx context.Context, db *pgxpool.Pool, merchantID int64, templateID int64) (*voucherTemplate, *Error) {
	row := db.QueryRow(ctx, `
		select id, name, audience, discount_type, discount_value, max_discount_amount, min_order_amount,
		       max_uses_total, max_uses_per_customer, max_uses_per_order, total_discount_cap,
		       requires_customer_login, allowed_order_types, valid_from, valid_until, days_of_week,
		       start_time, end_time, include_all_items, report_category, is_active
		from order_voucher_templates
		where merchant_id = $1 and id = $2
	`, merchantID, templateID)

	var (
		id                 int64
		name               string
		audience           string
		discountType       string
		discountValue      pgtype.Numeric
		maxDiscount        pgtype.Numeric
		minOrder           pgtype.Numeric
		maxUsesTotal       pgtype.Int4
		maxUsesPerCustomer pgtype.Int4
		maxUsesPerOrder    int32
		totalDiscountCap   pgtype.Numeric
		requiresLogin      bool
		allowedOrderTypes  []string
		validFrom          pgtype.Timestamptz
		validUntil         pgtype.Timestamptz
		daysOfWeek         []int32
		startTime          pgtype.Text
		endTime            pgtype.Text
		includeAll         bool
		reportCategory     pgtype.Text
		isActive           bool
	)

	if err := row.Scan(
		&id, &name, &audience, &discountType, &discountValue, &maxDiscount, &minOrder,
		&maxUsesTotal, &maxUsesPerCustomer, &maxUsesPerOrder, &totalDiscountCap,
		&requiresLogin, &allowedOrderTypes, &validFrom, &validUntil, &daysOfWeek,
		&startTime, &endTime, &includeAll, &reportCategory, &isActive,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
		}
		return nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
	}

	menuScopes, categoryScopes, err := loadVoucherScopes(ctx, db, id)
	if err != nil {
		return nil, err
	}

	return &voucherTemplate{
		ID:                    id,
		Name:                  name,
		Audience:              audience,
		DiscountType:          DiscountType(discountType),
		DiscountValue:         numericToFloat(discountValue),
		MaxDiscountAmount:     optionalNumeric(maxDiscount),
		MinOrderAmount:        optionalNumeric(minOrder),
		MaxUsesTotal:          optionalInt(maxUsesTotal),
		MaxUsesPerCustomer:    optionalInt(maxUsesPerCustomer),
		MaxUsesPerOrder:       maxUsesPerOrder,
		TotalDiscountCap:      optionalNumeric(totalDiscountCap),
		RequiresCustomerLogin: requiresLogin,
		AllowedOrderTypes:     textArrayToSlice(allowedOrderTypes),
		ValidFrom:             optionalTime(validFrom),
		ValidUntil:            optionalTime(validUntil),
		DaysOfWeek:            intArrayToSlice(daysOfWeek),
		StartTime:             optionalText(startTime),
		EndTime:               optionalText(endTime),
		IncludeAllItems:       includeAll,
		ReportCategory:        optionalText(reportCategory),
		IsActive:              isActive,
		MenuScopes:            menuScopes,
		CategoryScopes:        categoryScopes,
	}, nil
}

func loadVoucherScopes(ctx context.Context, db *pgxpool.Pool, templateID int64) ([]int64, []int64, *Error) {
	menuRows, err := db.Query(ctx, `select menu_id from order_voucher_template_menus where template_id = $1`, templateID)
	if err != nil {
		return nil, nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
	}
	defer menuRows.Close()

	menuScopes := make([]int64, 0)
	for menuRows.Next() {
		var menuID int64
		if err := menuRows.Scan(&menuID); err != nil {
			return nil, nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
		}
		menuScopes = append(menuScopes, menuID)
	}

	catRows, err := db.Query(ctx, `select category_id from order_voucher_template_categories where template_id = $1`, templateID)
	if err != nil {
		return nil, nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
	}
	defer catRows.Close()

	categoryScopes := make([]int64, 0)
	for catRows.Next() {
		var categoryID int64
		if err := catRows.Scan(&categoryID); err != nil {
			return nil, nil, ValidationError(ErrVoucherNotFound, "Voucher template not found", nil)
		}
		categoryScopes = append(categoryScopes, categoryID)
	}

	return menuScopes, categoryScopes, nil
}

func assertTimeAndChannel(template *voucherTemplate, now time.Time, merchantTimezone string, orderType string) *Error {
	if !template.IsActive {
		return ValidationError(ErrVoucherInactive, "Voucher is inactive", nil)
	}
	if len(template.AllowedOrderTypes) > 0 {
		allowed := false
		for _, t := range template.AllowedOrderTypes {
			if t == orderType {
				allowed = true
				break
			}
		}
		if !allowed {
			return ValidationError(ErrVoucherOrderTypeNotAllowed, "Voucher is not applicable for this order type", map[string]any{
				"orderType":         orderType,
				"allowedOrderTypes": template.AllowedOrderTypes,
			})
		}
	}

	if template.ValidFrom != nil && now.Before(*template.ValidFrom) {
		return ValidationError(ErrVoucherNotActiveYet, "Voucher is not active yet", map[string]any{"validFrom": *template.ValidFrom})
	}
	if template.ValidUntil != nil && now.After(*template.ValidUntil) {
		return ValidationError(ErrVoucherExpired, "Voucher has expired", map[string]any{"validUntil": *template.ValidUntil})
	}

	if len(template.DaysOfWeek) > 0 {
		loc := loadLocation(merchantTimezone)
		nowLocal := now.In(loc)
		dow := int(nowLocal.Weekday())
		allowed := false
		for _, d := range template.DaysOfWeek {
			if d == dow {
				allowed = true
				break
			}
		}
		if !allowed {
			return ValidationError(ErrVoucherNotAvailableToday, "Voucher is not available today", map[string]any{
				"daysOfWeek": template.DaysOfWeek,
				"today":      dow,
			})
		}
	}

	if template.StartTime != nil && template.EndTime != nil {
		if !isValidHHMM(*template.StartTime) || !isValidHHMM(*template.EndTime) {
			return ValidationError(ErrVoucherScheduleInvalid, "Voucher schedule is invalid", nil)
		}
		loc := loadLocation(merchantTimezone)
		nowLocal := now.In(loc)
		nowHHMM := nowLocal.Format("15:04")
		if !isTimeWithinWindow(nowHHMM, *template.StartTime, *template.EndTime) {
			return ValidationError(ErrVoucherNotAvailableNow, "Voucher is not available at this time", map[string]any{
				"startTime": *template.StartTime,
				"endTime":   *template.EndTime,
				"now":       nowHHMM,
			})
		}
	}

	return nil
}

func assertUsageLimits(ctx context.Context, db *pgxpool.Pool, merchantID int64, template *voucherTemplate, code *voucherCode, customerID *int64, excludeOrderID *int64) *Error {
	if template.MaxUsesTotal != nil {
		used, err := countOrderDiscounts(ctx, db, "voucher_template_id", template.ID, merchantID, excludeOrderID, nil)
		if err != nil {
			return err
		}
		if int64(used) >= int64(*template.MaxUsesTotal) {
			return ValidationError(ErrVoucherUsageLimitReached, "Voucher usage limit reached", map[string]any{
				"maxUsesTotal": *template.MaxUsesTotal,
				"used":         used,
			})
		}
	}

	if code != nil && code.MaxUsesTotal != nil {
		used, err := countOrderDiscounts(ctx, db, "voucher_code_id", code.ID, merchantID, excludeOrderID, nil)
		if err != nil {
			return err
		}
		if int64(used) >= int64(*code.MaxUsesTotal) {
			return ValidationError(ErrVoucherUsageLimitReached, "Voucher usage limit reached", map[string]any{
				"maxUsesTotal": *code.MaxUsesTotal,
				"used":         used,
			})
		}
	}

	if customerID != nil && template.MaxUsesPerCustomer != nil {
		used, err := countOrderDiscounts(ctx, db, "voucher_template_id", template.ID, merchantID, excludeOrderID, customerID)
		if err != nil {
			return err
		}
		if int64(used) >= int64(*template.MaxUsesPerCustomer) {
			return ValidationError(ErrVoucherUsageLimitReached, "Voucher usage limit reached", map[string]any{
				"maxUsesPerCustomer": *template.MaxUsesPerCustomer,
				"used":               used,
			})
		}
	}

	if customerID != nil && code != nil && code.MaxUsesPerCustomer != nil {
		used, err := countOrderDiscounts(ctx, db, "voucher_code_id", code.ID, merchantID, excludeOrderID, customerID)
		if err != nil {
			return err
		}
		if int64(used) >= int64(*code.MaxUsesPerCustomer) {
			return ValidationError(ErrVoucherUsageLimitReached, "Voucher usage limit reached", map[string]any{
				"maxUsesPerCustomer": *code.MaxUsesPerCustomer,
				"used":               used,
			})
		}
	}

	if template.TotalDiscountCap != nil {
		used, err := sumOrderDiscounts(ctx, db, template.ID, merchantID, excludeOrderID)
		if err != nil {
			return err
		}
		if used >= *template.TotalDiscountCap {
			return ValidationError(ErrVoucherDiscountCapReached, "Voucher discount budget reached", map[string]any{
				"totalDiscountCap": *template.TotalDiscountCap,
				"used":             used,
			})
		}
	}

	return nil
}

func assertStacking(ctx context.Context, db *pgxpool.Pool, merchantID int64, orderID int64) *Error {
	rows, err := db.Query(ctx, `
		select source, label
		from order_discounts
		where merchant_id = $1 and order_id = $2
		order by created_at asc
	`, merchantID, orderID)
	if err != nil {
		return ValidationError(ErrVoucherAlreadyApplied, "Only one voucher can be used per order", nil)
	}
	defer rows.Close()

	for rows.Next() {
		var source string
		var label string
		if err := rows.Scan(&source, &label); err != nil {
			return ValidationError(ErrVoucherAlreadyApplied, "Only one voucher can be used per order", nil)
		}
		if source == "MANUAL" {
			return ValidationError(ErrVoucherCannotStackManual, "Voucher cannot be combined with manual discount", map[string]any{
				"existingSource": source,
				"existingLabel":  label,
			})
		}
		if source == "POS_VOUCHER" || source == "CUSTOMER_VOUCHER" {
			return ValidationError(ErrVoucherAlreadyApplied, "Only one voucher can be used per order", map[string]any{
				"existingSource": source,
				"existingLabel":  label,
			})
		}
	}

	return nil
}

func computeEligibleSubtotal(ctx context.Context, db *pgxpool.Pool, template *voucherTemplate, items []OrderItemInput) (float64, *Error) {
	if template.IncludeAllItems {
		var sum float64
		for _, item := range items {
			sum += item.Subtotal
		}
		return sum, nil
	}

	if len(template.MenuScopes) == 0 && len(template.CategoryScopes) == 0 {
		return 0, nil
	}

	menuScope := make(map[int64]struct{})
	for _, id := range template.MenuScopes {
		menuScope[id] = struct{}{}
	}

	menuIDs := make([]int64, 0, len(items))
	for _, item := range items {
		menuIDs = append(menuIDs, item.MenuID)
	}

	categoryMenu := make(map[int64]struct{})
	if len(template.CategoryScopes) > 0 && len(menuIDs) > 0 {
		rows, err := db.Query(ctx, `
			select menu_id
			from menu_category_items
			where menu_id = any($1) and category_id = any($2)
		`, menuIDs, template.CategoryScopes)
		if err != nil {
			return 0, ValidationError(ErrVoucherNotApplicableItems, "Voucher is not applicable to selected items", nil)
		}
		defer rows.Close()
		for rows.Next() {
			var menuID int64
			if err := rows.Scan(&menuID); err != nil {
				return 0, ValidationError(ErrVoucherNotApplicableItems, "Voucher is not applicable to selected items", nil)
			}
			categoryMenu[menuID] = struct{}{}
		}
	}

	var eligible float64
	for _, item := range items {
		_, inMenu := menuScope[item.MenuID]
		_, inCategory := categoryMenu[item.MenuID]
		if inMenu || inCategory {
			eligible += item.Subtotal
		}
	}

	return eligible, nil
}

func audienceApplicable(templateAudience string, requestAudience Audience) bool {
	if strings.EqualFold(templateAudience, "BOTH") {
		return true
	}
	return strings.EqualFold(templateAudience, string(requestAudience))
}

func isValidHHMM(value string) bool {
	if len(value) != 5 || value[2] != ':' {
		return false
	}
	_, err := time.Parse("15:04", value)
	return err == nil
}

func isTimeWithinWindow(nowHHMM string, startHHMM string, endHHMM string) bool {
	if startHHMM == endHHMM {
		return true
	}
	if startHHMM < endHHMM {
		return nowHHMM >= startHHMM && nowHHMM <= endHHMM
	}
	return nowHHMM >= startHHMM || nowHHMM <= endHHMM
}

func loadLocation(tz string) *time.Location {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Local
	}
	return loc
}

func roundCurrency(amount float64, currency string) float64 {
	decimals := 2
	if strings.EqualFold(currency, "IDR") {
		decimals = 0
	}
	factor := math.Pow(10, float64(decimals))
	return math.Round((amount+math.SmallestNonzeroFloat64)*factor) / factor
}

func numericToFloat(value pgtype.Numeric) float64 {
	if !value.Valid {
		return 0
	}
	f, _ := value.Float64Value()
	return f.Float64
}

func optionalNumeric(value pgtype.Numeric) *float64 {
	if !value.Valid {
		return nil
	}
	f := numericToFloat(value)
	return &f
}

func optionalInt(value pgtype.Int4) *int32 {
	if !value.Valid {
		return nil
	}
	return &value.Int32
}

func optionalTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}

func optionalText(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func textArrayToSlice(value []string) []string {
	if len(value) == 0 {
		return nil
	}
	return value
}

func intArrayToSlice(value []int32) []int {
	if len(value) == 0 {
		return nil
	}
	result := make([]int, 0, len(value))
	for _, el := range value {
		result = append(result, int(el))
	}
	return result
}

func countOrderDiscounts(ctx context.Context, db *pgxpool.Pool, column string, id int64, merchantID int64, excludeOrderID *int64, customerID *int64) (int64, *Error) {
	query := "select count(*) from order_discounts where merchant_id = $1 and " + column + " = $2"
	args := []any{merchantID, id}
	if excludeOrderID != nil {
		query += " and order_id <> $3"
		args = append(args, *excludeOrderID)
		if customerID != nil {
			query += " and applied_by_customer_id = $4"
			args = append(args, *customerID)
		}
	} else if customerID != nil {
		query += " and applied_by_customer_id = $3"
		args = append(args, *customerID)
	}

	var count int64
	if err := db.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, ValidationError(ErrVoucherUsageLimitReached, "Voucher usage limit reached", nil)
	}
	return count, nil
}

func sumOrderDiscounts(ctx context.Context, db *pgxpool.Pool, templateID int64, merchantID int64, excludeOrderID *int64) (float64, *Error) {
	query := "select coalesce(sum(discount_amount), 0) from order_discounts where merchant_id = $1 and voucher_template_id = $2"
	args := []any{merchantID, templateID}
	if excludeOrderID != nil {
		query += " and order_id <> $3"
		args = append(args, *excludeOrderID)
	}
	var total pgtype.Numeric
	if err := db.QueryRow(ctx, query, args...).Scan(&total); err != nil {
		return 0, ValidationError(ErrVoucherDiscountCapReached, "Voucher discount budget reached", nil)
	}
	return numericToFloat(total), nil
}

func codeOrTemplateMax(codeMax pgtype.Int4, templateMax pgtype.Int4) pgtype.Int4 {
	if codeMax.Valid {
		return codeMax
	}
	return templateMax
}
