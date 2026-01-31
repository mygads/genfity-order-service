package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

const posCustomPlaceholderMenuNameAnalytics = "[POS] __CUSTOM_ITEM_PLACEHOLDER__"

func (h *Handler) MerchantSalesAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_REQUIRED", "Merchant context required")
		return
	}

	query := r.URL.Query()
	period := defaultString(query.Get("period"), "month")
	scope := defaultString(query.Get("scope"), "merchant")
	includeGroup := scope == "group"
	customStart := strings.TrimSpace(query.Get("startDate"))
	customEnd := strings.TrimSpace(query.Get("endDate"))
	scheduledOnly := query.Get("scheduledOnly") == "true"

	orderTypeFilters := splitQueryList(query.Get("orderType"))
	statusFilters := splitQueryList(query.Get("status"))
	paymentFilters := splitQueryList(query.Get("paymentMethod"))

	if includeGroup && !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required for group analytics")
		return
	}

	startDate, endDate, err := resolveSalesDateRange(period, customStart, customEnd)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	merchantIDs, timezone, err := h.resolveMerchantScope(ctx, *authCtx.MerchantID, includeGroup)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	cacheBucket := time.Now().Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey(
		"sales_analytics",
		*authCtx.MerchantID,
		period,
		scope,
		startDate.Format(time.RFC3339),
		endDate.Format(time.RFC3339),
		strings.Join(orderTypeFilters, ","),
		strings.Join(statusFilters, ","),
		strings.Join(paymentFilters, ","),
		fmt.Sprint(scheduledOnly),
		cacheBucket.Format(time.RFC3339),
	)
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	orders, err := h.loadSalesOrders(ctx, merchantIDs, startDate, endDate, orderTypeFilters, statusFilters, paymentFilters, scheduledOnly)
	if err != nil {
		h.Logger.Error("sales analytics orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch sales analytics")
		return
	}

	orderItems, err := h.loadSalesOrderItems(ctx, merchantIDs, startDate, endDate, orderTypeFilters, statusFilters, paymentFilters, scheduledOnly)
	if err != nil {
		h.Logger.Error("sales analytics items fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch sales analytics")
		return
	}

	completedOrders := make([]salesOrderRow, 0)
	for _, order := range orders {
		if order.Status == "COMPLETED" {
			completedOrders = append(completedOrders, order)
		}
	}

	totalRevenue := 0.0
	for _, order := range completedOrders {
		totalRevenue += order.TotalAmount
	}
	averageOrderValue := 0.0
	if len(completedOrders) > 0 {
		averageOrderValue = totalRevenue / float64(len(completedOrders))
	}

	cancelledOrders := 0
	for _, order := range orders {
		if order.Status == "CANCELLED" {
			cancelledOrders += 1
		}
	}

	summary := map[string]any{
		"totalRevenue":      totalRevenue,
		"totalOrders":       len(orders),
		"averageOrderValue": averageOrderValue,
		"completedOrders":   len(completedOrders),
		"cancelledOrders":   cancelledOrders,
		"completionRate": func() float64 {
			if len(orders) == 0 {
				return 0
			}
			return (float64(len(completedOrders)) / float64(len(orders))) * 100
		}(),
	}

	customerMetrics := buildSalesCustomerMetrics(completedOrders, startDate, endDate)
	cohortRetention := buildSalesCohorts(completedOrders, timezone, endDate)
	revenueTrend := buildSalesRevenueTrend(completedOrders, timezone)
	topSellingItems := buildSalesTopItems(orderItems, completedOrders)
	peakHours := buildSalesPeakHours(completedOrders, timezone)
	ordersByStatus := buildSalesStatusBreakdown(orders)
	paymentMethods := buildSalesPaymentMethods(completedOrders)
	orderTypes := buildSalesOrderTypes(completedOrders)
	categoryMix := buildSalesCategoryMix(orderItems, completedOrders)

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"summary":         summary,
			"customerMetrics": customerMetrics,
			"cohortRetention": cohortRetention,
			"revenueTrend":    revenueTrend,
			"topSellingItems": topSellingItems,
			"peakHours":       peakHours,
			"ordersByStatus":  ordersByStatus,
			"paymentMethods":  paymentMethods,
			"orderTypes":      orderTypes,
			"categoryMix":     categoryMix,
		},
		"meta": map[string]any{
			"period":    period,
			"startDate": startDate.Format(time.RFC3339),
			"endDate":   endDate.Format(time.RFC3339),
			"timezone":  timezone,
			"scope": func() string {
				if includeGroup {
					return "group"
				}
				return "merchant"
			}(),
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

type salesOrderRow struct {
	OrderID       int64
	Status        string
	OrderType     string
	TotalAmount   float64
	PlacedAt      time.Time
	CustomerID    *int64
	PaymentMethod string
}

type salesOrderItemRow struct {
	OrderID          int64
	MenuID           int64
	MenuName         string
	MenuNameOverride string
	Subtotal         float64
	Quantity         int32
	CategoryName     string
}

func resolveSalesDateRange(period, customStart, customEnd string) (time.Time, time.Time, error) {
	now := time.Now()
	var startDate time.Time
	endDate := now

	switch period {
	case "today":
		startDate = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	case "week":
		startDate = now.AddDate(0, 0, -7)
	case "month":
		startDate = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	case "quarter":
		startDate = now.AddDate(0, 0, -90)
	case "year":
		startDate = time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	case "custom":
		if customStart == "" || customEnd == "" {
			return time.Time{}, time.Time{}, errors.New("Start and end dates required for custom period")
		}
		parsedStart, err := parseDateInput(customStart)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("Invalid dates")
		}
		parsedEnd, err := parseDateInput(customEnd)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("Invalid dates")
		}
		startDate = parsedStart
		endDate = parsedEnd
	default:
		startDate = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	}
	return startDate, endDate, nil
}

func parseDateInput(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("empty")
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed, nil
	}
	return time.Time{}, errors.New("invalid date")
}

func splitQueryList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0)
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func (h *Handler) resolveMerchantScope(ctx context.Context, merchantID int64, includeGroup bool) ([]int64, string, error) {
	var (
		parentID pgtype.Int8
		timezone pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `select parent_merchant_id, timezone from merchants where id = $1`, merchantID).Scan(&parentID, &timezone); err != nil {
		return nil, "", err
	}

	mainID := merchantID
	if parentID.Valid {
		mainID = parentID.Int64
	}

	if !includeGroup {
		return []int64{merchantID}, textOrDefault(timezone, "Asia/Jakarta"), nil
	}

	rows, err := h.DB.Query(ctx, `select id, timezone from merchants where id = $1 or parent_merchant_id = $1`, mainID)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	ids := make([]int64, 0)
	mainTimezone := ""
	for rows.Next() {
		var id int64
		var tz pgtype.Text
		if err := rows.Scan(&id, &tz); err != nil {
			continue
		}
		ids = append(ids, id)
		if id == mainID {
			mainTimezone = textOrDefault(tz, "")
		}
	}

	if mainTimezone == "" {
		mainTimezone = textOrDefault(timezone, "Asia/Jakarta")
	}

	return ids, mainTimezone, nil
}

func (h *Handler) loadSalesOrders(ctx context.Context, merchantIDs []int64, startDate, endDate time.Time, orderTypes, statuses, paymentMethods []string, scheduledOnly bool) ([]salesOrderRow, error) {
	where, args := buildSalesWhereClause(merchantIDs, startDate, endDate, orderTypes, statuses, paymentMethods, scheduledOnly)

	query := `
		select o.id, o.status, o.order_type, o.total_amount, o.placed_at, o.customer_id, coalesce(p.payment_method::text, '')
		from orders o
		left join payments p on p.order_id = o.id
		where ` + where

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]salesOrderRow, 0)
	for rows.Next() {
		var (
			row        salesOrderRow
			total      pgtype.Numeric
			customerID pgtype.Int8
			payment    pgtype.Text
		)
		if err := rows.Scan(&row.OrderID, &row.Status, &row.OrderType, &total, &row.PlacedAt, &customerID, &payment); err != nil {
			continue
		}
		row.TotalAmount = utils.NumericToFloat64(total)
		if customerID.Valid {
			row.CustomerID = &customerID.Int64
		}
		row.PaymentMethod = textOrDefault(payment, "")
		orders = append(orders, row)
	}
	return orders, nil
}

func (h *Handler) loadSalesOrderItems(ctx context.Context, merchantIDs []int64, startDate, endDate time.Time, orderTypes, statuses, paymentMethods []string, scheduledOnly bool) ([]salesOrderItemRow, error) {
	where, args := buildSalesWhereClause(merchantIDs, startDate, endDate, orderTypes, statuses, paymentMethods, scheduledOnly)

	query := `
		select oi.order_id, oi.menu_id, oi.menu_name, oi.subtotal, oi.quantity, m.name, mc.name
		from orders o
		join order_items oi on oi.order_id = o.id
		left join menus m on m.id = oi.menu_id
		left join menu_categories mc on mc.id = m.category_id
		left join payments p on p.order_id = o.id
		where ` + where

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]salesOrderItemRow, 0)
	for rows.Next() {
		var (
			row      salesOrderItemRow
			subtotal pgtype.Numeric
			realName pgtype.Text
			category pgtype.Text
		)
		if err := rows.Scan(&row.OrderID, &row.MenuID, &row.MenuNameOverride, &subtotal, &row.Quantity, &realName, &category); err != nil {
			continue
		}
		row.Subtotal = utils.NumericToFloat64(subtotal)
		row.MenuName = textOrDefault(realName, row.MenuNameOverride)
		row.CategoryName = textOrDefault(category, "Uncategorized")
		items = append(items, row)
	}
	return items, nil
}

func buildSalesWhereClause(merchantIDs []int64, startDate, endDate time.Time, orderTypes, statuses, paymentMethods []string, scheduledOnly bool) (string, []any) {
	where := []string{"o.merchant_id = any($1)", "o.placed_at >= $2", "o.placed_at <= $3"}
	args := []any{merchantIDs, startDate, endDate}
	idx := 3

	if len(orderTypes) > 0 {
		idx += 1
		where = append(where, "o.order_type = any($"+intToString(idx)+")")
		args = append(args, orderTypes)
	}
	if len(statuses) > 0 {
		idx += 1
		where = append(where, "o.status = any($"+intToString(idx)+")")
		args = append(args, statuses)
	}
	if scheduledOnly {
		where = append(where, "o.is_scheduled = true")
	}
	if len(paymentMethods) > 0 {
		idx += 1
		where = append(where, "p.payment_method = any($"+intToString(idx)+")")
		args = append(args, paymentMethods)
	}
	return strings.Join(where, " and "), args
}

func buildSalesCustomerMetrics(orders []salesOrderRow, startDate, endDate time.Time) map[string]any {
	customerOrders := make(map[int64][]time.Time)
	for _, order := range orders {
		if order.CustomerID == nil {
			continue
		}
		list := customerOrders[*order.CustomerID]
		list = append(list, order.PlacedAt)
		customerOrders[*order.CustomerID] = list
	}

	totalCustomers := len(customerOrders)
	repeatCustomers := 0
	totalOrders := 0
	for _, list := range customerOrders {
		sort.Slice(list, func(i, j int) bool { return list[i].Before(list[j]) })
		totalOrders += len(list)
		if len(list) > 1 {
			repeatCustomers += 1
		}
	}

	newCustomers := 0
	returningCustomers := 0
	for _, list := range customerOrders {
		first := list[0]
		if (first.Equal(startDate) || first.After(startDate)) && (first.Equal(endDate) || first.Before(endDate)) {
			newCustomers += 1
		} else {
			returningCustomers += 1
		}
	}

	averageOrdersPerCustomer := 0.0
	if totalCustomers > 0 {
		averageOrdersPerCustomer = float64(totalOrders) / float64(totalCustomers)
	}

	repeatPurchaseRate := 0.0
	if totalCustomers > 0 {
		repeatPurchaseRate = (float64(repeatCustomers) / float64(totalCustomers)) * 100
	}

	return map[string]any{
		"totalCustomers":           totalCustomers,
		"newCustomers":             newCustomers,
		"returningCustomers":       returningCustomers,
		"repeatPurchaseRate":       repeatPurchaseRate,
		"averageOrdersPerCustomer": averageOrdersPerCustomer,
	}
}

func buildSalesCohorts(orders []salesOrderRow, timezone string, endDate time.Time) []map[string]any {
	loc := loadTimezone(timezone)
	cohortStart := endDate.AddDate(-1, 0, 0)
	customerMonths := make(map[int64]map[string]bool)

	for _, order := range orders {
		if order.CustomerID == nil {
			continue
		}
		if order.PlacedAt.Before(cohortStart) {
			continue
		}
		monthKey := order.PlacedAt.In(loc).Format("2006-01")
		months := customerMonths[*order.CustomerID]
		if months == nil {
			months = map[string]bool{}
			customerMonths[*order.CustomerID] = months
		}
		months[monthKey] = true
	}

	cohortMap := make(map[string]map[int64]bool)
	for customerID, months := range customerMonths {
		firstMonth := ""
		for month := range months {
			if firstMonth == "" || month < firstMonth {
				firstMonth = month
			}
		}
		if firstMonth == "" {
			continue
		}
		cohortSet := cohortMap[firstMonth]
		if cohortSet == nil {
			cohortSet = map[int64]bool{}
			cohortMap[firstMonth] = cohortSet
		}
		cohortSet[customerID] = true
	}

	cohorts := make([]map[string]any, 0)
	for cohortMonth, cohortCustomers := range cohortMap {
		parts := strings.Split(cohortMonth, "-")
		if len(parts) != 2 {
			continue
		}
		year, _ := parseStringToInt(parts[0])
		month, _ := parseStringToInt(parts[1])
		if year == 0 || month == 0 {
			continue
		}
		base := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, loc)
		month1 := base.AddDate(0, 1, 0).Format("2006-01")
		month2 := base.AddDate(0, 2, 0).Format("2006-01")
		month3 := base.AddDate(0, 3, 0).Format("2006-01")

		cohortSize := len(cohortCustomers)
		retention := map[string]int{"month1": 0, "month2": 0, "month3": 0}
		for customerID := range cohortCustomers {
			months := customerMonths[customerID]
			if months[month1] {
				retention["month1"] += 1
			}
			if months[month2] {
				retention["month2"] += 1
			}
			if months[month3] {
				retention["month3"] += 1
			}
		}

		cohorts = append(cohorts, map[string]any{
			"cohortMonth": cohortMonth,
			"size":        cohortSize,
			"month1":      percentage(retention["month1"], cohortSize),
			"month2":      percentage(retention["month2"], cohortSize),
			"month3":      percentage(retention["month3"], cohortSize),
		})
	}

	sort.Slice(cohorts, func(i, j int) bool {
		return StringValue(cohorts[i]["cohortMonth"]) < StringValue(cohorts[j]["cohortMonth"])
	})
	if len(cohorts) > 6 {
		cohorts = cohorts[len(cohorts)-6:]
	}
	return cohorts
}

func buildSalesRevenueTrend(orders []salesOrderRow, timezone string) []map[string]any {
	loc := loadTimezone(timezone)
	trendMap := make(map[string]map[string]any)
	for _, order := range orders {
		dateKey := order.PlacedAt.In(loc).Format("2006-01-02")
		row := trendMap[dateKey]
		if row == nil {
			row = map[string]any{"date": dateKey, "revenue": 0.0, "orders": 0}
			trendMap[dateKey] = row
		}
		row["revenue"] = toFloat64(row["revenue"]) + order.TotalAmount
		row["orders"] = int(toFloat64(row["orders"]) + 1)
	}

	trend := make([]map[string]any, 0, len(trendMap))
	for _, row := range trendMap {
		trend = append(trend, row)
	}
	sort.Slice(trend, func(i, j int) bool {
		return StringValue(trend[i]["date"]) < StringValue(trend[j]["date"])
	})
	return trend
}

func buildSalesTopItems(items []salesOrderItemRow, orders []salesOrderRow) []map[string]any {
	orderStatus := make(map[int64]string)
	for _, order := range orders {
		orderStatus[order.OrderID] = order.Status
	}

	itemMap := make(map[string]map[string]any)
	for _, item := range items {
		if orderStatus[item.OrderID] != "COMPLETED" {
			continue
		}

		key := int64ToString(item.MenuID)
		displayName := item.MenuName
		if item.MenuName == posCustomPlaceholderMenuNameAnalytics {
			key = "CUSTOM::" + item.MenuNameOverride
			displayName = item.MenuNameOverride
		}

		row := itemMap[key]
		if row == nil {
			row = map[string]any{"menuId": key, "menuName": displayName, "quantity": 0, "revenue": 0.0}
			itemMap[key] = row
		}

		row["quantity"] = int(toFloat64(row["quantity"]) + float64(item.Quantity))
		row["revenue"] = toFloat64(row["revenue"]) + item.Subtotal
	}

	itemsOut := make([]map[string]any, 0, len(itemMap))
	for _, row := range itemMap {
		itemsOut = append(itemsOut, row)
	}

	totalRevenue := 0.0
	for _, row := range itemsOut {
		totalRevenue += toFloat64(row["revenue"])
	}

	for _, row := range itemsOut {
		percentage := 0.0
		if totalRevenue > 0 {
			percentage = toFloat64(row["revenue"]) / totalRevenue * 100
		}
		row["percentage"] = percentage
	}

	sort.Slice(itemsOut, func(i, j int) bool {
		return toFloat64(itemsOut[i]["quantity"]) > toFloat64(itemsOut[j]["quantity"])
	})
	if len(itemsOut) > 10 {
		itemsOut = itemsOut[:10]
	}
	return itemsOut
}

func buildSalesPeakHours(orders []salesOrderRow, timezone string) []map[string]any {
	loc := loadTimezone(timezone)
	peakMap := make([]map[string]any, 24)
	for i := 0; i < 24; i++ {
		peakMap[i] = map[string]any{"hour": i, "orders": 0, "revenue": 0.0}
	}
	for _, order := range orders {
		hour := order.PlacedAt.In(loc).Hour()
		row := peakMap[hour]
		row["orders"] = int(toFloat64(row["orders"]) + 1)
		row["revenue"] = toFloat64(row["revenue"]) + order.TotalAmount
	}
	return peakMap
}

func buildSalesStatusBreakdown(orders []salesOrderRow) []map[string]any {
	statusMap := make(map[string]int)
	for _, order := range orders {
		statusMap[order.Status] += 1
	}

	result := make([]map[string]any, 0, len(statusMap))
	for status, count := range statusMap {
		percentage := 0.0
		if len(orders) > 0 {
			percentage = (float64(count) / float64(len(orders))) * 100
		}
		result = append(result, map[string]any{
			"status":     status,
			"count":      count,
			"percentage": percentage,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return toFloat64(result[i]["count"]) > toFloat64(result[j]["count"])
	})
	return result
}

func buildSalesPaymentMethods(orders []salesOrderRow) []map[string]any {
	paymentMap := make(map[string]map[string]any)
	for _, order := range orders {
		method := defaultString(order.PaymentMethod, "UNKNOWN")
		row := paymentMap[method]
		if row == nil {
			row = map[string]any{"method": method, "count": 0, "revenue": 0.0}
			paymentMap[method] = row
		}
		row["count"] = int(toFloat64(row["count"]) + 1)
		row["revenue"] = toFloat64(row["revenue"]) + order.TotalAmount
	}

	result := make([]map[string]any, 0, len(paymentMap))
	for _, row := range paymentMap {
		result = append(result, row)
	}

	total := 0.0
	for _, row := range result {
		total += toFloat64(row["count"])
	}

	for _, row := range result {
		percentage := 0.0
		if total > 0 {
			percentage = toFloat64(row["count"]) / total * 100
		}
		row["percentage"] = percentage
	}

	sort.Slice(result, func(i, j int) bool {
		return toFloat64(result[i]["count"]) > toFloat64(result[j]["count"])
	})
	return result
}

func buildSalesOrderTypes(orders []salesOrderRow) []map[string]any {
	typeMap := make(map[string]map[string]any)
	for _, order := range orders {
		key := defaultString(order.OrderType, "UNKNOWN")
		row := typeMap[key]
		if row == nil {
			row = map[string]any{"type": key, "count": 0, "revenue": 0.0}
			typeMap[key] = row
		}
		row["count"] = int(toFloat64(row["count"]) + 1)
		row["revenue"] = toFloat64(row["revenue"]) + order.TotalAmount
	}

	result := make([]map[string]any, 0, len(typeMap))
	for _, row := range typeMap {
		result = append(result, row)
	}

	total := 0.0
	for _, row := range result {
		total += toFloat64(row["count"])
	}

	for _, row := range result {
		percentage := 0.0
		if total > 0 {
			percentage = toFloat64(row["count"]) / total * 100
		}
		row["percentage"] = percentage
	}

	sort.Slice(result, func(i, j int) bool {
		return toFloat64(result[i]["count"]) > toFloat64(result[j]["count"])
	})
	return result
}

func buildSalesCategoryMix(items []salesOrderItemRow, orders []salesOrderRow) []map[string]any {
	orderStatus := make(map[int64]string)
	for _, order := range orders {
		orderStatus[order.OrderID] = order.Status
	}

	categoryMap := make(map[string]map[string]any)
	for _, item := range items {
		if orderStatus[item.OrderID] != "COMPLETED" {
			continue
		}
		key := defaultString(item.CategoryName, "Uncategorized")
		row := categoryMap[key]
		if row == nil {
			row = map[string]any{"category": key, "revenue": 0.0, "quantity": 0}
			categoryMap[key] = row
		}
		row["revenue"] = toFloat64(row["revenue"]) + item.Subtotal
		row["quantity"] = int(toFloat64(row["quantity"]) + float64(item.Quantity))
	}

	result := make([]map[string]any, 0, len(categoryMap))
	for _, row := range categoryMap {
		result = append(result, row)
	}

	totalRevenue := 0.0
	for _, row := range result {
		totalRevenue += toFloat64(row["revenue"])
	}

	for _, row := range result {
		percentage := 0.0
		if totalRevenue > 0 {
			percentage = toFloat64(row["revenue"]) / totalRevenue * 100
		}
		row["percentage"] = percentage
	}

	sort.Slice(result, func(i, j int) bool {
		return toFloat64(result[i]["revenue"]) > toFloat64(result[j]["revenue"])
	})
	return result
}

func loadTimezone(timezone string) *time.Location {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Local
	}
	return loc
}

func percentage(count int, total int) float64 {
	if total == 0 {
		return 0
	}
	return (float64(count) / float64(total)) * 100
}

func StringValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return ""
	}
}
