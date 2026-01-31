package handlers

import (
	"context"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type reportOrder struct {
	ID                  int64
	Status              string
	OrderType           string
	PlacedAt            time.Time
	CompletedAt         *time.Time
	IsScheduled         bool
	TotalAmount         pgtype.Numeric
	Subtotal            pgtype.Numeric
	TaxAmount           pgtype.Numeric
	ServiceChargeAmount pgtype.Numeric
	PackagingFeeAmount  pgtype.Numeric
	DeliveryFeeAmount   pgtype.Numeric
	DiscountAmount      pgtype.Numeric
	PaymentMethod       pgtype.Text
}

type reportFilters struct {
	OrderTypes     []string
	Statuses       []string
	PaymentMethods []string
	VoucherSources []string
	ScheduledOnly  bool
}

type reportSummary struct {
	TotalOrders        int64   `json:"totalOrders"`
	CompletedOrders    int64   `json:"completedOrders"`
	CancelledOrders    int64   `json:"cancelledOrders"`
	TotalRevenue       float64 `json:"totalRevenue"`
	Subtotal           float64 `json:"subtotal"`
	TotalTax           float64 `json:"totalTax"`
	TotalServiceCharge float64 `json:"totalServiceCharge"`
	TotalPackagingFee  float64 `json:"totalPackagingFee"`
	TotalDeliveryFee   float64 `json:"totalDeliveryFee"`
	TotalDiscount      float64 `json:"totalDiscount"`
	NetRevenue         float64 `json:"netRevenue"`
	AverageOrderValue  float64 `json:"averageOrderValue"`
	CompletionRate     float64 `json:"completionRate"`
}

type dailyRevenueEntry struct {
	Date         string  `json:"date"`
	TotalRevenue float64 `json:"totalRevenue"`
	TotalOrders  int64   `json:"totalOrders"`
}

type hourlyPerformanceEntry struct {
	Hour        int      `json:"hour"`
	OrderCount  int64    `json:"orderCount"`
	Efficiency  float64  `json:"efficiency"`
	AvgPrepTime *float64 `json:"avgPrepTime"`
}

type reportAnomaly struct {
	Date     string  `json:"date"`
	Revenue  float64 `json:"revenue"`
	Expected float64 `json:"expected"`
	DeltaPct float64 `json:"deltaPct"`
}

func (h *Handler) MerchantReports(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	period := strings.TrimSpace(r.URL.Query().Get("period"))
	if period == "" {
		period = "month"
	}

	anomalyWindow := parseIntWithDefault(r.URL.Query().Get("anomalyWindow"), 7)
	anomalyStdDev := parseFloatWithDefault(r.URL.Query().Get("anomalyStdDev"), 2)
	anomalyMinDropPct := parseFloatWithDefault(r.URL.Query().Get("anomalyMinDropPct"), 15)

	filters := reportFilters{
		OrderTypes:     parseList(r.URL.Query().Get("orderType")),
		Statuses:       parseList(r.URL.Query().Get("status")),
		PaymentMethods: parseList(r.URL.Query().Get("paymentMethod")),
		VoucherSources: parseList(r.URL.Query().Get("voucherSource")),
		ScheduledOnly:  strings.EqualFold(r.URL.Query().Get("scheduledOnly"), "true"),
	}

	currency, timezone, err := h.getMerchantCurrencyTimezone(ctx, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("merchant report merchant lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reports data")
		return
	}

	location := resolveLocation(timezone)
	startDate, endDate := resolveReportRange(period, r.URL.Query().Get("startDate"), r.URL.Query().Get("endDate"), location)
	previousStart, previousEnd := resolvePreviousRange(period, startDate, endDate, location)
	cacheBucket := time.Now().Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey(
		"merchant_reports",
		*authCtx.MerchantID,
		period,
		startDate.Format(time.RFC3339),
		endDate.Format(time.RFC3339),
		strings.Join(filters.OrderTypes, ","),
		strings.Join(filters.Statuses, ","),
		strings.Join(filters.PaymentMethods, ","),
		strings.Join(filters.VoucherSources, ","),
		strconv.FormatBool(filters.ScheduledOnly),
		strconv.Itoa(anomalyWindow),
		strconv.FormatFloat(anomalyStdDev, 'f', -1, 64),
		strconv.FormatFloat(anomalyMinDropPct, 'f', -1, 64),
		cacheBucket.Format(time.RFC3339),
	)
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	orders, err := h.fetchReportOrders(ctx, *authCtx.MerchantID, startDate, endDate, filters)
	if err != nil {
		h.Logger.Error("merchant report orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reports data")
		return
	}

	previousOrders, err := h.fetchReportOrders(ctx, *authCtx.MerchantID, previousStart, previousEnd, filters)
	if err != nil {
		h.Logger.Error("merchant report previous orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reports data")
		return
	}

	currentSummary := summarizeReportOrders(orders)
	previousSummary := summarizeReportOrders(previousOrders)

	completedOrders := filterOrdersByStatus(orders, "COMPLETED")
	dailyRevenue := buildDailyRevenue(completedOrders, location)
	anomalies := computeReportAnomalies(dailyRevenue, anomalyWindow, anomalyStdDev, anomalyMinDropPct)
	voucherSummary, err := h.buildVoucherSummary(ctx, completedOrders)
	if err != nil {
		h.Logger.Error("merchant report voucher summary failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reports data")
		return
	}
	orderTypeBreakdown := buildOrderTypeBreakdownReport(completedOrders)
	orderStatusBreakdown := buildOrderStatusBreakdownReport(orders)
	paymentBreakdown := buildPaymentBreakdownReport(completedOrders)
	topMenuItems, err := h.buildTopMenuItemsReport(ctx, completedOrders)
	if err != nil {
		h.Logger.Error("merchant report top menu items failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reports data")
		return
	}
	hourlyPerformance := buildHourlyPerformanceReport(orders, location)
	scheduledSummary := buildScheduledSummary(orders)

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"period": period,
			"dateRange": map[string]any{
				"current":  map[string]any{"start": startDate, "end": endDate},
				"previous": map[string]any{"start": previousStart, "end": previousEnd},
			},
			"merchant": map[string]any{
				"currency": currency,
				"timezone": timezone,
			},
			"summary": currentSummary,
			"periodComparison": map[string]any{
				"metrics": []map[string]any{
					{"label": "Total Revenue", "current": currentSummary.TotalRevenue, "previous": previousSummary.TotalRevenue, "format": "currency"},
					{"label": "Net Revenue", "current": currentSummary.NetRevenue, "previous": previousSummary.NetRevenue, "format": "currency"},
					{"label": "Total Orders", "current": currentSummary.TotalOrders, "previous": previousSummary.TotalOrders, "format": "number"},
					{"label": "Avg. Order Value", "current": currentSummary.AverageOrderValue, "previous": previousSummary.AverageOrderValue, "format": "currency"},
					{"label": "Completion Rate", "current": currentSummary.CompletionRate, "previous": previousSummary.CompletionRate, "format": "decimal"},
				},
			},
			"voucherSummary": voucherSummary,
			"feesBreakdown": map[string]any{
				"tax":           currentSummary.TotalTax,
				"serviceCharge": currentSummary.TotalServiceCharge,
				"packagingFee":  currentSummary.TotalPackagingFee,
				"deliveryFee":   currentSummary.TotalDeliveryFee,
				"discount":      currentSummary.TotalDiscount,
			},
			"orderTypeBreakdown":   orderTypeBreakdown,
			"orderStatusBreakdown": orderStatusBreakdown,
			"paymentBreakdown":     paymentBreakdown,
			"scheduledSummary":     scheduledSummary,
			"dailyRevenue":         dailyRevenue,
			"anomalies":            anomalies,
			"anomalySettings": map[string]any{
				"windowSize":       anomalyWindow,
				"stdDevMultiplier": anomalyStdDev,
				"minDropPct":       anomalyMinDropPct,
			},
			"topMenuItems":      topMenuItems,
			"hourlyPerformance": hourlyPerformance,
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

func (h *Handler) MerchantReportsSalesDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	period := strings.TrimSpace(r.URL.Query().Get("period"))
	if period == "" {
		period = "month"
	}

	startDate, previousStart, previousEnd := resolveSalesDashboardDates(period)
	cacheBucket := time.Now().Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey(
		"sales_dashboard",
		*authCtx.MerchantID,
		period,
		startDate.Format(time.RFC3339),
		previousStart.Format(time.RFC3339),
		previousEnd.Format(time.RFC3339),
		cacheBucket.Format(time.RFC3339),
	)
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}
	orders, err := h.fetchReportOrders(ctx, *authCtx.MerchantID, startDate, time.Now(), reportFilters{})
	if err != nil {
		h.Logger.Error("sales dashboard orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "SERVER_ERROR", "Failed to fetch sales dashboard")
		return
	}

	previousOrders, err := h.fetchReportOrders(ctx, *authCtx.MerchantID, previousStart, previousEnd, reportFilters{Statuses: []string{"COMPLETED"}})
	if err != nil {
		h.Logger.Error("sales dashboard previous orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "SERVER_ERROR", "Failed to fetch sales dashboard")
		return
	}

	completedOrders := filterOrdersByStatus(orders, "COMPLETED")
	previousRevenue := sumOrderTotals(previousOrders)
	totalRevenue := sumOrderTotals(completedOrders)

	summary := map[string]any{
		"totalRevenue":      totalRevenue,
		"totalOrders":       int64(len(orders)),
		"averageOrderValue": averageValue(totalRevenue, int64(len(completedOrders))),
		"completedOrders":   int64(len(completedOrders)),
		"cancelledOrders":   int64(len(filterOrdersByStatus(orders, "CANCELLED"))),
		"pendingOrders":     int64(len(filterOrdersByStatuses(orders, []string{"PENDING", "ACCEPTED", "IN_PROGRESS", "READY"}))),
	}

	revenueTrend := buildRevenueTrend(completedOrders)
	topSellingItems, err := h.buildTopMenuItemsSales(ctx, completedOrders)
	if err != nil {
		h.Logger.Error("sales dashboard top items failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "SERVER_ERROR", "Failed to fetch sales dashboard")
		return
	}
	peakHours := buildPeakHours(completedOrders)
	orderTypeBreakdown := buildOrderTypeSales(completedOrders)
	paymentMethodBreakdown := buildPaymentMethodSales(completedOrders)

	percentageChange := 0.0
	if previousRevenue > 0 {
		percentageChange = ((totalRevenue - previousRevenue) / previousRevenue) * 100
	} else if totalRevenue > 0 {
		percentageChange = 100
	}

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"summary":                summary,
			"revenueTrend":           revenueTrend,
			"topSellingItems":        topSellingItems,
			"peakHours":              peakHours,
			"orderTypeBreakdown":     orderTypeBreakdown,
			"paymentMethodBreakdown": paymentMethodBreakdown,
			"revenueComparison": map[string]any{
				"current":          totalRevenue,
				"previous":         previousRevenue,
				"percentageChange": percentageChange,
			},
		},
		"meta": map[string]any{
			"period":    period,
			"startDate": startDate.Format(time.RFC3339),
			"endDate":   time.Now().Format(time.RFC3339),
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

func (h *Handler) fetchReportOrders(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time, filters reportFilters) ([]reportOrder, error) {
	query := strings.Builder{}
	query.WriteString(`
		select o.id, o.status, o.order_type, o.placed_at, o.completed_at, o.is_scheduled,
		       o.total_amount, o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee,
		       o.delivery_fee_amount, o.discount_amount, p.payment_method
		from orders o
		left join payments p on p.order_id = o.id
		where o.merchant_id = $1
		  and o.placed_at >= $2
		  and o.placed_at <= $3
	`)

	args := []any{merchantID, startDate, endDate}
	idx := 4

	if len(filters.OrderTypes) > 0 {
		query.WriteString(" and o.order_type = any($" + strconv.Itoa(idx) + ")")
		args = append(args, filters.OrderTypes)
		idx++
	}
	if len(filters.Statuses) > 0 {
		query.WriteString(" and o.status = any($" + strconv.Itoa(idx) + ")")
		args = append(args, filters.Statuses)
		idx++
	}
	if filters.ScheduledOnly {
		query.WriteString(" and o.is_scheduled = true")
	}
	if len(filters.PaymentMethods) > 0 {
		query.WriteString(" and p.payment_method = any($" + strconv.Itoa(idx) + ")")
		args = append(args, filters.PaymentMethods)
		idx++
	}
	if len(filters.VoucherSources) > 0 {
		query.WriteString(" and exists (select 1 from order_discounts od where od.order_id = o.id and od.source = any($" + strconv.Itoa(idx) + "))")
		args = append(args, filters.VoucherSources)
		idx++
	}

	rows, err := h.DB.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]reportOrder, 0)
	for rows.Next() {
		var (
			completedAt   pgtype.Timestamptz
			paymentMethod pgtype.Text
		)
		order := reportOrder{}
		if err := rows.Scan(&order.ID, &order.Status, &order.OrderType, &order.PlacedAt, &completedAt, &order.IsScheduled,
			&order.TotalAmount, &order.Subtotal, &order.TaxAmount, &order.ServiceChargeAmount, &order.PackagingFeeAmount,
			&order.DeliveryFeeAmount, &order.DiscountAmount, &paymentMethod); err != nil {
			return nil, err
		}
		if completedAt.Valid {
			copyTime := completedAt.Time
			order.CompletedAt = &copyTime
		}
		order.PaymentMethod = paymentMethod
		orders = append(orders, order)
	}

	return orders, nil
}

func (h *Handler) getMerchantCurrencyTimezone(ctx context.Context, merchantID int64) (string, string, error) {
	var currency pgtype.Text
	var timezone pgtype.Text
	if err := h.DB.QueryRow(ctx, "select currency, timezone from merchants where id = $1", merchantID).Scan(&currency, &timezone); err != nil {
		return "AUD", "Asia/Jakarta", err
	}

	currencyValue := "AUD"
	if currency.Valid {
		currencyValue = currency.String
	}
	timezoneValue := "Asia/Jakarta"
	if timezone.Valid {
		timezoneValue = timezone.String
	}
	return currencyValue, timezoneValue, nil
}

func summarizeReportOrders(orders []reportOrder) reportSummary {
	summary := reportSummary{}
	var completedRevenue float64
	for _, order := range orders {
		summary.TotalOrders++
		switch order.Status {
		case "COMPLETED":
			summary.CompletedOrders++
			completedRevenue += utils.NumericToFloat64(order.TotalAmount)
			summary.Subtotal += utils.NumericToFloat64(order.Subtotal)
			summary.TotalRevenue += utils.NumericToFloat64(order.TotalAmount)
			summary.TotalTax += utils.NumericToFloat64(order.TaxAmount)
			summary.TotalServiceCharge += utils.NumericToFloat64(order.ServiceChargeAmount)
			summary.TotalPackagingFee += utils.NumericToFloat64(order.PackagingFeeAmount)
			summary.TotalDeliveryFee += utils.NumericToFloat64(order.DeliveryFeeAmount)
			summary.TotalDiscount += utils.NumericToFloat64(order.DiscountAmount)
		case "CANCELLED":
			summary.CancelledOrders++
		}
	}

	if summary.CompletedOrders > 0 {
		summary.AverageOrderValue = completedRevenue / float64(summary.CompletedOrders)
	}
	if summary.TotalOrders > 0 {
		summary.CompletionRate = (float64(summary.CompletedOrders) / float64(summary.TotalOrders)) * 100
	}

	summary.NetRevenue = summary.Subtotal - summary.TotalDiscount
	return summary
}

func filterOrdersByStatus(orders []reportOrder, status string) []reportOrder {
	filtered := make([]reportOrder, 0)
	for _, order := range orders {
		if order.Status == status {
			filtered = append(filtered, order)
		}
	}
	return filtered
}

func filterOrdersByStatuses(orders []reportOrder, statuses []string) []reportOrder {
	lookup := make(map[string]struct{})
	for _, status := range statuses {
		lookup[status] = struct{}{}
	}
	filtered := make([]reportOrder, 0)
	for _, order := range orders {
		if _, ok := lookup[order.Status]; ok {
			filtered = append(filtered, order)
		}
	}
	return filtered
}

func buildDailyRevenue(orders []reportOrder, location *time.Location) []dailyRevenueEntry {
	mapByDate := make(map[string]*dailyRevenueEntry)
	for _, order := range orders {
		dateKey := order.PlacedAt.In(location).Format("2006-01-02")
		entry := mapByDate[dateKey]
		if entry == nil {
			entry = &dailyRevenueEntry{Date: dateKey}
			mapByDate[dateKey] = entry
		}
		entry.TotalOrders++
		entry.TotalRevenue += utils.NumericToFloat64(order.TotalAmount)
	}

	entries := make([]dailyRevenueEntry, 0, len(mapByDate))
	for _, entry := range mapByDate {
		entries = append(entries, *entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Date < entries[j].Date })
	return entries
}

func computeReportAnomalies(series []dailyRevenueEntry, windowSize int, stdDevMultiplier float64, minDropPct float64) []reportAnomaly {
	if windowSize < 3 {
		windowSize = 3
	}
	if stdDevMultiplier < 0.5 {
		stdDevMultiplier = 0.5
	}
	if minDropPct < 0 {
		minDropPct = 0
	}

	anomalies := make([]reportAnomaly, 0)
	for i := 0; i < len(series); i++ {
		if i < windowSize {
			continue
		}
		window := series[i-windowSize : i]
		mean := 0.0
		for _, entry := range window {
			mean += entry.TotalRevenue
		}
		mean = mean / float64(len(window))
		variance := 0.0
		for _, entry := range window {
			variance += math.Pow(entry.TotalRevenue-mean, 2)
		}
		variance = variance / float64(len(window))
		std := math.Sqrt(variance)
		current := series[i]
		if mean <= 0 {
			continue
		}
		threshold := mean - stdDevMultiplier*std
		if current.TotalRevenue < threshold {
			deltaPct := ((current.TotalRevenue - mean) / mean) * 100
			if math.Abs(deltaPct) >= minDropPct {
				anomalies = append(anomalies, reportAnomaly{
					Date:     current.Date,
					Revenue:  current.TotalRevenue,
					Expected: mean,
					DeltaPct: deltaPct,
				})
			}
		}
	}
	return anomalies
}

func (h *Handler) buildVoucherSummary(ctx context.Context, orders []reportOrder) (map[string]any, error) {
	orderIDs := extractOrderIDs(orders)
	if len(orderIDs) == 0 {
		return map[string]any{"bySource": []map[string]any{}, "topTemplates": []map[string]any{}}, nil
	}

	rows, err := h.DB.Query(ctx, `
        select source, discount_amount, voucher_template_id, label
        from order_discounts
        where order_id = any($1)
    `, orderIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sourceAgg := make(map[string]map[string]float64)
	templateAgg := make(map[string]map[string]float64)
	templateIDs := make([]int64, 0)
	for rows.Next() {
		var (
			source     string
			discount   pgtype.Numeric
			templateID pgtype.Int8
			label      pgtype.Text
		)
		if err := rows.Scan(&source, &discount, &templateID, &label); err != nil {
			return nil, err
		}
		amount := utils.NumericToFloat64(discount)
		entry := sourceAgg[source]
		if entry == nil {
			entry = map[string]float64{"count": 0, "amount": 0}
		}
		entry["count"] += 1
		entry["amount"] += amount
		sourceAgg[source] = entry

		key := ""
		labelValue := "Voucher"
		if label.Valid {
			labelValue = label.String
		}
		if templateID.Valid {
			key = strconv.FormatInt(templateID.Int64, 10)
			templateIDs = append(templateIDs, templateID.Int64)
		} else {
			key = labelValue
		}
		entry = templateAgg[key]
		if entry == nil {
			entry = map[string]float64{"count": 0, "amount": 0}
		}
		entry["count"] += 1
		entry["amount"] += amount
		templateAgg[key] = entry
	}

	templateNames := map[int64]string{}
	if len(templateIDs) > 0 {
		rows, err = h.DB.Query(ctx, "select id, name from order_voucher_templates where id = any($1)", templateIDs)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				rows.Close()
				return nil, err
			}
			templateNames[id] = name
		}
		rows.Close()
	}

	bySource := make([]map[string]any, 0, len(sourceAgg))
	for source, entry := range sourceAgg {
		bySource = append(bySource, map[string]any{
			"source": source,
			"count":  int64(entry["count"]),
			"amount": entry["amount"],
		})
	}

	topTemplates := make([]map[string]any, 0, len(templateAgg))
	for key, entry := range templateAgg {
		label := key
		if id, err := strconv.ParseInt(key, 10, 64); err == nil {
			if name, ok := templateNames[id]; ok {
				label = name
			}
		}
		topTemplates = append(topTemplates, map[string]any{
			"label":  label,
			"count":  int64(entry["count"]),
			"amount": entry["amount"],
		})
	}
	return map[string]any{
		"bySource":     bySource,
		"topTemplates": sortByAmount(topTemplates, 5),
	}, nil
}

func buildOrderTypeBreakdownReport(orders []reportOrder) []map[string]any {
	agg := make(map[string]map[string]float64)
	for _, order := range orders {
		key := order.OrderType
		entry := agg[key]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		entry["count"] += 1
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		agg[key] = entry
	}

	out := make([]map[string]any, 0, len(agg))
	for key, entry := range agg {
		out = append(out, map[string]any{
			"type":    key,
			"count":   int64(entry["count"]),
			"revenue": entry["revenue"],
		})
	}
	return out
}

func buildOrderStatusBreakdownReport(orders []reportOrder) []map[string]any {
	agg := make(map[string]int64)
	for _, order := range orders {
		agg[order.Status]++
	}
	out := make([]map[string]any, 0, len(agg))
	for status, count := range agg {
		out = append(out, map[string]any{
			"status": status,
			"count":  count,
		})
	}
	return out
}

func buildPaymentBreakdownReport(orders []reportOrder) []map[string]any {
	agg := make(map[string]map[string]float64)
	for _, order := range orders {
		method := "UNKNOWN"
		if order.PaymentMethod.Valid {
			method = order.PaymentMethod.String
		}
		entry := agg[method]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		entry["count"] += 1
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		agg[method] = entry
	}

	out := make([]map[string]any, 0, len(agg))
	for method, entry := range agg {
		out = append(out, map[string]any{
			"method":  method,
			"count":   int64(entry["count"]),
			"revenue": entry["revenue"],
		})
	}
	return out
}

func (h *Handler) buildTopMenuItemsReport(ctx context.Context, orders []reportOrder) ([]map[string]any, error) {
	orderIDs := extractOrderIDs(orders)
	if len(orderIDs) == 0 {
		return []map[string]any{}, nil
	}
	rows, err := h.DB.Query(ctx, `
        select menu_id, menu_name, quantity, subtotal
        from order_items
        where order_id = any($1)
    `, orderIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	agg := make(map[string]map[string]any)
	for rows.Next() {
		var (
			menuID   pgtype.Int8
			menuName pgtype.Text
			quantity int32
			subtotal pgtype.Numeric
		)
		if err := rows.Scan(&menuID, &menuName, &quantity, &subtotal); err != nil {
			return nil, err
		}
		name := "Menu"
		if menuName.Valid {
			name = menuName.String
		}
		key := "CUSTOM::" + name
		if menuID.Valid {
			key = strconv.FormatInt(menuID.Int64, 10)
		}
		entry := agg[key]
		if entry == nil {
			entry = map[string]any{
				"key":      key,
				"name":     name,
				"quantity": int64(0),
				"revenue":  float64(0),
			}
		}
		entry["quantity"] = entry["quantity"].(int64) + int64(quantity)
		entry["revenue"] = entry["revenue"].(float64) + utils.NumericToFloat64(subtotal)
		agg[key] = entry
	}

	items := make([]map[string]any, 0, len(agg))
	for _, entry := range agg {
		items = append(items, entry)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i]["quantity"].(int64) > items[j]["quantity"].(int64)
	})
	if len(items) > 10 {
		items = items[:10]
	}
	return items, nil
}

func buildHourlyPerformanceReport(orders []reportOrder, location *time.Location) []hourlyPerformanceEntry {
	buckets := make([]hourlyPerformanceEntry, 24)
	prepBuckets := make(map[int][]float64)
	for hour := 0; hour < 24; hour++ {
		buckets[hour] = hourlyPerformanceEntry{Hour: hour}
	}

	for _, order := range orders {
		hour := order.PlacedAt.In(location).Hour()
		bucket := buckets[hour]
		bucket.OrderCount++
		buckets[hour] = bucket

		if order.CompletedAt != nil {
			minutes := order.CompletedAt.Sub(order.PlacedAt).Minutes()
			prepBuckets[hour] = append(prepBuckets[hour], minutes)
		}
	}

	for hour := range buckets {
		prepTimes := prepBuckets[hour]
		var avgPrep *float64
		if len(prepTimes) > 0 {
			sum := 0.0
			for _, v := range prepTimes {
				sum += v
			}
			value := sum / float64(len(prepTimes))
			avgPrep = &value
		}
		avgValue := 0.0
		if avgPrep != nil {
			avgValue = *avgPrep
		}
		targetPrepTime := 15.0
		prepTimeScore := math.Max(0, 100-(avgValue/targetPrepTime)*100)
		volumeScore := math.Min(100, float64(buckets[hour].OrderCount)*10)
		efficiency := (prepTimeScore + volumeScore) / 2
		buckets[hour].Efficiency = math.Min(100, math.Max(0, efficiency))
		buckets[hour].AvgPrepTime = avgPrep
	}
	return buckets
}

func buildScheduledSummary(orders []reportOrder) map[string]any {
	count := int64(0)
	revenue := 0.0
	for _, order := range orders {
		if order.IsScheduled {
			count++
			if order.Status == "COMPLETED" {
				revenue += utils.NumericToFloat64(order.TotalAmount)
			}
		}
	}
	return map[string]any{
		"scheduledCount":   count,
		"scheduledRevenue": revenue,
	}
}

func resolveReportRange(period string, startDateRaw string, endDateRaw string, location *time.Location) (time.Time, time.Time) {
	now := time.Now().In(location)
	period = strings.ToLower(period)
	if period == "custom" && startDateRaw != "" && endDateRaw != "" {
		start := parseDateValue(startDateRaw, location)
		end := parseDateValue(endDateRaw, location)
		if !end.IsZero() && !start.IsZero() {
			return start, end
		}
	}

	switch period {
	case "week":
		return now.AddDate(0, 0, -7), now
	case "year":
		start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, location)
		return start, now
	default:
		return now.AddDate(0, 0, -30), now
	}
}

func resolvePreviousRange(period string, start time.Time, end time.Time, location *time.Location) (time.Time, time.Time) {
	period = strings.ToLower(period)
	if period == "custom" {
		diff := end.Sub(start)
		prevEnd := start.Add(-time.Nanosecond)
		return prevEnd.Add(-diff), prevEnd
	}
	if period == "week" {
		prevEnd := start.Add(-time.Nanosecond)
		return prevEnd.AddDate(0, 0, -7), prevEnd
	}
	if period == "year" {
		previousStart := time.Date(start.Year()-1, 1, 1, 0, 0, 0, 0, location)
		previousEnd := time.Date(start.Year()-1, 12, 31, 23, 59, 59, 0, location)
		return previousStart, previousEnd
	}
	prevEnd := start.Add(-time.Nanosecond)
	return prevEnd.AddDate(0, 0, -30), prevEnd
}

func resolveSalesDashboardDates(period string) (time.Time, time.Time, time.Time) {
	now := time.Now()
	period = strings.ToLower(period)
	switch period {
	case "today":
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		prevStart := start.AddDate(0, 0, -1)
		return start, prevStart, start
	case "week":
		start := now.AddDate(0, 0, -7)
		prevStart := start.AddDate(0, 0, -7)
		return start, prevStart, start
	case "year":
		start := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		prevStart := time.Date(now.Year()-1, 1, 1, 0, 0, 0, 0, time.UTC)
		prevEnd := time.Date(now.Year()-1, 12, 31, 23, 59, 59, 0, time.UTC)
		return start, prevStart, prevEnd
	case "month":
		fallthrough
	default:
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		prevStart := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		prevEnd := time.Date(now.Year(), now.Month(), 0, 23, 59, 59, 0, time.UTC)
		return start, prevStart, prevEnd
	}
}

func parseList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0)
	seen := make(map[string]struct{})
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func parseDateValue(value string, location *time.Location) time.Time {
	if strings.TrimSpace(value) == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		if location != nil {
			return parsed.In(location)
		}
		return parsed
	}
	if location == nil {
		location = time.UTC
	}
	if parsed, err := time.ParseInLocation("2006-01-02", value, location); err == nil {
		return parsed
	}
	return time.Time{}
}

func parseIntWithDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseFloatWithDefault(value string, fallback float64) float64 {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func resolveLocation(timezone string) *time.Location {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return time.UTC
	}
	return location
}

func extractOrderIDs(orders []reportOrder) []int64 {
	ids := make([]int64, 0, len(orders))
	seen := make(map[int64]struct{})
	for _, order := range orders {
		if _, ok := seen[order.ID]; ok {
			continue
		}
		seen[order.ID] = struct{}{}
		ids = append(ids, order.ID)
	}
	return ids
}

func sortByAmount(items []map[string]any, limit int) []map[string]any {
	sort.Slice(items, func(i, j int) bool {
		return items[i]["amount"].(float64) > items[j]["amount"].(float64)
	})
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func sumOrderTotals(orders []reportOrder) float64 {
	sum := 0.0
	for _, order := range orders {
		sum += utils.NumericToFloat64(order.TotalAmount)
	}
	return sum
}

func averageValue(total float64, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return total / float64(count)
}

func buildRevenueTrend(orders []reportOrder) []map[string]any {
	agg := make(map[string]map[string]float64)
	for _, order := range orders {
		date := order.PlacedAt.Format("2006-01-02")
		entry := agg[date]
		if entry == nil {
			entry = map[string]float64{"revenue": 0, "count": 0}
		}
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		entry["count"] += 1
		agg[date] = entry
	}

	keys := make([]string, 0, len(agg))
	for date := range agg {
		keys = append(keys, date)
	}
	sort.Strings(keys)

	out := make([]map[string]any, 0, len(agg))
	for _, date := range keys {
		entry := agg[date]
		out = append(out, map[string]any{
			"date":       date,
			"revenue":    entry["revenue"],
			"orderCount": int64(entry["count"]),
		})
	}
	return out
}

func (h *Handler) buildTopMenuItemsSales(ctx context.Context, orders []reportOrder) ([]map[string]any, error) {
	orderIDs := extractOrderIDs(orders)
	if len(orderIDs) == 0 {
		return []map[string]any{}, nil
	}
	rows, err := h.DB.Query(ctx, `
        select oi.menu_id, oi.menu_name, oi.quantity, oi.subtotal, m.image_url
        from order_items oi
        left join menus m on m.id = oi.menu_id
        where oi.order_id = any($1)
    `, orderIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	agg := make(map[string]map[string]any)
	for rows.Next() {
		var (
			menuID   pgtype.Int8
			menuName pgtype.Text
			quantity int32
			subtotal pgtype.Numeric
			imageURL pgtype.Text
		)
		if err := rows.Scan(&menuID, &menuName, &quantity, &subtotal, &imageURL); err != nil {
			return nil, err
		}
		if !menuID.Valid {
			continue
		}
		menuIDStr := strconv.FormatInt(menuID.Int64, 10)
		name := "Menu"
		if menuName.Valid {
			name = menuName.String
		}
		entry := agg[menuIDStr]
		if entry == nil {
			image := any(nil)
			if imageURL.Valid {
				image = imageURL.String
			}
			entry = map[string]any{
				"menuId":   menuIDStr,
				"menuName": name,
				"quantity": int64(0),
				"revenue":  float64(0),
				"imageUrl": image,
			}
		}
		entry["quantity"] = entry["quantity"].(int64) + int64(quantity)
		entry["revenue"] = entry["revenue"].(float64) + utils.NumericToFloat64(subtotal)
		agg[menuIDStr] = entry
	}

	items := make([]map[string]any, 0, len(agg))
	for _, entry := range agg {
		items = append(items, entry)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i]["quantity"].(int64) > items[j]["quantity"].(int64)
	})
	if len(items) > 10 {
		items = items[:10]
	}
	return items, nil
}

func buildPeakHours(orders []reportOrder) []map[string]any {
	agg := make(map[int]map[string]float64)
	for _, order := range orders {
		hour := order.PlacedAt.Hour()
		entry := agg[hour]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		entry["count"] += 1
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		agg[hour] = entry
	}

	out := make([]map[string]any, 0, 24)
	for hour := 0; hour < 24; hour++ {
		entry := agg[hour]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		out = append(out, map[string]any{
			"hour":       hour,
			"orderCount": int64(entry["count"]),
			"revenue":    entry["revenue"],
		})
	}
	return out
}

func buildOrderTypeSales(orders []reportOrder) []map[string]any {
	agg := make(map[string]map[string]float64)
	for _, order := range orders {
		entry := agg[order.OrderType]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		entry["count"] += 1
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		agg[order.OrderType] = entry
	}

	total := float64(len(orders))
	out := make([]map[string]any, 0, len(agg))
	for orderType, entry := range agg {
		percentage := 0.0
		if total > 0 {
			percentage = (entry["count"] / total) * 100
		}
		out = append(out, map[string]any{
			"type":       orderType,
			"count":      int64(entry["count"]),
			"revenue":    entry["revenue"],
			"percentage": percentage,
		})
	}
	return out
}

func buildPaymentMethodSales(orders []reportOrder) []map[string]any {
	agg := make(map[string]map[string]float64)
	for _, order := range orders {
		method := "UNKNOWN"
		if order.PaymentMethod.Valid {
			method = order.PaymentMethod.String
		}
		entry := agg[method]
		if entry == nil {
			entry = map[string]float64{"count": 0, "revenue": 0}
		}
		entry["count"] += 1
		entry["revenue"] += utils.NumericToFloat64(order.TotalAmount)
		agg[method] = entry
	}

	total := float64(len(orders))
	out := make([]map[string]any, 0, len(agg))
	for method, entry := range agg {
		percentage := 0.0
		if total > 0 {
			percentage = (entry["count"] / total) * 100
		}
		out = append(out, map[string]any{
			"method":     method,
			"count":      int64(entry["count"]),
			"revenue":    entry["revenue"],
			"percentage": percentage,
		})
	}
	return out
}
