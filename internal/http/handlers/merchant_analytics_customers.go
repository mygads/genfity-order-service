package handlers

import (
	"net/http"
	"sort"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type customerOrderSnapshot struct {
	PlacedAt time.Time
	Total    float64
}

type customerAggregate struct {
	CustomerID    int64
	CustomerName  string
	CustomerPhone string
	CreatedAt     time.Time
	Orders        []customerOrderSnapshot
}

type customerGrowthRow struct {
	Date           string `json:"date"`
	NewCustomers   int    `json:"newCustomers"`
	TotalCustomers int    `json:"totalCustomers"`
}

func (h *Handler) MerchantCustomerAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	period := r.URL.Query().Get("period")
	if period == "" {
		period = "month"
	}

	now := time.Now()
	startDate := startDateForPeriod(now, period)
	cacheBucket := now.Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey("customer_analytics", *authCtx.MerchantID, period, startDate.Format("2006-01-02"), cacheBucket.Format(time.RFC3339))
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	rows, err := h.DB.Query(ctx, `
		select o.placed_at, o.total_amount, c.id, c.name, c.phone, c.created_at
		from orders o
		join customers c on c.id = o.customer_id
		where o.merchant_id = $1 and o.status = 'COMPLETED'
		order by o.placed_at desc
	`, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("customer analytics query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch customer analytics")
		return
	}
	defer rows.Close()

	customersMap := make(map[int64]*customerAggregate)
	orderCount := 0
	var totalRevenue float64

	for rows.Next() {
		var (
			placedAt   time.Time
			total      pgtype.Numeric
			customerID int64
			name       pgtype.Text
			phone      pgtype.Text
			createdAt  time.Time
		)
		if err := rows.Scan(&placedAt, &total, &customerID, &name, &phone, &createdAt); err != nil {
			continue
		}
		amount := utils.NumericToFloat64(total)
		orderCount += 1
		totalRevenue += amount

		agg := customersMap[customerID]
		if agg == nil {
			agg = &customerAggregate{
				CustomerID:    customerID,
				CustomerName:  defaultString(name.String, "Unknown"),
				CustomerPhone: textOrDefault(phone, ""),
				CreatedAt:     createdAt,
				Orders:        []customerOrderSnapshot{},
			}
			customersMap[customerID] = agg
		}
		agg.Orders = append(agg.Orders, customerOrderSnapshot{PlacedAt: placedAt, Total: amount})
	}

	customers := make([]*customerAggregate, 0, len(customersMap))
	for _, customer := range customersMap {
		customers = append(customers, customer)
	}

	totalCustomers := len(customers)
	periodCustomers := make([]*customerAggregate, 0)
	for _, customer := range customers {
		for _, order := range customer.Orders {
			if !order.PlacedAt.Before(startDate) {
				periodCustomers = append(periodCustomers, customer)
				break
			}
		}
	}

	newCustomers := 0
	returningCustomers := 0
	for _, customer := range customers {
		if !customer.CreatedAt.Before(startDate) {
			newCustomers += 1
		}
	}
	for _, customer := range periodCustomers {
		hasBefore := false
		for _, order := range customer.Orders {
			if order.PlacedAt.Before(startDate) {
				hasBefore = true
				break
			}
		}
		if hasBefore {
			returningCustomers += 1
		}
	}

	averageOrdersPerCustomer := 0.0
	averageLifetimeValue := 0.0
	if totalCustomers > 0 {
		averageOrdersPerCustomer = float64(orderCount) / float64(totalCustomers)
		averageLifetimeValue = totalRevenue / float64(totalCustomers)
	}

	repeatCustomers := 0
	segmentation := map[string]int{"new": 0, "casual": 0, "regular": 0, "vip": 0}
	for _, customer := range customers {
		count := len(customer.Orders)
		if count > 1 {
			repeatCustomers += 1
		}
		switch {
		case count == 1:
			segmentation["new"] += 1
		case count >= 2 && count <= 3:
			segmentation["casual"] += 1
		case count >= 4 && count <= 9:
			segmentation["regular"] += 1
		case count >= 10:
			segmentation["vip"] += 1
		}
	}

	retentionRate := 0.0
	if totalCustomers > 0 {
		retentionRate = (float64(repeatCustomers) / float64(totalCustomers)) * 100
	}

	growthMap := make(map[string]*customerGrowthRow)
	sortedCustomers := make([]*customerAggregate, 0, len(customers))
	for _, customer := range customers {
		sortedCustomers = append(sortedCustomers, customer)
	}
	sort.Slice(sortedCustomers, func(i, j int) bool {
		return sortedCustomers[i].CreatedAt.Before(sortedCustomers[j].CreatedAt)
	})

	cumulative := 0
	for _, customer := range sortedCustomers {
		if customer.CreatedAt.Before(startDate) {
			continue
		}
		dateKey := customer.CreatedAt.Format("2006-01-02")
		row := growthMap[dateKey]
		cumulative += 1
		if row == nil {
			growthMap[dateKey] = &customerGrowthRow{Date: dateKey, NewCustomers: 1, TotalCustomers: cumulative}
			continue
		}
		row.NewCustomers += 1
		row.TotalCustomers = cumulative
	}

	growthRows := make([]customerGrowthRow, 0, len(growthMap))
	for _, row := range growthMap {
		growthRows = append(growthRows, *row)
	}
	sort.Slice(growthRows, func(i, j int) bool { return growthRows[i].Date < growthRows[j].Date })

	frequencyRanges := []struct {
		Label string
		Min   int
		Max   int
	}{
		{"1 order", 1, 1},
		{"2-3 orders", 2, 3},
		{"4-6 orders", 4, 6},
		{"7-10 orders", 7, 10},
		{"11+ orders", 11, 999999},
	}
	orderFrequency := make([]map[string]any, 0, len(frequencyRanges))
	for _, rng := range frequencyRanges {
		count := 0
		for _, customer := range customers {
			length := len(customer.Orders)
			if length >= rng.Min && length <= rng.Max {
				count += 1
			}
		}
		percentage := 0.0
		if totalCustomers > 0 {
			percentage = (float64(count) / float64(totalCustomers)) * 100
		}
		orderFrequency = append(orderFrequency, map[string]any{
			"range":      rng.Label,
			"count":      count,
			"percentage": percentage,
		})
	}

	topCustomers := make([]map[string]any, 0)
	for _, customer := range customers {
		totalSpent := 0.0
		latest := time.Time{}
		for _, order := range customer.Orders {
			totalSpent += order.Total
			if latest.IsZero() || order.PlacedAt.After(latest) {
				latest = order.PlacedAt
			}
		}
		avgOrder := 0.0
		if len(customer.Orders) > 0 {
			avgOrder = totalSpent / float64(len(customer.Orders))
		}
		topCustomers = append(topCustomers, map[string]any{
			"customerId":    int64ToString(customer.CustomerID),
			"customerName":  customer.CustomerName,
			"customerPhone": customer.CustomerPhone,
			"totalOrders":   len(customer.Orders),
			"totalSpent":    totalSpent,
			"averageOrder":  avgOrder,
			"lastOrderDate": func() string {
				if latest.IsZero() {
					return ""
				}
				return latest.Format(time.RFC3339)
			}(),
		})
	}
	sort.Slice(topCustomers, func(i, j int) bool {
		return toFloat64(topCustomers[i]["totalSpent"]) > toFloat64(topCustomers[j]["totalSpent"])
	})
	if len(topCustomers) > 20 {
		topCustomers = topCustomers[:20]
	}

	customerRetention := make([]map[string]any, 0)
	for i := 2; i >= 0; i-- {
		cohortStart := time.Date(now.Year(), now.Month()-time.Month(i), 1, 0, 0, 0, 0, now.Location())
		cohortEnd := cohortStart.AddDate(0, 1, -1)
		cohortCustomers := make([]*customerAggregate, 0)
		for _, customer := range customers {
			if (customer.CreatedAt.Equal(cohortStart) || customer.CreatedAt.After(cohortStart)) && (customer.CreatedAt.Equal(cohortEnd) || customer.CreatedAt.Before(cohortEnd)) {
				cohortCustomers = append(cohortCustomers, customer)
			}
		}

		month1End := cohortStart.AddDate(0, 1, -1)
		month2End := cohortStart.AddDate(0, 2, -1)
		month3End := cohortStart.AddDate(0, 3, -1)

		month1Active := 0
		month2Active := 0
		month3Active := 0
		for _, customer := range cohortCustomers {
			seenMonth1 := false
			seenMonth2 := false
			seenMonth3 := false
			for _, order := range customer.Orders {
				if order.PlacedAt.After(cohortEnd) && (order.PlacedAt.Before(month1End) || order.PlacedAt.Equal(month1End)) {
					seenMonth1 = true
				}
				if order.PlacedAt.After(month1End) && (order.PlacedAt.Before(month2End) || order.PlacedAt.Equal(month2End)) {
					seenMonth2 = true
				}
				if order.PlacedAt.After(month2End) && (order.PlacedAt.Before(month3End) || order.PlacedAt.Equal(month3End)) {
					seenMonth3 = true
				}
			}
			if seenMonth1 {
				month1Active += 1
			}
			if seenMonth2 {
				month2Active += 1
			}
			if seenMonth3 {
				month3Active += 1
			}
		}

		cohortSize := len(cohortCustomers)
		percentage := func(count int) float64 {
			if cohortSize == 0 {
				return 0
			}
			return (float64(count) / float64(cohortSize)) * 100
		}

		customerRetention = append(customerRetention, map[string]any{
			"cohort":         cohortStart.Format("2006-01"),
			"totalCustomers": cohortSize,
			"month1":         percentage(month1Active),
			"month2":         percentage(month2Active),
			"month3":         percentage(month3Active),
		})
	}

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"summary": map[string]any{
				"totalCustomers":           totalCustomers,
				"newCustomers":             newCustomers,
				"returningCustomers":       returningCustomers,
				"averageOrdersPerCustomer": averageOrdersPerCustomer,
				"averageLifetimeValue":     averageLifetimeValue,
				"retentionRate":            retentionRate,
			},
			"segmentation": map[string]any{
				"new":     segmentation["new"],
				"casual":  segmentation["casual"],
				"regular": segmentation["regular"],
				"vip":     segmentation["vip"],
			},
			"customerGrowth":    growthRows,
			"orderFrequency":    orderFrequency,
			"topCustomers":      topCustomers,
			"customerRetention": customerRetention,
		},
		"meta": map[string]any{
			"period":    period,
			"startDate": startDate.Format(time.RFC3339),
			"endDate":   now.Format(time.RFC3339),
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

func startDateForPeriod(now time.Time, period string) time.Time {
	switch period {
	case "quarter":
		return now.AddDate(0, 0, -90)
	case "year":
		return time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	default:
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	}
}

func toFloat64(value any) float64 {
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}
