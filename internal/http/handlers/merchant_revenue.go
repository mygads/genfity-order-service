package handlers

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) MerchantRevenue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var currency pgtype.Text
	if err := h.DB.QueryRow(ctx, `select currency from merchants where id = $1`, *authCtx.MerchantID).Scan(&currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	startDate, endDate := resolveRevenueRange(r)
	cacheBucket := time.Now().Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey(
		"revenue",
		*authCtx.MerchantID,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
		cacheBucket.Format(time.RFC3339),
	)
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	rows, err := h.DB.Query(ctx, `
		select o.placed_at, o.status, o.order_type, o.subtotal, o.tax_amount, o.service_charge_amount,
		       o.packaging_fee, o.total_amount
		from orders o
		join payments p on p.order_id = o.id
		where o.merchant_id = $1
		  and o.placed_at >= $2
		  and o.placed_at <= $3
		  and p.status = 'COMPLETED'
		order by o.placed_at asc
	`, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("revenue query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve revenue data")
		return
	}
	defer rows.Close()

	dailyMap := make(map[string]map[string]any)
	statusCounts := make(map[string]int)
	orderTypeMap := make(map[string]map[string]any)
	var (
		totalOrders        int
		totalSubtotal      float64
		totalTax           float64
		totalServiceCharge float64
		totalPackagingFee  float64
		grandTotal         float64
	)

	hourly := make([]map[string]any, 24)
	for i := 0; i < 24; i++ {
		hourly[i] = map[string]any{"hour": i, "orderCount": 0, "revenue": 0.0}
	}

	for rows.Next() {
		var (
			placedAt            time.Time
			status              string
			orderType           string
			subtotal            pgtype.Numeric
			taxAmount           pgtype.Numeric
			serviceChargeAmount pgtype.Numeric
			packagingFeeAmount  pgtype.Numeric
			totalAmount         pgtype.Numeric
		)
		if err := rows.Scan(&placedAt, &status, &orderType, &subtotal, &taxAmount, &serviceChargeAmount, &packagingFeeAmount, &totalAmount); err != nil {
			continue
		}

		day := placedAt.Format("2006-01-02")
		row := dailyMap[day]
		if row == nil {
			row = map[string]any{
				"date":               day,
				"totalOrders":        0,
				"totalRevenue":       0.0,
				"totalTax":           0.0,
				"totalServiceCharge": 0.0,
				"totalPackagingFee":  0.0,
				"grandTotal":         0.0,
			}
			dailyMap[day] = row
		}

		sub := utils.NumericToFloat64(subtotal)
		tax := utils.NumericToFloat64(taxAmount)
		serviceCharge := utils.NumericToFloat64(serviceChargeAmount)
		packagingFee := utils.NumericToFloat64(packagingFeeAmount)
		total := utils.NumericToFloat64(totalAmount)

		row["totalOrders"] = int(toFloat64(row["totalOrders"]) + 1)
		row["totalRevenue"] = toFloat64(row["totalRevenue"]) + sub
		row["totalTax"] = toFloat64(row["totalTax"]) + tax
		row["totalServiceCharge"] = toFloat64(row["totalServiceCharge"]) + serviceCharge
		row["totalPackagingFee"] = toFloat64(row["totalPackagingFee"]) + packagingFee
		row["grandTotal"] = toFloat64(row["grandTotal"]) + total

		totalOrders += 1
		totalSubtotal += sub
		totalTax += tax
		totalServiceCharge += serviceCharge
		totalPackagingFee += packagingFee
		grandTotal += total

		statusCounts[status] += 1

		typeRow := orderTypeMap[orderType]
		if typeRow == nil {
			typeRow = map[string]any{"type": orderType, "count": 0, "revenue": 0.0}
			orderTypeMap[orderType] = typeRow
		}
		typeRow["count"] = int(toFloat64(typeRow["count"]) + 1)
		typeRow["revenue"] = toFloat64(typeRow["revenue"]) + total

		hour := placedAt.Hour()
		hourRow := hourly[hour]
		hourRow["orderCount"] = int(toFloat64(hourRow["orderCount"]) + 1)
		hourRow["revenue"] = toFloat64(hourRow["revenue"]) + total
	}

	dailyRevenue := make([]map[string]any, 0, len(dailyMap))
	for _, row := range dailyMap {
		dailyRevenue = append(dailyRevenue, row)
	}
	sort.Slice(dailyRevenue, func(i, j int) bool {
		return StringValue(dailyRevenue[i]["date"]) < StringValue(dailyRevenue[j]["date"])
	})

	orderStatusBreakdown := make([]map[string]any, 0, len(statusCounts))
	for status, count := range statusCounts {
		orderStatusBreakdown = append(orderStatusBreakdown, map[string]any{
			"status": status,
			"count":  count,
		})
	}

	orderTypeBreakdown := make([]map[string]any, 0, len(orderTypeMap))
	for _, row := range orderTypeMap {
		orderTypeBreakdown = append(orderTypeBreakdown, row)
	}

	topMenuItems := h.fetchRevenueTopMenus(ctx, *authCtx.MerchantID, startDate, endDate)

	avgOrderValue := 0.0
	if totalOrders > 0 {
		avgOrderValue = grandTotal / float64(totalOrders)
	}

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"dateRange": map[string]any{
				"startDate": startDate.Format(time.RFC3339),
				"endDate":   endDate.Format(time.RFC3339),
			},
			"merchant": map[string]any{
				"currency": textOrDefault(currency, "AUD"),
			},
			"summary": map[string]any{
				"totalOrders":        totalOrders,
				"totalRevenue":       totalSubtotal,
				"totalTax":           totalTax,
				"totalServiceCharge": totalServiceCharge,
				"totalPackagingFee":  totalPackagingFee,
				"grandTotal":         grandTotal,
				"averageOrderValue":  avgOrderValue,
			},
			"dailyRevenue":         dailyRevenue,
			"orderStatusBreakdown": orderStatusBreakdown,
			"orderTypeBreakdown":   orderTypeBreakdown,
			"topMenuItems":         topMenuItems,
			"hourlyDistribution":   hourly,
		},
		"message":    "Revenue analytics retrieved successfully",
		"statusCode": 200,
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

func resolveRevenueRange(r *http.Request) (time.Time, time.Time) {
	query := r.URL.Query()
	startDateStr := strings.TrimSpace(query.Get("startDate"))
	endDateStr := strings.TrimSpace(query.Get("endDate"))

	endDate := time.Now()
	if endDateStr != "" {
		if parsed, err := parseDateInput(endDateStr); err == nil {
			endDate = parsed
		}
	}
	endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 23, 59, 59, 0, endDate.Location())

	startDate := endDate.AddDate(0, 0, -30)
	if startDateStr != "" {
		if parsed, err := parseDateInput(startDateStr); err == nil {
			startDate = parsed
		}
	}
	startDate = time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, startDate.Location())

	return startDate, endDate
}

func (h *Handler) fetchRevenueTopMenus(ctx context.Context, merchantID int64, startDate, endDate time.Time) []map[string]any {
	rows, err := h.DB.Query(ctx, `
		select oi.menu_id, oi.menu_name, sum(oi.quantity), sum(oi.subtotal)
		from order_items oi
		join orders o on o.id = oi.order_id
		join payments p on p.order_id = o.id
		where o.merchant_id = $1
		  and o.placed_at >= $2
		  and o.placed_at <= $3
		  and p.status = 'COMPLETED'
		group by oi.menu_id, oi.menu_name
		order by sum(oi.subtotal) desc
		limit 10
	`, merchantID, startDate, endDate)
	if err != nil {
		return []map[string]any{}
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			menuID   int64
			menuName string
			qty      pgtype.Int8
			subtotal pgtype.Numeric
		)
		if err := rows.Scan(&menuID, &menuName, &qty, &subtotal); err != nil {
			continue
		}
		items = append(items, map[string]any{
			"menuId":   int64ToString(menuID),
			"menuName": menuName,
			"totalQuantity": func() int64 {
				if qty.Valid {
					return qty.Int64
				}
				return 0
			}(),
			"totalRevenue": utils.NumericToFloat64(subtotal),
		})
	}
	return items
}
