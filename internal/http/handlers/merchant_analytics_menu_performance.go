package handlers

import (
	"context"
	"net/http"
	"sort"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type menuPerformanceRow struct {
	MenuID           int64
	MenuName         string
	CategoryName     string
	Price            float64
	QuantitySold     int
	Revenue          float64
	OrdersWithAddons int
	TotalOrders      int
	LastOrderDate    *time.Time
	CreatedAt        time.Time
	IsActive         bool
}

func (h *Handler) MerchantMenuPerformanceAnalytics(w http.ResponseWriter, r *http.Request) {
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
	startDate := startDateForMenuPeriod(now, period)
	prevStart, prevEnd := previousMenuPeriod(now, period, startDate)
	cacheBucket := now.Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey("menu_performance", *authCtx.MerchantID, period, startDate.Format("2006-01-02"), cacheBucket.Format(time.RFC3339))
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	daysInPeriod := int(now.Sub(startDate).Hours()/24) + 1
	if daysInPeriod < 1 {
		daysInPeriod = 1
	}

	menus, err := h.loadMenuPerformanceBase(ctx, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("menu performance menu fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu performance analytics")
		return
	}

	orderItems, orderItemAddonCounts, err := h.loadMenuPerformanceOrders(ctx, *authCtx.MerchantID, startDate, now)
	if err != nil {
		h.Logger.Error("menu performance orders fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu performance analytics")
		return
	}

	previousQuantities, err := h.loadMenuPerformancePrevious(ctx, *authCtx.MerchantID, prevStart, prevEnd)
	if err != nil {
		h.Logger.Error("menu performance previous fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu performance analytics")
		return
	}

	for _, item := range orderItems {
		row := menus[item.MenuID]
		if row == nil {
			continue
		}
		row.QuantitySold += int(item.Quantity)
		row.Revenue += item.Subtotal
		row.TotalOrders += 1
		if count := orderItemAddonCounts[item.ID]; count > 0 {
			row.OrdersWithAddons += 1
		}
		if row.LastOrderDate == nil || item.PlacedAt.After(*row.LastOrderDate) {
			last := item.PlacedAt
			row.LastOrderDate = &last
		}
	}

	performance := make([]*menuPerformanceRow, 0, len(menus))
	for _, row := range menus {
		performance = append(performance, row)
	}

	totalRevenue := 0.0
	totalItemsSold := 0
	for _, row := range performance {
		totalRevenue += row.Revenue
		totalItemsSold += row.QuantitySold
	}

	summary := map[string]any{
		"totalMenuItems": len(performance),
		"activeItems":    countActiveMenus(performance),
		"totalItemsSold": totalItemsSold,
		"totalRevenue":   totalRevenue,
		"averageItemRevenue": func() float64 {
			if len(performance) == 0 {
				return 0
			}
			return totalRevenue / float64(len(performance))
		}(),
	}

	topPerformers := make([]map[string]any, 0)
	lowPerformers := make([]map[string]any, 0)
	neverOrdered := make([]map[string]any, 0)

	for _, row := range performance {
		if row.QuantitySold > 0 {
			revenuePercentage := 0.0
			if totalRevenue > 0 {
				revenuePercentage = (row.Revenue / totalRevenue) * 100
			}
			salesVelocity := float64(row.QuantitySold) / float64(daysInPeriod)
			addonAttachmentRate := 0.0
			if row.TotalOrders > 0 {
				addonAttachmentRate = (float64(row.OrdersWithAddons) / float64(row.TotalOrders)) * 100
			}
			topPerformers = append(topPerformers, map[string]any{
				"menuId":              int64ToString(row.MenuID),
				"menuName":            row.MenuName,
				"categoryName":        row.CategoryName,
				"price":               row.Price,
				"quantitySold":        row.QuantitySold,
				"revenue":             row.Revenue,
				"revenuePercentage":   revenuePercentage,
				"salesVelocity":       salesVelocity,
				"addonAttachmentRate": addonAttachmentRate,
			})

			if row.QuantitySold < 5 {
				lowPerformers = append(lowPerformers, map[string]any{
					"menuId":             int64ToString(row.MenuID),
					"menuName":           row.MenuName,
					"categoryName":       row.CategoryName,
					"price":              row.Price,
					"quantitySold":       row.QuantitySold,
					"revenue":            row.Revenue,
					"daysSinceLastOrder": daysSince(row.LastOrderDate, now),
				})
			}
		} else {
			daysActive := int(now.Sub(row.CreatedAt).Hours()/24) + 1
			neverOrdered = append(neverOrdered, map[string]any{
				"menuId":       int64ToString(row.MenuID),
				"menuName":     row.MenuName,
				"categoryName": row.CategoryName,
				"price":        row.Price,
				"daysActive":   daysActive,
			})
		}
	}

	sort.Slice(topPerformers, func(i, j int) bool {
		return toFloat64(topPerformers[i]["quantitySold"]) > toFloat64(topPerformers[j]["quantitySold"])
	})
	if len(topPerformers) > 10 {
		topPerformers = topPerformers[:10]
	}

	sort.Slice(lowPerformers, func(i, j int) bool {
		return toFloat64(lowPerformers[i]["quantitySold"]) < toFloat64(lowPerformers[j]["quantitySold"])
	})
	if len(lowPerformers) > 10 {
		lowPerformers = lowPerformers[:10]
	}

	sort.Slice(neverOrdered, func(i, j int) bool {
		return toFloat64(neverOrdered[i]["daysActive"]) > toFloat64(neverOrdered[j]["daysActive"])
	})

	categoryPerformance := buildMenuCategoryPerformance(performance, totalRevenue)
	addonPerformance := h.buildAddonPerformance(ctx, *authCtx.MerchantID, startDate, now, len(orderItems))
	salesTrendByItem := buildMenuSalesTrend(performance, previousQuantities)

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"summary":             summary,
			"topPerformers":       topPerformers,
			"lowPerformers":       lowPerformers,
			"categoryPerformance": categoryPerformance,
			"addonPerformance":    addonPerformance,
			"salesTrendByItem":    salesTrendByItem,
			"neverOrdered":        neverOrdered,
		},
		"meta": map[string]any{
			"period":       period,
			"startDate":    startDate.Format(time.RFC3339),
			"endDate":      now.Format(time.RFC3339),
			"daysInPeriod": daysInPeriod,
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

type orderItemSnapshot struct {
	ID       int64
	MenuID   int64
	Quantity int32
	Subtotal float64
	PlacedAt time.Time
}

func (h *Handler) loadMenuPerformanceBase(ctx context.Context, merchantID int64) (map[int64]*menuPerformanceRow, error) {
	rows, err := h.DB.Query(ctx, `
		select m.id, m.name, m.price, m.is_active, m.created_at, mc.name
		from menus m
		left join menu_category_items mci on mci.menu_id = m.id
		left join menu_categories mc on mc.id = mci.category_id
		where m.merchant_id = $1 and m.deleted_at is null
		order by m.id asc, mci.created_at asc
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	menus := make(map[int64]*menuPerformanceRow)
	for rows.Next() {
		var (
			id        int64
			name      string
			price     pgtype.Numeric
			isActive  bool
			createdAt time.Time
			category  pgtype.Text
		)
		if err := rows.Scan(&id, &name, &price, &isActive, &createdAt, &category); err != nil {
			continue
		}
		row := menus[id]
		if row == nil {
			row = &menuPerformanceRow{
				MenuID:       id,
				MenuName:     name,
				CategoryName: textOrDefault(category, "Uncategorized"),
				Price:        utils.NumericToFloat64(price),
				CreatedAt:    createdAt,
				IsActive:     isActive,
			}
			menus[id] = row
		}
		if row.CategoryName == "" && category.Valid {
			row.CategoryName = category.String
		}
	}
	return menus, nil
}

func (h *Handler) loadMenuPerformanceOrders(ctx context.Context, merchantID int64, startDate, endDate time.Time) ([]orderItemSnapshot, map[int64]int, error) {
	rows, err := h.DB.Query(ctx, `
		select oi.id, oi.menu_id, oi.quantity, oi.subtotal, o.placed_at
		from orders o
		join order_items oi on oi.order_id = o.id
		where o.merchant_id = $1 and o.status = 'COMPLETED' and o.placed_at >= $2 and o.placed_at <= $3
	`, merchantID, startDate, endDate)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := make([]orderItemSnapshot, 0)
	orderItemIDs := make([]int64, 0)
	for rows.Next() {
		var (
			id       int64
			menuID   int64
			quantity int32
			subtotal pgtype.Numeric
			placedAt time.Time
		)
		if err := rows.Scan(&id, &menuID, &quantity, &subtotal, &placedAt); err != nil {
			continue
		}
		items = append(items, orderItemSnapshot{
			ID:       id,
			MenuID:   menuID,
			Quantity: quantity,
			Subtotal: utils.NumericToFloat64(subtotal),
			PlacedAt: placedAt,
		})
		orderItemIDs = append(orderItemIDs, id)
	}

	addonCounts := make(map[int64]int)
	if len(orderItemIDs) > 0 {
		addonRows, err := h.DB.Query(ctx, `
			select order_item_id, count(*)
			from order_item_addons
			where order_item_id = any($1)
			group by order_item_id
		`, orderItemIDs)
		if err == nil {
			defer addonRows.Close()
			for addonRows.Next() {
				var orderItemID int64
				var count int64
				if err := addonRows.Scan(&orderItemID, &count); err != nil {
					continue
				}
				addonCounts[orderItemID] = int(count)
			}
		}
	}

	return items, addonCounts, nil
}

func (h *Handler) loadMenuPerformancePrevious(ctx context.Context, merchantID int64, startDate, endDate time.Time) (map[int64]int, error) {
	rows, err := h.DB.Query(ctx, `
		select oi.menu_id, sum(oi.quantity)
		from orders o
		join order_items oi on oi.order_id = o.id
		where o.merchant_id = $1 and o.status = 'COMPLETED' and o.placed_at >= $2 and o.placed_at <= $3
		group by oi.menu_id
	`, merchantID, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	quantities := make(map[int64]int)
	for rows.Next() {
		var menuID int64
		var total pgtype.Int8
		if err := rows.Scan(&menuID, &total); err != nil {
			continue
		}
		if total.Valid {
			quantities[menuID] = int(total.Int64)
		}
	}
	return quantities, nil
}

func (h *Handler) buildAddonPerformance(ctx context.Context, merchantID int64, startDate, endDate time.Time, totalOrderItems int) []map[string]any {
	rows, err := h.DB.Query(ctx, `
		select oia.addon_item_id, oia.quantity, ai.name, ai.price
		from order_item_addons oia
		join order_items oi on oi.id = oia.order_item_id
		join orders o on o.id = oi.order_id
		join addon_items ai on ai.id = oia.addon_item_id
		where o.merchant_id = $1 and o.status = 'COMPLETED' and o.placed_at >= $2 and o.placed_at <= $3
	`, merchantID, startDate, endDate)
	if err != nil {
		return []map[string]any{}
	}
	defer rows.Close()

	type addonAgg struct {
		AddonID    int64
		Name       string
		Price      float64
		Quantity   int
		Revenue    float64
		OrderCount int
	}

	addonMap := make(map[int64]*addonAgg)
	for rows.Next() {
		var (
			addonID  int64
			quantity int32
			name     string
			price    pgtype.Numeric
		)
		if err := rows.Scan(&addonID, &quantity, &name, &price); err != nil {
			continue
		}
		agg := addonMap[addonID]
		if agg == nil {
			agg = &addonAgg{AddonID: addonID, Name: name, Price: utils.NumericToFloat64(price)}
			addonMap[addonID] = agg
		}
		agg.Quantity += int(quantity)
		agg.Revenue += agg.Price * float64(quantity)
		agg.OrderCount += 1
	}

	result := make([]map[string]any, 0)
	for _, agg := range addonMap {
		attachmentRate := 0.0
		if totalOrderItems > 0 {
			attachmentRate = (float64(agg.OrderCount) / float64(totalOrderItems)) * 100
		}
		result = append(result, map[string]any{
			"addonId":        int64ToString(agg.AddonID),
			"addonName":      agg.Name,
			"categoryName":   "Addon",
			"price":          agg.Price,
			"quantitySold":   agg.Quantity,
			"revenue":        agg.Revenue,
			"attachmentRate": attachmentRate,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return toFloat64(result[i]["quantitySold"]) > toFloat64(result[j]["quantitySold"])
	})
	if len(result) > 10 {
		result = result[:10]
	}

	return result
}

func buildMenuCategoryPerformance(performance []*menuPerformanceRow, totalRevenue float64) []map[string]any {
	categoryMap := make(map[string]map[string]any)
	for _, row := range performance {
		key := row.CategoryName
		entry := categoryMap[key]
		if entry == nil {
			entry = map[string]any{
				"categoryId":        "",
				"categoryName":      key,
				"itemCount":         0,
				"totalQuantitySold": 0,
				"totalRevenue":      0.0,
			}
			categoryMap[key] = entry
		}
		entry["itemCount"] = toFloat64(entry["itemCount"]) + 1
		entry["totalQuantitySold"] = toFloat64(entry["totalQuantitySold"]) + float64(row.QuantitySold)
		entry["totalRevenue"] = toFloat64(entry["totalRevenue"]) + row.Revenue
	}

	categories := make([]map[string]any, 0, len(categoryMap))
	for _, entry := range categoryMap {
		revenuePercentage := 0.0
		if totalRevenue > 0 {
			revenuePercentage = toFloat64(entry["totalRevenue"]) / totalRevenue * 100
		}
		entry["revenuePercentage"] = revenuePercentage
		categories = append(categories, entry)
	}

	sort.Slice(categories, func(i, j int) bool {
		return toFloat64(categories[i]["totalRevenue"]) > toFloat64(categories[j]["totalRevenue"])
	})
	return categories
}

func buildMenuSalesTrend(performance []*menuPerformanceRow, previous map[int64]int) []map[string]any {
	trends := make([]map[string]any, 0)
	for _, row := range performance {
		if row.QuantitySold <= 0 {
			continue
		}
		previousQty := previous[row.MenuID]
		changePercent := 0.0
		trend := "stable"
		if previousQty > 0 {
			changePercent = (float64(row.QuantitySold-previousQty) / float64(previousQty)) * 100
			if changePercent > 10 {
				trend = "rising"
			} else if changePercent < -10 {
				trend = "falling"
			}
		} else if row.QuantitySold > 0 {
			changePercent = 100
			trend = "rising"
		}
		trends = append(trends, map[string]any{
			"menuId":        int64ToString(row.MenuID),
			"menuName":      row.MenuName,
			"trend":         trend,
			"changePercent": changePercent,
		})
	}

	sort.Slice(trends, func(i, j int) bool {
		return toFloat64(trends[i]["changePercent"]) > toFloat64(trends[j]["changePercent"])
	})
	if len(trends) > 10 {
		trends = trends[:10]
	}
	return trends
}

func startDateForMenuPeriod(now time.Time, period string) time.Time {
	switch period {
	case "week":
		return now.AddDate(0, 0, -7)
	case "quarter":
		return now.AddDate(0, 0, -90)
	case "year":
		return time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	default:
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	}
}

func previousMenuPeriod(now time.Time, period string, startDate time.Time) (time.Time, time.Time) {
	switch period {
	case "week":
		return startDate.AddDate(0, 0, -7), startDate.Add(-time.Nanosecond)
	case "quarter":
		return startDate.AddDate(0, 0, -90), startDate.Add(-time.Nanosecond)
	case "year":
		prevStart := time.Date(now.Year()-1, 1, 1, 0, 0, 0, 0, now.Location())
		prevEnd := time.Date(now.Year()-1, 12, 31, 23, 59, 59, 0, now.Location())
		return prevStart, prevEnd
	default:
		prevStart := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, now.Location())
		prevEnd := time.Date(now.Year(), now.Month(), 0, 23, 59, 59, 0, now.Location())
		return prevStart, prevEnd
	}
}

func countActiveMenus(rows []*menuPerformanceRow) int {
	count := 0
	for _, row := range rows {
		if row.IsActive {
			count += 1
		}
	}
	return count
}

func daysSince(date *time.Time, now time.Time) any {
	if date == nil {
		return nil
	}
	return int(now.Sub(*date).Hours() / 24)
}
