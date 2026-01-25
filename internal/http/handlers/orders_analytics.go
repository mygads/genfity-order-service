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

type PopularItem struct {
	MenuID     int64   `json:"menuId"`
	MenuName   string  `json:"menuName"`
	Quantity   int64   `json:"quantity"`
	Revenue    float64 `json:"revenue"`
	OrderCount int64   `json:"orderCount"`
}

type RevenueByDate struct {
	Date       string  `json:"date"`
	Revenue    float64 `json:"revenue"`
	OrderCount int64   `json:"orderCount"`
}

type PeakHour struct {
	Hour       int   `json:"hour"`
	OrderCount int64 `json:"orderCount"`
}

type AnalyticsData struct {
	Statistics    OrderStatistics   `json:"statistics"`
	PaymentStats  PaymentStatistics `json:"paymentStats"`
	PopularItems  []PopularItem     `json:"popularItems"`
	RevenueByDate []RevenueByDate   `json:"revenueByDate"`
	PeakHours     []PeakHour        `json:"peakHours"`
}

func (h *Handler) MerchantOrderAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	startDate, endDate, err := parseDateRange(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	stats, err := h.getOrderStatistics(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("order analytics stats failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch analytics data")
		return
	}

	payments, err := h.getPaymentStatistics(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("order analytics payments failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch analytics data")
		return
	}

	popularItems, err := h.getPopularItems(ctx, *authCtx.MerchantID, startDate, endDate, 10)
	if err != nil {
		h.Logger.Error("order analytics popular items failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch analytics data")
		return
	}

	revenueByDate, err := h.getRevenueByDate(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("order analytics revenue failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch analytics data")
		return
	}

	peakHours, err := h.getPeakHours(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("order analytics peak hours failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch analytics data")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": AnalyticsData{
			Statistics:    stats,
			PaymentStats:  payments,
			PopularItems:  popularItems,
			RevenueByDate: revenueByDate,
			PeakHours:     peakHours,
		},
		"dateRange": map[string]string{
			"start": startDate.Format(time.RFC3339),
			"end":   endDate.Format(time.RFC3339),
		},
	})
}

func (h *Handler) getPopularItems(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time, limit int) ([]PopularItem, error) {
	rows, err := h.DB.Query(ctx, `
		select oi.menu_id, oi.menu_name, oi.quantity, oi.subtotal
		from order_items oi
		join orders o on o.id = oi.order_id
		where o.merchant_id = $1
		  and o.placed_at >= $2
		  and o.placed_at <= $3
		  and o.status = 'COMPLETED'
		  and oi.menu_id is not null
	`, merchantID, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type agg struct {
		item PopularItem
	}
	items := make(map[int64]*agg)

	for rows.Next() {
		var (
			menuID   int64
			menuName string
			quantity int32
			subtotal pgtype.Numeric
		)
		if err := rows.Scan(&menuID, &menuName, &quantity, &subtotal); err != nil {
			return nil, err
		}

		existing := items[menuID]
		if existing == nil {
			existing = &agg{item: PopularItem{MenuID: menuID, MenuName: menuName}}
			items[menuID] = existing
		}
		existing.item.Quantity += int64(quantity)
		existing.item.Revenue += utils.NumericToFloat64(subtotal)
		existing.item.OrderCount++
	}

	result := make([]PopularItem, 0, len(items))
	for _, entry := range items {
		result = append(result, entry.item)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Quantity > result[j].Quantity
	})

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}

	return result, nil
}

func (h *Handler) getRevenueByDate(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time) ([]RevenueByDate, error) {
	rows, err := h.DB.Query(ctx, `
		select o.placed_at, o.total_amount
		from orders o
		join payments p on p.order_id = o.id and p.status = 'COMPLETED'
		where o.merchant_id = $1
		  and o.placed_at >= $2
		  and o.placed_at <= $3
		order by o.placed_at asc
	`, merchantID, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byDate := make(map[string]*RevenueByDate)
	for rows.Next() {
		var (
			placedAt time.Time
			total    pgtype.Numeric
		)
		if err := rows.Scan(&placedAt, &total); err != nil {
			return nil, err
		}

		dateKey := placedAt.Format("2006-01-02")
		entry := byDate[dateKey]
		if entry == nil {
			entry = &RevenueByDate{Date: dateKey}
			byDate[dateKey] = entry
		}
		entry.Revenue += utils.NumericToFloat64(total)
		entry.OrderCount++
	}

	result := make([]RevenueByDate, 0, len(byDate))
	for _, value := range byDate {
		result = append(result, *value)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Date < result[j].Date
	})

	return result, nil
}

func (h *Handler) getPeakHours(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time) ([]PeakHour, error) {
	rows, err := h.DB.Query(ctx, `
		select extract(hour from placed_at) as hour, count(*)
		from orders
		where merchant_id = $1
		  and placed_at >= $2
		  and placed_at <= $3
		group by hour
		order by hour
	`, merchantID, startDate, endDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hourCounts := make([]PeakHour, 24)
	for i := 0; i < 24; i++ {
		hourCounts[i] = PeakHour{Hour: i, OrderCount: 0}
	}

	for rows.Next() {
		var (
			hour  float64
			count int64
		)
		if err := rows.Scan(&hour, &count); err != nil {
			return nil, err
		}

		idx := int(hour)
		if idx >= 0 && idx < 24 {
			hourCounts[idx].OrderCount = count
		}
	}

	return hourCounts, nil
}
