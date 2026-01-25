package handlers

import (
	"context"
	"net/http"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type OrderStatistics struct {
	TotalOrders       int64            `json:"totalOrders"`
	OrdersByStatus    map[string]int64 `json:"ordersByStatus"`
	OrdersByType      map[string]int64 `json:"ordersByType"`
	CompletedOrders   int64            `json:"completedOrders"`
	CancelledOrders   int64            `json:"cancelledOrders"`
	PendingOrders     int64            `json:"pendingOrders"`
	AverageOrderValue float64          `json:"averageOrderValue"`
}

type PaymentMethodStats struct {
	Count  int64   `json:"count"`
	Amount float64 `json:"amount"`
}

type PaymentStatistics struct {
	TotalRevenue      float64                       `json:"totalRevenue"`
	CompletedPayments int64                         `json:"completedPayments"`
	PendingPayments   int64                         `json:"pendingPayments"`
	FailedPayments    int64                         `json:"failedPayments"`
	RefundedPayments  int64                         `json:"refundedPayments"`
	ByMethod          map[string]PaymentMethodStats `json:"byMethod"`
}

func (h *Handler) MerchantOrderStats(w http.ResponseWriter, r *http.Request) {
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

	orderStats, err := h.getOrderStatistics(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("order stats query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch order stats")
		return
	}

	paymentStats, err := h.getPaymentStatistics(ctx, *authCtx.MerchantID, startDate, endDate)
	if err != nil {
		h.Logger.Error("payment stats query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch payment stats")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"orders":   orderStats,
			"payments": paymentStats,
		},
		"dateRange": map[string]string{
			"start": startDate.Format(time.RFC3339),
			"end":   endDate.Format(time.RFC3339),
		},
	})
}

func parseDateRange(r *http.Request) (time.Time, time.Time, error) {
	query := r.URL.Query()

	endDate := time.Now()
	if value := query.Get("endDate"); value != "" {
		parsed, err := parseDateParam(value)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		endDate = parsed
	}

	startDate := endDate.Add(-30 * 24 * time.Hour)
	if value := query.Get("startDate"); value != "" {
		parsed, err := parseDateParam(value)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		startDate = parsed
	}

	if startDate.After(endDate) {
		return time.Time{}, time.Time{}, errInvalidDateRange
	}

	return startDate, endDate, nil
}

var errInvalidDateRange = &handlerError{message: "startDate must be before endDate"}

type handlerError struct {
	message string
}

func (e *handlerError) Error() string {
	return e.message
}

func parseDateParam(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, &handlerError{message: "Invalid date"}
	}

	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}

	if parsed, err := time.ParseInLocation("2006-01-02", value, time.Local); err == nil {
		return parsed, nil
	}

	return time.Time{}, &handlerError{message: "Invalid date"}
}

func (h *Handler) getOrderStatistics(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time) (OrderStatistics, error) {
	stats := OrderStatistics{
		OrdersByStatus: map[string]int64{
			"PENDING":     0,
			"ACCEPTED":    0,
			"IN_PROGRESS": 0,
			"READY":       0,
			"COMPLETED":   0,
			"CANCELLED":   0,
		},
		OrdersByType: map[string]int64{
			"DINE_IN":  0,
			"TAKEAWAY": 0,
			"DELIVERY": 0,
		},
	}

	var totalAmount float64
	rows, err := h.DB.Query(ctx, `
        select status, order_type, total_amount
        from orders
        where merchant_id = $1
          and placed_at >= $2
          and placed_at <= $3
    `, merchantID, startDate, endDate)
	if err != nil {
		return OrderStatistics{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			status     string
			orderType  string
			totalValue pgtype.Numeric
		)
		if err := rows.Scan(&status, &orderType, &totalValue); err != nil {
			return OrderStatistics{}, err
		}

		stats.TotalOrders++
		stats.OrdersByStatus[status]++
		stats.OrdersByType[orderType]++
		totalAmount += utils.NumericToFloat64(totalValue)

		switch status {
		case "COMPLETED":
			stats.CompletedOrders++
		case "CANCELLED":
			stats.CancelledOrders++
		case "PENDING":
			stats.PendingOrders++
		}
	}

	if stats.TotalOrders > 0 {
		stats.AverageOrderValue = totalAmount / float64(stats.TotalOrders)
	}

	return stats, nil
}

func (h *Handler) getPaymentStatistics(ctx context.Context, merchantID int64, startDate time.Time, endDate time.Time) (PaymentStatistics, error) {
	stats := PaymentStatistics{
		ByMethod: make(map[string]PaymentMethodStats),
	}

	rows, err := h.DB.Query(ctx, `
        select p.status, p.payment_method, p.amount
        from payments p
        join orders o on o.id = p.order_id
        where o.merchant_id = $1
          and o.placed_at >= $2
          and o.placed_at <= $3
    `, merchantID, startDate, endDate)
	if err != nil {
		return PaymentStatistics{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			status        string
			paymentMethod pgtype.Text
			amountValue   pgtype.Numeric
		)
		if err := rows.Scan(&status, &paymentMethod, &amountValue); err != nil {
			return PaymentStatistics{}, err
		}

		amount := utils.NumericToFloat64(amountValue)

		switch status {
		case "COMPLETED":
			stats.TotalRevenue += amount
			stats.CompletedPayments++
		case "PENDING":
			stats.PendingPayments++
		case "FAILED":
			stats.FailedPayments++
		case "REFUNDED":
			stats.RefundedPayments++
			stats.TotalRevenue -= amount
		}

		method := "UNKNOWN"
		if paymentMethod.Valid && paymentMethod.String != "" {
			method = paymentMethod.String
		}

		current := stats.ByMethod[method]
		current.Count++
		if status == "COMPLETED" {
			current.Amount += amount
		}
		stats.ByMethod[method] = current
	}

	return stats, nil
}
