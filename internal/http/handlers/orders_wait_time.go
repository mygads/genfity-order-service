package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type basePrepCacheEntry struct {
	value     int
	expiresAt time.Time
}

var (
	basePrepCacheMu sync.Mutex
	basePrepCache   = make(map[string]basePrepCacheEntry)
)

func (h *Handler) PublicOrderWaitTime(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	query := `
		select o.order_number, o.status, o.order_type, o.placed_at, o.updated_at,
		       o.is_scheduled, o.scheduled_date, o.scheduled_time,
		       m.id, m.code, m.timezone
		from orders o
		join merchants m on m.id = o.merchant_id
		where o.order_number = $1
		limit 1
	`

	var (
		orderNum         string
		status           string
		orderType        string
		placedAt         time.Time
		updatedAt        time.Time
		isScheduled      bool
		scheduledDate    pgtype.Text
		scheduledTime    pgtype.Text
		merchantID       int64
		merchantCode     string
		merchantTimezone pgtype.Text
	)

	if err := h.DB.QueryRow(ctx, query, orderNumber).Scan(
		&orderNum, &status, &orderType, &placedAt, &updatedAt,
		&isScheduled, &scheduledDate, &scheduledTime,
		&merchantID, &merchantCode, &merchantTimezone,
	); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode, orderNum) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if isFinalStatus(status) {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"minMinutes":      0,
				"maxMinutes":      0,
				"cappedAt60":      false,
				"queueAhead":      0,
				"basePrepMinutes": nil,
				"status":          status,
			},
		})
		return
	}

	basePrep := h.getBasePrepMinutesCached(ctx, merchantID, orderType)

	mz := "UTC"
	if merchantTimezone.Valid {
		mz = merchantTimezone.String
	}

	var scheduledAt *time.Time
	if isScheduled && scheduledDate.Valid && scheduledTime.Valid {
		if dt := parseScheduledAt(scheduledDate.String, scheduledTime.String, mz); dt != nil {
			scheduledAt = dt
		}
	}

	queueAhead := 0
	queuePosition := any(nil)
	if status == "PENDING" || status == "ACCEPTED" {
		queueAhead = h.computeQueueAhead(ctx, merchantID, orderType, placedAt)
		queuePosition = queueAhead + 1
	}

	now := time.Now()
	if scheduledAt != nil && scheduledAt.After(now) && (status == "PENDING" || status == "ACCEPTED") {
		minutesUntil := clampWaitTimeInt(int(math.Round(minutesBetween(now, *scheduledAt))), 0, 60)
		slack := clampWaitTimeInt(int(math.Round(float64(basePrep)*0.25)), 2, 15)
		minMinutes := clampWaitTimeInt(minutesUntil-slack, 0, 60)
		maxMinutes := clampWaitTimeInt(minutesUntil+slack, minMinutes, 60)
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"minMinutes":      minMinutes,
				"maxMinutes":      maxMinutes,
				"cappedAt60":      maxMinutes >= 60,
				"queueAhead":      0,
				"queuePosition":   nil,
				"basePrepMinutes": basePrep,
				"status":          status,
				"isScheduled":     true,
			},
		})
		return
	}

	multiplier := 1
	if status == "PENDING" {
		multiplier = queueAhead + 1
	} else if status == "ACCEPTED" {
		multiplier = int(math.Max(1, math.Ceil(float64(queueAhead+1)*0.7)))
	}

	totalEstimate := clampWaitTimeInt(int(math.Round(float64(basePrep)*float64(multiplier))), 5, 60)
	elapsedFrom := placedAt
	if status == "IN_PROGRESS" {
		elapsedFrom = updatedAt
	}
	elapsed := math.Max(0, minutesBetween(elapsedFrom, now))
	remaining := clampWaitTimeInt(int(math.Round(float64(totalEstimate)-elapsed)), 0, 60)
	if remaining <= 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"minMinutes":      0,
				"maxMinutes":      0,
				"cappedAt60":      totalEstimate >= 60,
				"queueAhead":      queueAhead,
				"queuePosition":   queuePosition,
				"basePrepMinutes": basePrep,
				"status":          status,
				"isScheduled":     false,
			},
		})
		return
	}

	minMinutes := clampWaitTimeInt(int(math.Round(float64(remaining)*0.75)), 1, 60)
	maxMinutes := clampWaitTimeInt(int(math.Round(float64(remaining)*1.25)), minMinutes, 60)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"minMinutes":      minMinutes,
			"maxMinutes":      maxMinutes,
			"cappedAt60":      maxMinutes >= 60,
			"queueAhead":      queueAhead,
			"queuePosition":   queuePosition,
			"basePrepMinutes": basePrep,
			"status":          status,
			"isScheduled":     false,
		},
	})
}

func (h *Handler) getBasePrepMinutesCached(ctx context.Context, merchantID int64, orderType string) int {
	key := fmt.Sprintf("%d:%s", merchantID, orderType)
	basePrepCacheMu.Lock()
	entry, ok := basePrepCache[key]
	if ok && entry.expiresAt.After(time.Now()) {
		basePrepCacheMu.Unlock()
		return entry.value
	}
	basePrepCacheMu.Unlock()

	value := h.computeBasePrepMinutes(ctx, merchantID, orderType)

	basePrepCacheMu.Lock()
	basePrepCache[key] = basePrepCacheEntry{value: value, expiresAt: time.Now().Add(2 * time.Minute)}
	if len(basePrepCache) > 500 {
		basePrepCache = make(map[string]basePrepCacheEntry)
	}
	basePrepCacheMu.Unlock()

	return value
}

func (h *Handler) computeBasePrepMinutes(ctx context.Context, merchantID int64, orderType string) int {
	query := `
		select placed_at, actual_ready_at, completed_at
		from orders
		where merchant_id = $1
		  and order_type = $2
		  and status = 'COMPLETED'
		  and (actual_ready_at is not null or completed_at is not null)
		order by placed_at desc
		limit 60
	`

	rows, err := h.DB.Query(ctx, query, merchantID, orderType)
	if err != nil {
		return 20
	}
	defer rows.Close()

	samples := make([]int, 0)
	for rows.Next() {
		var placedAt time.Time
		var actualReady pgtype.Timestamptz
		var completedAt pgtype.Timestamptz
		if err := rows.Scan(&placedAt, &actualReady, &completedAt); err != nil {
			continue
		}
		end := time.Time{}
		if actualReady.Valid {
			end = actualReady.Time
		} else if completedAt.Valid {
			end = completedAt.Time
		}
		if end.IsZero() {
			continue
		}
		minutes := int(math.Round(minutesBetween(placedAt, end)))
		if minutes < 2 || minutes > 120 {
			continue
		}
		samples = append(samples, minutes)
	}

	if len(samples) >= 5 {
		sort.Ints(samples)
		mid := len(samples) / 2
		med := samples[mid]
		if len(samples)%2 == 0 {
			med = int(math.Round(float64(samples[mid-1]+samples[mid]) / 2))
		}
		return clampWaitTimeInt(med, 5, 60)
	}

	return 20
}

func (h *Handler) computeQueueAhead(ctx context.Context, merchantID int64, orderType string, placedAt time.Time) int {
	query := `
		select count(*)
		from orders
		where merchant_id = $1
		  and order_type = $2
		  and status in ('PENDING', 'ACCEPTED', 'IN_PROGRESS')
		  and placed_at < $3
	`

	var count int
	if err := h.DB.QueryRow(ctx, query, merchantID, orderType, placedAt).Scan(&count); err != nil {
		return 0
	}
	return count
}

func isFinalStatus(status string) bool {
	return status == "READY" || status == "COMPLETED" || status == "CANCELLED"
}

func minutesBetween(a time.Time, b time.Time) float64 {
	return b.Sub(a).Minutes()
}

func clampWaitTimeInt(value int, min int, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func parseScheduledAt(dateStr, timeStr, tz string) *time.Time {
	if dateStr == "" || timeStr == "" {
		return nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	parsed, err := time.ParseInLocation("2006-01-02 15:04", dateStr+" "+timeStr, loc)
	if err != nil {
		return nil
	}
	return &parsed
}
