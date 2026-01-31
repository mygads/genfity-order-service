package handlers

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

var feedbackPositiveWords = []string{
	"good", "great", "excellent", "amazing", "love", "friendly", "fast", "quick",
	"tasty", "delicious", "nice", "satisfied", "mantap", "enak", "lezat", "ramah",
	"cepat", "bagus", "puas", "mantul", "recommended", "recommend",
}

var feedbackNegativeWords = []string{
	"bad", "slow", "late", "cold", "rude", "burnt", "dirty", "expensive", "small",
	"poor", "not good", "delay", "overpriced", "mahal", "lama", "dingin", "kurang",
	"kecewa", "jelek", "kotor", "asin", "gosong",
}

var feedbackTagKeywords = map[string][]string{
	"service":     {"service", "staff", "waiter", "cashier", "ramah", "pelayanan", "kasir"},
	"food":        {"food", "taste", "flavor", "tasty", "delicious", "enak", "lezat", "rasa", "makanan"},
	"delivery":    {"delivery", "driver", "courier", "antar", "pengantaran"},
	"price":       {"price", "expensive", "cheap", "mahal", "murah", "value"},
	"portion":     {"portion", "portion size", "small", "besar", "porsi", "portion"},
	"cleanliness": {"clean", "dirty", "kotor", "bersih"},
	"packaging":   {"packaging", "package", "kemasan", "bungkus"},
	"speed":       {"fast", "quick", "slow", "late", "cepat", "lama"},
}

func (h *Handler) MerchantFeedbackList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_REQUIRED", "Merchant context required")
		return
	}

	query := r.URL.Query()
	page := parseQueryIntValue(query.Get("page"), 1)
	limit := parseQueryIntValue(query.Get("limit"), 20)
	if limit > 100 {
		limit = 100
	}
	minRating := parseQueryIntValue(query.Get("minRating"), 1)
	maxRating := parseQueryIntValue(query.Get("maxRating"), 5)
	startDate := strings.TrimSpace(query.Get("startDate"))
	endDate := strings.TrimSpace(query.Get("endDate"))
	sentiment := strings.TrimSpace(query.Get("sentiment"))
	tag := strings.TrimSpace(query.Get("tag"))
	search := strings.TrimSpace(query.Get("search"))

	where := []string{"merchant_id = $1"}
	args := []any{*authCtx.MerchantID}
	idx := 1

	if minRating > 1 || maxRating < 5 {
		idx += 1
		where = append(where, "overall_rating >= $"+intToString(idx))
		args = append(args, clamp(minRating, 1, 5))
		idx += 1
		where = append(where, "overall_rating <= $"+intToString(idx))
		args = append(args, clamp(maxRating, 1, 5))
	}

	if startDate != "" {
		if parsed, err := parseDateInput(startDate); err == nil {
			idx += 1
			where = append(where, "created_at >= $"+intToString(idx))
			args = append(args, parsed)
		}
	}
	if endDate != "" {
		if parsed, err := parseDateInput(endDate); err == nil {
			idx += 1
			where = append(where, "created_at <= $"+intToString(idx))
			args = append(args, parsed)
		}
	}

	if search != "" {
		idx += 1
		where = append(where, "(comment ilike $"+intToString(idx)+" or order_number ilike $"+intToString(idx)+")")
		args = append(args, "%"+search+"%")
	}

	requiresClientFilter := sentiment != "" || tag != ""
	offset := (page - 1) * limit
	limitClause := ""
	if !requiresClientFilter {
		idx += 1
		args = append(args, limit)
		limitClause = " limit $" + intToString(idx)
		idx += 1
		args = append(args, offset)
		limitClause += " offset $" + intToString(idx)
	}

	querySQL := `
		select id, order_number, overall_rating, service_rating, food_rating, order_completion_minutes,
		       comment, created_at
		from order_feedbacks
		where ` + strings.Join(where, " and ") +
		` order by created_at desc` + limitClause

	rows, err := h.DB.Query(ctx, querySQL, args...)
	if err != nil {
		h.Logger.Error("feedback list query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch feedback")
		return
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id                int64
			orderNumber       string
			overallRating     int32
			serviceRating     pgtype.Int4
			foodRating        pgtype.Int4
			completionMinutes pgtype.Int4
			comment           pgtype.Text
			createdAt         time.Time
		)
		if err := rows.Scan(&id, &orderNumber, &overallRating, &serviceRating, &foodRating, &completionMinutes, &comment, &createdAt); err != nil {
			continue
		}

		commentValue := textOrDefault(comment, "")
		sentimentValue := feedbackSentiment(commentValue, int(overallRating))
		tags := feedbackTags(commentValue)

		item := map[string]any{
			"id":                     int64ToString(id),
			"orderNumber":            orderNumber,
			"overallRating":          overallRating,
			"serviceRating":          nullIfInvalidInt(serviceRating),
			"foodRating":             nullIfInvalidInt(foodRating),
			"orderCompletionMinutes": nullIfInvalidInt(completionMinutes),
			"comment":                nullIfEmpty(commentValue),
			"createdAt":              createdAt,
			"sentiment":              sentimentValue,
			"tags":                   tags,
		}

		items = append(items, item)
	}

	filtered := items
	if requiresClientFilter {
		filtered = make([]map[string]any, 0)
		for _, item := range items {
			if sentiment != "" && StringValue(item["sentiment"]) != sentiment {
				continue
			}
			if tag != "" {
				match := false
				if list, ok := item["tags"].([]string); ok {
					for _, t := range list {
						if t == tag {
							match = true
							break
						}
					}
				}
				if !match {
					continue
				}
			}
			filtered = append(filtered, item)
		}
	}

	totalCount := 0
	if requiresClientFilter {
		totalCount = len(filtered)
	} else {
		countQuery := `select count(*) from order_feedbacks where ` + strings.Join(where, " and ")
		if err := h.DB.QueryRow(ctx, countQuery, args[:len(args)-2]...).Scan(&totalCount); err != nil {
			totalCount = len(filtered)
		}
	}

	if requiresClientFilter {
		start := offset
		end := offset + limit
		if start > len(filtered) {
			start = len(filtered)
		}
		if end > len(filtered) {
			end = len(filtered)
		}
		filtered = filtered[start:end]
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    filtered,
		"meta": map[string]any{
			"page":       page,
			"limit":      limit,
			"totalCount": totalCount,
			"totalPages": func() int {
				if limit == 0 {
					return 0
				}
				return int((totalCount + limit - 1) / limit)
			}(),
		},
	})
}

func (h *Handler) MerchantFeedbackAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_REQUIRED", "Merchant context required")
		return
	}

	now := time.Now()
	period := defaultString(r.URL.Query().Get("period"), "month")
	startDate := startDateForFeedbackPeriod(now, period)
	cacheBucket := now.Truncate(5 * time.Minute)
	cacheKey := analyticsCacheKey("feedback_analytics", *authCtx.MerchantID, period, startDate.Format("2006-01-02"), cacheBucket.Format(time.RFC3339))
	if cached, ok := getAnalyticsCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, cached)
		return
	}

	var timezone pgtype.Text
	_ = h.DB.QueryRow(ctx, `select timezone from merchants where id = $1`, *authCtx.MerchantID).Scan(&timezone)
	loc := loadTimezone(textOrDefault(timezone, "Asia/Jakarta"))

	rows, err := h.DB.Query(ctx, `
		select overall_rating, service_rating, food_rating, order_completion_minutes, comment, created_at
		from order_feedbacks
		where merchant_id = $1 and created_at >= $2
		order by created_at asc
	`, *authCtx.MerchantID, startDate)
	if err != nil {
		h.Logger.Error("feedback analytics query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch feedback analytics")
		return
	}
	defer rows.Close()

	feedbackRows := make([]map[string]any, 0)
	for rows.Next() {
		var (
			overallRating     int32
			serviceRating     pgtype.Int4
			foodRating        pgtype.Int4
			completionMinutes pgtype.Int4
			comment           pgtype.Text
			createdAt         time.Time
		)
		if err := rows.Scan(&overallRating, &serviceRating, &foodRating, &completionMinutes, &comment, &createdAt); err != nil {
			continue
		}
		feedbackRows = append(feedbackRows, map[string]any{
			"overallRating":     int(overallRating),
			"serviceRating":     nullIfInvalidInt(serviceRating),
			"foodRating":        nullIfInvalidInt(foodRating),
			"completionMinutes": nullIfInvalidInt(completionMinutes),
			"comment":           textOrDefault(comment, ""),
			"createdAt":         createdAt,
		})
	}

	totalFeedback := len(feedbackRows)
	sumOverall := 0
	serviceCount := 0
	serviceSum := 0
	foodCount := 0
	foodSum := 0
	completionCount := 0
	completionSum := 0

	sentimentCounts := map[string]int{"positive": 0, "neutral": 0, "negative": 0}
	tagCounts := make(map[string]int)
	ratingCounts := make(map[int]int)
	for i := 1; i <= 5; i++ {
		ratingCounts[i] = 0
	}

	trendMap := make(map[string]map[string]any)

	for _, row := range feedbackRows {
		overall := 0
		if value, ok := row["overallRating"].(int); ok {
			overall = value
		}
		sumOverall += overall
		ratingCounts[overall] += 1

		if value, ok := row["serviceRating"].(*int32); ok && value != nil {
			serviceCount += 1
			serviceSum += int(*value)
		}
		if value, ok := row["foodRating"].(*int32); ok && value != nil {
			foodCount += 1
			foodSum += int(*value)
		}
		if value, ok := row["completionMinutes"].(*int32); ok && value != nil {
			completionCount += 1
			completionSum += int(*value)
		}

		comment := StringValue(row["comment"])
		sentiment := feedbackSentiment(comment, overall)
		sentimentCounts[sentiment] += 1
		for _, tag := range feedbackTags(comment) {
			tagCounts[tag] += 1
		}

		dateKey := row["createdAt"].(time.Time).In(loc).Format("2006-01-02")
		trend := trendMap[dateKey]
		if trend == nil {
			trend = map[string]any{"date": dateKey, "count": 0, "sumRating": 0}
			trendMap[dateKey] = trend
		}
		trend["count"] = int(toFloat64(trend["count"]) + 1)
		trend["sumRating"] = int(toFloat64(trend["sumRating"]) + float64(overall))
	}

	averageOverall := 0.0
	if totalFeedback > 0 {
		averageOverall = float64(sumOverall) / float64(totalFeedback)
	}

	averageService := (*float64)(nil)
	if serviceCount > 0 {
		value := float64(serviceSum) / float64(serviceCount)
		averageService = &value
	}

	averageFood := (*float64)(nil)
	if foodCount > 0 {
		value := float64(foodSum) / float64(foodCount)
		averageFood = &value
	}

	averageCompletion := (*float64)(nil)
	if completionCount > 0 {
		value := float64(completionSum) / float64(completionCount)
		averageCompletion = &value
	}

	sentimentDistribution := make([]map[string]any, 0, 3)
	for _, key := range []string{"positive", "neutral", "negative"} {
		count := sentimentCounts[key]
		percentage := 0.0
		if totalFeedback > 0 {
			percentage = float64(count) / float64(totalFeedback) * 100
		}
		sentimentDistribution = append(sentimentDistribution, map[string]any{
			"sentiment":  key,
			"count":      count,
			"percentage": percentage,
		})
	}

	topTags := make([]map[string]any, 0)
	for tag, count := range tagCounts {
		percentage := 0.0
		if totalFeedback > 0 {
			percentage = float64(count) / float64(totalFeedback) * 100
		}
		topTags = append(topTags, map[string]any{"tag": tag, "count": count, "percentage": percentage})
	}
	sort.Slice(topTags, func(i, j int) bool {
		return toFloat64(topTags[i]["count"]) > toFloat64(topTags[j]["count"])
	})
	if len(topTags) > 8 {
		topTags = topTags[:8]
	}

	ratingDistribution := make([]map[string]any, 0, 5)
	for rating := 1; rating <= 5; rating++ {
		count := ratingCounts[rating]
		percentage := 0.0
		if totalFeedback > 0 {
			percentage = float64(count) / float64(totalFeedback) * 100
		}
		ratingDistribution = append(ratingDistribution, map[string]any{
			"rating":     rating,
			"count":      count,
			"percentage": percentage,
		})
	}

	recentTrends := make([]map[string]any, 0, len(trendMap))
	for _, row := range trendMap {
		average := 0.0
		count := int(toFloat64(row["count"]))
		if count > 0 {
			average = float64(int(toFloat64(row["sumRating"]))) / float64(count)
		}
		recentTrends = append(recentTrends, map[string]any{
			"date":          row["date"],
			"count":         count,
			"averageRating": average,
		})
	}
	sort.Slice(recentTrends, func(i, j int) bool {
		return StringValue(recentTrends[i]["date"]) < StringValue(recentTrends[j]["date"])
	})

	payload := map[string]any{
		"success": true,
		"data": map[string]any{
			"summary": map[string]any{
				"totalFeedback":         totalFeedback,
				"averageOverallRating":  round1(averageOverall),
				"averageServiceRating":  round1Ptr(averageService),
				"averageFoodRating":     round1Ptr(averageFood),
				"averageCompletionTime": round0Ptr(averageCompletion),
			},
			"sentimentDistribution": sentimentDistribution,
			"topTags":               topTags,
			"ratingDistribution":    ratingDistribution,
			"recentTrends":          recentTrends,
		},
		"meta": map[string]any{
			"period":    period,
			"startDate": startDate.Format(time.RFC3339),
			"endDate":   now.Format(time.RFC3339),
			"timezone":  loc.String(),
		},
	}
	setAnalyticsCache(cacheKey, payload, 5*time.Minute)
	response.JSON(w, http.StatusOK, payload)
}

func feedbackSentiment(comment string, rating int) string {
	text := strings.ToLower(comment)
	hasPositive := containsAny(text, feedbackPositiveWords)
	hasNegative := containsAny(text, feedbackNegativeWords)

	if rating >= 4 && hasPositive && !hasNegative {
		return "positive"
	}
	if rating <= 2 || hasNegative {
		return "negative"
	}
	return "neutral"
}

func feedbackTags(comment string) []string {
	text := strings.ToLower(comment)
	tags := make([]string, 0)
	for tag, keywords := range feedbackTagKeywords {
		if containsAny(text, keywords) {
			tags = append(tags, tag)
		}
	}
	return tags
}

func containsAny(text string, values []string) bool {
	for _, value := range values {
		if strings.Contains(text, value) {
			return true
		}
	}
	return false
}

func startDateForFeedbackPeriod(now time.Time, period string) time.Time {
	switch period {
	case "week":
		return now.AddDate(0, 0, -7)
	case "quarter":
		return now.AddDate(0, 0, -90)
	case "year":
		return time.Date(now.Year()-1, now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	default:
		return now.AddDate(0, 0, -30)
	}
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func nullIfInvalidInt(value pgtype.Int4) *int32 {
	if value.Valid {
		v := value.Int32
		return &v
	}
	return nil
}

func round1(value float64) float64 {
	return float64(int(value*10+0.5)) / 10
}

func round1Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	v := round1(*value)
	return &v
}

func round0Ptr(value *float64) *float64 {
	if value == nil {
		return nil
	}
	v := float64(int(*value + 0.5))
	return &v
}
