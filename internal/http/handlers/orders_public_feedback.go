package handlers

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type publicFeedbackRequest struct {
	OverallRating int     `json:"overallRating"`
	ServiceRating *int    `json:"serviceRating"`
	FoodRating    *int    `json:"foodRating"`
	Comment       *string `json:"comment"`
}

func (h *Handler) PublicOrderFeedbackGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	var merchantCode string
	if err := h.DB.QueryRow(ctx, `
		select o.order_number, m.code
		from orders o
		join merchants m on m.id = o.merchant_id
		where o.order_number = $1
		limit 1
	`, orderNumber).Scan(&orderNumber, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode, orderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	var (
		id            int64
		overallRating int32
		serviceRating pgtype.Int4
		foodRating    pgtype.Int4
		comment       pgtype.Text
		createdAt     time.Time
	)

	feedbackErr := h.DB.QueryRow(ctx, `
		select id, overall_rating, service_rating, food_rating, comment, created_at
		from order_feedbacks
		where order_number = $1
		limit 1
	`, orderNumber).Scan(
		&id,
		&overallRating,
		&serviceRating,
		&foodRating,
		&comment,
		&createdAt,
	)

	if feedbackErr != nil {
		if feedbackErr == pgx.ErrNoRows {
			response.JSON(w, http.StatusOK, map[string]any{
				"success":     true,
				"hasFeedback": false,
				"data":        nil,
			})
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to check feedback")
		return
	}

	result := map[string]any{
		"id":            id,
		"overallRating": overallRating,
		"createdAt":     createdAt,
	}
	if serviceRating.Valid {
		result["serviceRating"] = serviceRating.Int32
	}
	if foodRating.Valid {
		result["foodRating"] = foodRating.Int32
	}
	if comment.Valid {
		result["comment"] = comment.String
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"hasFeedback": true,
		"data":        result,
	})
}

func (h *Handler) PublicOrderFeedbackCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	var body publicFeedbackRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if body.OverallRating < 1 || body.OverallRating > 5 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Overall rating must be between 1 and 5")
		return
	}
	if body.ServiceRating != nil && (*body.ServiceRating < 1 || *body.ServiceRating > 5) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Service rating must be between 1 and 5")
		return
	}
	if body.FoodRating != nil && (*body.FoodRating < 1 || *body.FoodRating > 5) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Food rating must be between 1 and 5")
		return
	}

	var existingID int64
	if err := h.DB.QueryRow(ctx, `select id from order_feedbacks where order_number = $1`, orderNumber).Scan(&existingID); err == nil {
		response.JSON(w, http.StatusConflict, map[string]any{
			"success": false,
			"message": "Feedback already submitted for this order",
		})
		return
	}

	var (
		orderID             int64
		merchantID          int64
		orderType           string
		deliveryStatus      pgtype.Text
		placedAt            pgtype.Timestamptz
		completedAt         pgtype.Timestamptz
		deliveryDeliveredAt pgtype.Timestamptz
		merchantCode        string
	)

	if err := h.DB.QueryRow(ctx, `
		select o.id, o.merchant_id, o.order_type, o.delivery_status, o.placed_at, o.completed_at, o.delivery_delivered_at,
		       m.code
		from orders o
		join merchants m on m.id = o.merchant_id
		where o.order_number = $1
		limit 1
	`, orderNumber).Scan(
		&orderID,
		&merchantID,
		&orderType,
		&deliveryStatus,
		&placedAt,
		&completedAt,
		&deliveryDeliveredAt,
		&merchantCode,
	); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	token := r.URL.Query().Get("token")
	if !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode, orderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if strings.ToUpper(orderType) != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Feedback is only available for delivery orders")
		return
	}

	if !deliveryDeliveredAt.Valid && (!deliveryStatus.Valid || strings.ToUpper(deliveryStatus.String) != "DELIVERED") {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Feedback can only be submitted after delivery is completed")
		return
	}

	var orderCompletionMinutes *int32
	if placedAt.Valid {
		endAt := completedAt
		if deliveryDeliveredAt.Valid {
			endAt = deliveryDeliveredAt
		}
		if endAt.Valid {
			duration := endAt.Time.Sub(placedAt.Time)
			minutes := int32(math.Round(duration.Minutes()))
			orderCompletionMinutes = &minutes
		}
	}

	comment := strings.TrimSpace(defaultStringPtr(body.Comment))
	if comment == "" {
		body.Comment = nil
	} else {
		body.Comment = &comment
	}

	if _, err := h.DB.Exec(ctx, `
		insert into order_feedbacks (order_number, merchant_id, overall_rating, service_rating, food_rating, comment, order_completion_minutes)
		values ($1,$2,$3,$4,$5,$6,$7)
	`, orderNumber, merchantID, body.OverallRating, body.ServiceRating, body.FoodRating, body.Comment, orderCompletionMinutes); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to submit feedback")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"message": "Feedback submitted successfully",
		"data": map[string]any{
			"orderId": orderID,
		},
	})
}

func defaultStringPtr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
