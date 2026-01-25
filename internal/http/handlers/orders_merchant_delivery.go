package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) MerchantOrderDeliveryAssign(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var body struct {
		DriverUserID any `json:"driverUserId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	var (
		orderType      string
		deliveryStatus pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `
		select order_type, delivery_status
		from orders
		where id = $1 and merchant_id = $2
	`, orderID, *authCtx.MerchantID).Scan(&orderType, &deliveryStatus); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	if orderType != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Only DELIVERY orders can be assigned a driver")
		return
	}

	if deliveryStatus.Valid && deliveryStatus.String == "DELIVERED" {
		response.Error(w, http.StatusConflict, "INVALID_STATE", "Delivered orders cannot be modified")
		return
	}

	driverUserIDRaw := strings.TrimSpace(toString(body.DriverUserID))
	if driverUserIDRaw == "" {
		_, err = h.DB.Exec(ctx, `
			update orders
			set delivery_driver_user_id = null,
				delivery_assigned_at = null,
				delivery_status = 'PENDING_ASSIGNMENT'
			where id = $1 and merchant_id = $2
		`, orderID, *authCtx.MerchantID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update driver assignment")
			return
		}

		data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
		if err != nil {
			response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    data,
			"message": "Driver unassigned successfully",
		})
		return
	}

	driverUserID, err := parseInt64Value(driverUserIDRaw)
	if err != nil || driverUserID <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "driverUserId must be a valid ID")
		return
	}

	var driverExists bool
	if err := h.DB.QueryRow(ctx, `
		select exists (
			select 1
			from merchant_users mu
			join users u on u.id = mu.user_id
			where mu.merchant_id = $1
				and mu.user_id = $2
				and mu.is_active = true
				and u.is_active = true
				and (
					mu.role in ('OWNER', 'DRIVER')
					or (mu.role = 'STAFF' and mu.invitation_status = 'ACCEPTED' and $3 = any(mu.permissions))
				)
		)
	`, *authCtx.MerchantID, driverUserID, "driver_dashboard").Scan(&driverExists); err != nil || !driverExists {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Driver not found for this merchant")
		return
	}

	_, err = h.DB.Exec(ctx, `
		update orders
		set delivery_driver_user_id = $1,
			delivery_assigned_at = $2,
			delivery_status = 'ASSIGNED'
		where id = $3 and merchant_id = $4
	`, driverUserID, time.Now(), orderID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update driver assignment")
		return
	}

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    data,
		"message": "Driver assigned successfully",
	})
}

func (h *Handler) MerchantOrderCashOnDeliveryConfirm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var body struct {
		Notes *string `json:"notes"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	var (
		orderType     string
		totalAmount   pgtype.Numeric
		paymentMethod pgtype.Text
		paymentStatus pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `
		select o.order_type, o.total_amount, p.payment_method, p.status
		from orders o
		left join payments p on p.order_id = o.id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, *authCtx.MerchantID).Scan(&orderType, &totalAmount, &paymentMethod, &paymentStatus); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Delivery order not found")
		return
	}

	if orderType != "DELIVERY" {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Delivery order not found")
		return
	}

	if paymentMethod.Valid && paymentMethod.String != "CASH_ON_DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "This order is not Cash on Delivery")
		return
	}

	if paymentStatus.Valid && paymentStatus.String == "COMPLETED" {
		data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
		if err != nil {
			response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
			return
		}
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    map[string]any{"order": data, "payment": data["payment"]},
			"message": "Payment already completed",
		})
		return
	}

	amount := utils.NumericToFloat64(totalAmount)
	result, err := h.recordOrderPayment(ctx, *authCtx.MerchantID, orderID, "CASH_ON_DELIVERY", amount, authCtx.UserID, body.Notes)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    result,
		"message": "Cash on Delivery payment confirmed",
	})
}

func parseInt64Value(value string) (int64, error) {
	var out int64
	_, err := fmt.Sscan(value, &out)
	return out, err
}
