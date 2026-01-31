package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var allowedTransitions = map[string][]string{
	"PENDING":     {"ACCEPTED", "CANCELLED"},
	"ACCEPTED":    {"IN_PROGRESS", "CANCELLED"},
	"IN_PROGRESS": {"READY", "CANCELLED"},
	"READY":       {"COMPLETED", "CANCELLED"},
	"COMPLETED":   {},
	"CANCELLED":   {},
}

func (h *Handler) MerchantActiveOrders(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	query := `
		select
		  o.id, o.merchant_id, o.customer_id, o.order_number, o.order_type, o.table_number,
		  o.status, o.is_scheduled, o.scheduled_date, o.scheduled_time,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount,
		  o.delivery_distance_km, o.delivery_delivered_at,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.notes, o.admin_note, o.kitchen_notes, o.placed_at, o.updated_at, o.edited_at, o.edited_by_user_id,
		  p.id as payment_id, p.status as payment_status, p.payment_method, p.paid_at,
		  r.id as reservation_id, r.party_size, r.reservation_date, r.reservation_time, r.table_number as reservation_table_number, r.status as reservation_status,
		  c.id as customer_id2, c.name as customer_name, c.phone as customer_phone,
		  d.id as driver_id, d.name as driver_name, d.email as driver_email, d.phone as driver_phone,
		  count(oi.id) as order_items_count
		from orders o
		left join payments p on p.order_id = o.id
		left join reservations r on r.order_id = o.id
		left join customers c on c.id = o.customer_id
		left join users d on d.id = o.delivery_driver_user_id
		left join order_items oi on oi.order_id = o.id
		where o.merchant_id = $1
		  and o.status in ('PENDING', 'ACCEPTED', 'IN_PROGRESS', 'READY')
		group by o.id, p.id, r.id, c.id, d.id
		order by o.placed_at asc
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("active orders query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch active orders")
		return
	}
	defer rows.Close()

	orders := make([]OrderListItem, 0)
	for rows.Next() {
		var (
			order             OrderListItem
			paymentID         pgtype.Int8
			paymentStatus     pgtype.Text
			paymentMethod     pgtype.Text
			paidAt            pgtype.Timestamptz
			reservationID     pgtype.Int8
			reservationParty  pgtype.Int4
			reservationDate   pgtype.Text
			reservationTime   pgtype.Text
			reservationTable  pgtype.Text
			reservationStatus pgtype.Text
			customerID        pgtype.Int8
			customerName      pgtype.Text
			customerPhone     pgtype.Text
			driverID          pgtype.Int8
			driverName        pgtype.Text
			driverEmail       pgtype.Text
			driverPhone       pgtype.Text
			orderItemsCount   pgtype.Int8
			subtotal          pgtype.Numeric
			taxAmount         pgtype.Numeric
			serviceCharge     pgtype.Numeric
			packagingFee      pgtype.Numeric
			discountAmount    pgtype.Numeric
			totalAmount       pgtype.Numeric
			deliveryFee       pgtype.Numeric
			deliveryDistance  pgtype.Numeric
		)

		err = rows.Scan(
			&order.ID,
			&order.MerchantID,
			&order.CustomerID,
			&order.OrderNumber,
			&order.OrderType,
			&order.TableNumber,
			&order.Status,
			&order.IsScheduled,
			&order.ScheduledDate,
			&order.ScheduledTime,
			&order.DeliveryStatus,
			&order.DeliveryUnit,
			&order.DeliveryAddress,
			&deliveryFee,
			&deliveryDistance,
			&order.DeliveryDeliveredAt,
			&subtotal,
			&taxAmount,
			&serviceCharge,
			&packagingFee,
			&discountAmount,
			&totalAmount,
			&order.Notes,
			&order.AdminNote,
			&order.KitchenNotes,
			&order.PlacedAt,
			&order.UpdatedAt,
			&order.EditedAt,
			&order.EditedByUserID,
			&paymentID,
			&paymentStatus,
			&paymentMethod,
			&paidAt,
			&reservationID,
			&reservationParty,
			&reservationDate,
			&reservationTime,
			&reservationTable,
			&reservationStatus,
			&customerID,
			&customerName,
			&customerPhone,
			&driverID,
			&driverName,
			&driverEmail,
			&driverPhone,
			&orderItemsCount,
		)
		if err != nil {
			h.Logger.Error("active orders scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch active orders")
			return
		}

		order.Subtotal = utils.NumericToFloat64(subtotal)
		order.TaxAmount = utils.NumericToFloat64(taxAmount)
		order.ServiceChargeAmount = utils.NumericToFloat64(serviceCharge)
		order.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
		order.DiscountAmount = utils.NumericToFloat64(discountAmount)
		order.TotalAmount = utils.NumericToFloat64(totalAmount)
		order.DeliveryFeeAmount = utils.NumericToFloat64(deliveryFee)
		if deliveryDistance.Valid {
			d := utils.NumericToFloat64(deliveryDistance)
			order.DeliveryDistanceKm = &d
		}

		if paymentID.Valid {
			order.Payment = &PaymentSummary{
				ID:            paymentID.Int64,
				Status:        paymentStatus.String,
				PaymentMethod: paymentMethod.String,
			}
			if paidAt.Valid {
				order.Payment.PaidAt = &paidAt.Time
			}
		}

		if reservationID.Valid {
			order.Reservation = &ReservationSummary{
				ID:              reservationID.Int64,
				PartySize:       reservationParty.Int32,
				ReservationDate: reservationDate.String,
				ReservationTime: reservationTime.String,
				Status:          reservationStatus.String,
			}
			if reservationTable.Valid {
				order.Reservation.TableNumber = &reservationTable.String
			}
		}

		if customerID.Valid {
			order.Customer = &CustomerSummary{
				ID:   customerID.Int64,
				Name: customerName.String,
			}
			if customerPhone.Valid {
				order.Customer.Phone = &customerPhone.String
			}
		}

		if driverID.Valid {
			order.DeliveryDriver = &DeliveryDriverSummary{
				ID:    driverID.Int64,
				Name:  driverName.String,
				Email: ptrString(driverEmail),
				Phone: ptrString(driverPhone),
			}
		}

		if orderItemsCount.Valid {
			order.Count = &OrderCount{OrderItems: orderItemsCount.Int64}
		}

		orders = append(orders, order)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    orders,
		"count":   len(orders),
	})
}

func (h *Handler) MerchantUpdateOrderStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	var payload struct {
		Status        string  `json:"status"`
		Note          *string `json:"note"`
		ForceComplete bool    `json:"forceComplete"`
		ForceMarkPaid bool    `json:"forceMarkPaid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if payload.Status == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Status is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var (
		currentStatus   string
		orderType       string
		deliveryStatus  pgtype.Text
		isScheduled     bool
		stockDeductedAt pgtype.Timestamptz
	)
	check := `
		select status, order_type, delivery_status, is_scheduled, stock_deducted_at
		from orders
		where id = $1 and merchant_id = $2
	`
	if err := h.DB.QueryRow(ctx, check, orderID, *authCtx.MerchantID).Scan(&currentStatus, &orderType, &deliveryStatus, &isScheduled, &stockDeductedAt); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if strings.EqualFold(orderType, "DELIVERY") && payload.Status == "COMPLETED" {
		if !payload.ForceComplete {
			if !deliveryStatus.Valid || deliveryStatus.String != "DELIVERED" {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot transition order to COMPLETED before delivery status is DELIVERED")
				return
			}
		}
	} else {
		if !isValidTransition(currentStatus, payload.Status) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot transition status")
			return
		}
	}

	shouldForceMarkDelivered := strings.EqualFold(orderType, "DELIVERY") && payload.Status == "COMPLETED" && payload.ForceComplete && (!deliveryStatus.Valid || deliveryStatus.String != "DELIVERED")
	shouldAutoMarkPaid := currentStatus == "READY" && payload.Status == "COMPLETED"
	shouldMarkPaid := (payload.Status == "COMPLETED" && payload.ForceMarkPaid) || shouldAutoMarkPaid
	shouldDeductStock := isScheduled && !stockDeductedAt.Valid && currentStatus == "PENDING" && payload.Status == "ACCEPTED"

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now()
	if shouldDeductStock {
		if err := h.deductStockForScheduledOrder(ctx, tx, orderID, now); err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}
	}

	updateQuery := `
		update orders
		set status = $1::"OrderStatus",
			updated_at = $2,
			delivery_status = case when $1::"OrderStatus" = 'COMPLETED'::"OrderStatus" and $5 then 'DELIVERED' else delivery_status end,
			delivery_delivered_at = case when $1::"OrderStatus" = 'COMPLETED'::"OrderStatus" and $5 then $2 else delivery_delivered_at end,
			actual_ready_at = case when $1::"OrderStatus" = 'READY'::"OrderStatus" then $2 else actual_ready_at end,
			completed_at = case when $1::"OrderStatus" = 'COMPLETED'::"OrderStatus" then $2 else completed_at end,
			cancelled_at = case when $1::"OrderStatus" = 'CANCELLED'::"OrderStatus" then $2 else cancelled_at end,
			cancel_reason = case when $1::"OrderStatus" = 'CANCELLED'::"OrderStatus" then coalesce($3, cancel_reason) else cancel_reason end
		where id = $4
		returning id
	`

	var updatedID int64
	if err := tx.QueryRow(ctx, updateQuery, payload.Status, now, payload.Note, orderID, shouldForceMarkDelivered).Scan(&updatedID); err != nil {
		h.Logger.Error("order status update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
		return
	}

	if shouldMarkPaid {
		var paymentID pgtype.Int8
		if err := tx.QueryRow(ctx, `select id from payments where order_id = $1`, orderID).Scan(&paymentID); err != nil && err != pgx.ErrNoRows {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
			return
		}
		if paymentID.Valid {
			_, err = tx.Exec(ctx, `
				update payments
				set status = 'COMPLETED',
					paid_at = coalesce(paid_at, $1),
					paid_by_user_id = coalesce(paid_by_user_id, $2)
				where order_id = $3
			`, now, authCtx.UserID, orderID)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
				return
			}
		} else {
			defaultMethod := "CASH_ON_COUNTER"
			if strings.EqualFold(orderType, "DELIVERY") {
				defaultMethod = "CASH_ON_DELIVERY"
			}
			_, err = tx.Exec(ctx, `
				insert into payments (order_id, amount, payment_method, status, paid_at, paid_by_user_id)
				select id, total_amount, $2, 'COMPLETED', $3, $4 from orders where id = $1
			`, orderID, defaultMethod, now, authCtx.UserID)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
				return
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
		return
	}

	if h.Queue != nil {
		event := map[string]any{
			"type":       "order.status.updated",
			"orderId":    orderID,
			"merchantId": *authCtx.MerchantID,
			"status":     payload.Status,
			"note":       payload.Note,
			"userId":     authCtx.UserID,
			"updatedAt":  time.Now().UTC(),
		}
		_ = h.Queue.PublishJSON(ctx, "genfity.events", "order.status.updated", event)
	}

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order status")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Order status updated",
		"data":    data,
	})
}

func (h *Handler) MerchantResolveOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	orderNumber := r.URL.Query().Get("orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "ORDER_NUMBER_REQUIRED", "Order number is required")
		return
	}

	var orderID int64
	query := `select id from orders where merchant_id = $1 and order_number = $2`
	if err := h.DB.QueryRow(ctx, query, *authCtx.MerchantID, orderNumber).Scan(&orderID); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"orderId":     orderID,
			"orderNumber": orderNumber,
		},
	})
}

func isValidTransition(current, next string) bool {
	if current == next {
		return false
	}
	allowed := allowedTransitions[current]
	for _, s := range allowed {
		if s == next {
			return true
		}
	}
	return false
}

func ptrString(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
