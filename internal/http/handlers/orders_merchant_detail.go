package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/bcrypt"
)

func (h *Handler) MerchantOrderDetailGet(w http.ResponseWriter, r *http.Request) {
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

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    data,
	})
}

func (h *Handler) MerchantOrderDetailPatch(w http.ResponseWriter, r *http.Request) {
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
		TableNumber any `json:"tableNumber"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	newTable := strings.TrimSpace(toString(body.TableNumber))
	if newTable == "" {
		response.Error(w, http.StatusBadRequest, "TABLE_NUMBER_REQUIRED", "Table number is required")
		return
	}
	if len(newTable) > 50 {
		response.Error(w, http.StatusBadRequest, "TABLE_NUMBER_TOO_LONG", "Table number is too long")
		return
	}

	var orderType string
	if err := h.DB.QueryRow(ctx, `select order_type from orders where id = $1 and merchant_id = $2`, orderID, *authCtx.MerchantID).Scan(&orderType); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}
	if orderType != "DINE_IN" {
		response.Error(w, http.StatusBadRequest, "TABLE_NUMBER_DINEIN_ONLY", "Table number can only be set for dine-in orders")
		return
	}

	if _, err := h.DB.Exec(ctx, `update orders set table_number = $1 where id = $2`, newTable, orderID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order")
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
	})
}

func (h *Handler) MerchantOrderDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body struct {
		Pin string `json:"pin"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "PIN_REQUIRED", "PIN is required to delete an order")
		return
	}
	if strings.TrimSpace(body.Pin) == "" {
		response.Error(w, http.StatusBadRequest, "PIN_REQUIRED", "PIN is required to delete an order")
		return
	}

	var deletePin pgtype.Text
	if err := h.DB.QueryRow(ctx, `select delete_pin from merchants where id = $1`, *authCtx.MerchantID).Scan(&deletePin); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}
	if !deletePin.Valid || strings.TrimSpace(deletePin.String) == "" {
		response.Error(w, http.StatusBadRequest, "PIN_NOT_SET", "Delete PIN is not configured for this merchant")
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(deletePin.String), []byte(body.Pin)); err != nil {
		response.Error(w, http.StatusUnauthorized, "INVALID_PIN", "Invalid PIN")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, `select exists(select 1 from orders where id = $1 and merchant_id = $2)`, orderID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `delete from order_item_addons where order_item_id in (select id from order_items where order_id = $1)`, orderID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}
	if _, err := tx.Exec(ctx, `delete from order_items where order_id = $1`, orderID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}
	if _, err := tx.Exec(ctx, `delete from payments where order_id = $1`, orderID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}
	if _, err := tx.Exec(ctx, `delete from orders where id = $1`, orderID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete order")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Order deleted successfully",
	})
}

func (h *Handler) MerchantOrderAdminNote(w http.ResponseWriter, r *http.Request) {
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
		AdminNote *string `json:"adminNote"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	adminNote := strings.TrimSpace(defaultStringPtr(body.AdminNote))
	if adminNote != "" && len(adminNote) > 2000 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Admin note is too long")
		return
	}
	var adminNotePtr *string
	if adminNote != "" {
		adminNotePtr = &adminNote
	}

	var customerNote pgtype.Text
	if err := h.DB.QueryRow(ctx, `select notes from orders where id = $1 and merchant_id = $2`, orderID, *authCtx.MerchantID).Scan(&customerNote); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	customerNoteTrim := strings.TrimSpace(defaultStringPtr(textPtr(customerNote)))
	var kitchenNotes *string
	if adminNotePtr != nil {
		combined := "- admin: " + *adminNotePtr
		if customerNoteTrim != "" {
			combined = customerNoteTrim + " - admin: " + *adminNotePtr
		}
		kitchenNotes = &combined
	}

	if _, err := h.DB.Exec(ctx, `
		update orders set admin_note = $1, kitchen_notes = $2 where id = $3 and merchant_id = $4
	`, adminNotePtr, kitchenNotes, orderID, *authCtx.MerchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update admin note")
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
	})
}

func (h *Handler) MerchantOrderPayment(w http.ResponseWriter, r *http.Request) {
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
		PaymentMethod string  `json:"paymentMethod"`
		Amount        float64 `json:"amount"`
		Note          *string `json:"note"`
		Notes         *string `json:"notes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if strings.TrimSpace(body.PaymentMethod) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Payment method is required")
		return
	}
	if body.Amount <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Valid amount is required")
		return
	}

	allowedMethods := map[string]bool{
		"CASH_ON_COUNTER":  true,
		"CASH_ON_DELIVERY": true,
		"CARD":             true,
		"BANK_TRANSFER":    true,
		"PAYPAL":           true,
		"STRIPE":           true,
		"EFTPOS":           true,
		"QRIS":             true,
	}
	if !allowedMethods[strings.ToUpper(body.PaymentMethod)] {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payment method")
		return
	}

	note := strings.TrimSpace(defaultStringPtr(body.Note))
	if note == "" {
		note = strings.TrimSpace(defaultStringPtr(body.Notes))
	}
	var notePtr *string
	if note != "" {
		notePtr = &note
	}

	result, err := h.recordOrderPayment(ctx, *authCtx.MerchantID, orderID, strings.ToUpper(body.PaymentMethod), body.Amount, authCtx.UserID, notePtr)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    result,
		"message": "Payment recorded successfully",
	})
}

func (h *Handler) fetchMerchantOrderDetail(ctx context.Context, merchantID, orderID int64) (map[string]any, error) {
	query := `
		select
		  o.id, o.merchant_id, o.customer_id, o.order_number, o.order_type, o.table_number,
		  o.status, o.is_scheduled, o.scheduled_date, o.scheduled_time, o.stock_deducted_at,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount,
		  o.delivery_distance_km, o.delivery_delivered_at,
		  o.delivery_building_name, o.delivery_building_number, o.delivery_floor, o.delivery_instructions,
		  o.delivery_street_line, o.delivery_suburb, o.delivery_city, o.delivery_state, o.delivery_postcode, o.delivery_country,
		  o.delivery_latitude, o.delivery_longitude,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.notes, o.admin_note, o.kitchen_notes,
		  o.created_at, o.updated_at, o.placed_at, o.completed_at, o.cancelled_at, o.actual_ready_at,
		  o.edited_at, o.edited_by_user_id,
		  p.id, p.status, p.payment_method, p.amount, p.paid_at, p.paid_by_user_id,
		  p.customer_paid_at, p.customer_proof_url, p.customer_proof_uploaded_at, p.customer_payment_note, p.customer_proof_meta,
		  pu.id, pu.name, pu.email,
		  r.id, r.party_size, r.reservation_date, r.reservation_time, r.table_number, r.status, r.created_at, r.updated_at,
		  c.id, c.name, c.email, c.phone,
		  d.id, d.name, d.email, d.phone
		from orders o
		left join payments p on p.order_id = o.id
		left join users pu on pu.id = p.paid_by_user_id
		left join reservations r on r.order_id = o.id
		left join customers c on c.id = o.customer_id
		left join users d on d.id = o.delivery_driver_user_id
		where o.id = $1 and o.merchant_id = $2
		limit 1
	`

	var (
		order                  OrderListItem
		customerID             pgtype.Int8
		customerName           pgtype.Text
		customerEmail          pgtype.Text
		customerPhone          pgtype.Text
		paymentID              pgtype.Int8
		paymentStatus          pgtype.Text
		paymentMethod          pgtype.Text
		paymentAmount          pgtype.Numeric
		paidAt                 pgtype.Timestamptz
		paidByUserID           pgtype.Int8
		customerPaidAt         pgtype.Timestamptz
		customerProofURL       pgtype.Text
		customerProofUploaded  pgtype.Timestamptz
		customerPaymentNote    pgtype.Text
		customerProofMeta      []byte
		paidByID               pgtype.Int8
		paidByName             pgtype.Text
		paidByEmail            pgtype.Text
		reservationID          pgtype.Int8
		reservationParty       pgtype.Int4
		reservationDate        pgtype.Text
		reservationTime        pgtype.Text
		reservationTable       pgtype.Text
		reservationStatus      pgtype.Text
		reservationCreated     pgtype.Timestamptz
		reservationUpdated     pgtype.Timestamptz
		driverID               pgtype.Int8
		driverName             pgtype.Text
		driverEmail            pgtype.Text
		driverPhone            pgtype.Text
		scheduledDate          pgtype.Text
		scheduledTime          pgtype.Text
		deliveryStatus         pgtype.Text
		deliveryUnit           pgtype.Text
		deliveryAddress        pgtype.Text
		deliveryFee            pgtype.Numeric
		deliveryDistance       pgtype.Numeric
		deliveryDelivered      pgtype.Timestamptz
		deliveryLat            pgtype.Numeric
		deliveryLng            pgtype.Numeric
		subtotal               pgtype.Numeric
		taxAmount              pgtype.Numeric
		serviceCharge          pgtype.Numeric
		packagingFee           pgtype.Numeric
		discountAmount         pgtype.Numeric
		totalAmount            pgtype.Numeric
		notes                  pgtype.Text
		adminNote              pgtype.Text
		kitchenNotes           pgtype.Text
		completedAt            pgtype.Timestamptz
		cancelledAt            pgtype.Timestamptz
		actualReadyAt          pgtype.Timestamptz
		editedAt               pgtype.Timestamptz
		editedBy               pgtype.Int8
		stockDeductedAt        pgtype.Timestamptz
		deliveryBuildingName   pgtype.Text
		deliveryBuildingNumber pgtype.Text
		deliveryFloor          pgtype.Text
		deliveryInstructions   pgtype.Text
		deliveryStreetLine     pgtype.Text
		deliverySuburb         pgtype.Text
		deliveryCity           pgtype.Text
		deliveryState          pgtype.Text
		deliveryPostcode       pgtype.Text
		deliveryCountry        pgtype.Text
	)

	if err := h.DB.QueryRow(ctx, query, orderID, merchantID).Scan(
		&order.ID,
		&order.MerchantID,
		&order.CustomerID,
		&order.OrderNumber,
		&order.OrderType,
		&order.TableNumber,
		&order.Status,
		&order.IsScheduled,
		&scheduledDate,
		&scheduledTime,
		&stockDeductedAt,
		&deliveryStatus,
		&deliveryUnit,
		&deliveryAddress,
		&deliveryFee,
		&deliveryDistance,
		&deliveryDelivered,
		&deliveryBuildingName,
		&deliveryBuildingNumber,
		&deliveryFloor,
		&deliveryInstructions,
		&deliveryStreetLine,
		&deliverySuburb,
		&deliveryCity,
		&deliveryState,
		&deliveryPostcode,
		&deliveryCountry,
		&deliveryLat,
		&deliveryLng,
		&subtotal,
		&taxAmount,
		&serviceCharge,
		&packagingFee,
		&discountAmount,
		&totalAmount,
		&notes,
		&adminNote,
		&kitchenNotes,
		&order.CreatedAt,
		&order.UpdatedAt,
		&order.PlacedAt,
		&completedAt,
		&cancelledAt,
		&actualReadyAt,
		&editedAt,
		&editedBy,
		&paymentID,
		&paymentStatus,
		&paymentMethod,
		&paymentAmount,
		&paidAt,
		&paidByUserID,
		&customerPaidAt,
		&customerProofURL,
		&customerProofUploaded,
		&customerPaymentNote,
		&customerProofMeta,
		&paidByID,
		&paidByName,
		&paidByEmail,
		&reservationID,
		&reservationParty,
		&reservationDate,
		&reservationTime,
		&reservationTable,
		&reservationStatus,
		&reservationCreated,
		&reservationUpdated,
		&customerID,
		&customerName,
		&customerEmail,
		&customerPhone,
		&driverID,
		&driverName,
		&driverEmail,
		&driverPhone,
	); err != nil {
		return nil, err
	}

	if scheduledDate.Valid {
		order.ScheduledDate = &scheduledDate.String
	}
	if scheduledTime.Valid {
		order.ScheduledTime = &scheduledTime.String
	}
	if deliveryStatus.Valid {
		order.DeliveryStatus = &deliveryStatus.String
	}
	if deliveryUnit.Valid {
		order.DeliveryUnit = &deliveryUnit.String
	}
	if deliveryAddress.Valid {
		order.DeliveryAddress = &deliveryAddress.String
	}
	if notes.Valid {
		order.Notes = &notes.String
	}
	if adminNote.Valid {
		order.AdminNote = &adminNote.String
	}
	if kitchenNotes.Valid {
		order.KitchenNotes = &kitchenNotes.String
	}
	if editedAt.Valid {
		order.EditedAt = &editedAt.Time
	}
	if editedBy.Valid {
		order.EditedByUserID = &editedBy.Int64
	}
	if deliveryDistance.Valid {
		d := utils.NumericToFloat64(deliveryDistance)
		order.DeliveryDistanceKm = &d
	}
	if deliveryDelivered.Valid {
		order.DeliveryDeliveredAt = &deliveryDelivered.Time
	}
	order.Subtotal = utils.NumericToFloat64(subtotal)
	order.TaxAmount = utils.NumericToFloat64(taxAmount)
	order.ServiceChargeAmount = utils.NumericToFloat64(serviceCharge)
	order.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
	order.DiscountAmount = utils.NumericToFloat64(discountAmount)
	order.TotalAmount = utils.NumericToFloat64(totalAmount)
	order.DeliveryFeeAmount = utils.NumericToFloat64(deliveryFee)

	data := map[string]any{
		"id":            order.ID,
		"merchantId":    order.MerchantID,
		"customerId":    order.CustomerID,
		"orderNumber":   order.OrderNumber,
		"orderType":     order.OrderType,
		"tableNumber":   order.TableNumber,
		"status":        order.Status,
		"isScheduled":   order.IsScheduled,
		"scheduledDate": order.ScheduledDate,
		"scheduledTime": order.ScheduledTime,
		"stockDeductedAt": func() any {
			if stockDeductedAt.Valid {
				return stockDeductedAt.Time
			}
			return nil
		}(),
		"deliveryStatus":         order.DeliveryStatus,
		"deliveryUnit":           order.DeliveryUnit,
		"deliveryAddress":        order.DeliveryAddress,
		"deliveryFeeAmount":      order.DeliveryFeeAmount,
		"deliveryDistanceKm":     order.DeliveryDistanceKm,
		"deliveryDeliveredAt":    order.DeliveryDeliveredAt,
		"deliveryBuildingName":   nullIfEmptyText(deliveryBuildingName),
		"deliveryBuildingNumber": nullIfEmptyText(deliveryBuildingNumber),
		"deliveryFloor":          nullIfEmptyText(deliveryFloor),
		"deliveryInstructions":   nullIfEmptyText(deliveryInstructions),
		"deliveryStreetLine":     nullIfEmptyText(deliveryStreetLine),
		"deliverySuburb":         nullIfEmptyText(deliverySuburb),
		"deliveryCity":           nullIfEmptyText(deliveryCity),
		"deliveryState":          nullIfEmptyText(deliveryState),
		"deliveryPostcode":       nullIfEmptyText(deliveryPostcode),
		"deliveryCountry":        nullIfEmptyText(deliveryCountry),
		"deliveryLatitude":       nullableNumeric(deliveryLat),
		"deliveryLongitude":      nullableNumeric(deliveryLng),
		"subtotal":               order.Subtotal,
		"taxAmount":              order.TaxAmount,
		"serviceChargeAmount":    order.ServiceChargeAmount,
		"packagingFeeAmount":     order.PackagingFeeAmount,
		"discountAmount":         order.DiscountAmount,
		"totalAmount":            order.TotalAmount,
		"notes":                  order.Notes,
		"adminNote":              order.AdminNote,
		"kitchenNotes":           order.KitchenNotes,
		"createdAt":              order.CreatedAt,
		"updatedAt":              order.UpdatedAt,
		"placedAt":               order.PlacedAt,
		"completedAt": func() any {
			if completedAt.Valid {
				return completedAt.Time
			}
			return nil
		}(),
		"cancelledAt": func() any {
			if cancelledAt.Valid {
				return cancelledAt.Time
			}
			return nil
		}(),
		"actualReadyAt": func() any {
			if actualReadyAt.Valid {
				return actualReadyAt.Time
			}
			return nil
		}(),
		"editedAt":       order.EditedAt,
		"editedByUserId": order.EditedByUserID,
	}

	if paymentID.Valid {
		var proofMeta any
		if len(customerProofMeta) > 0 {
			var parsed map[string]any
			if err := json.Unmarshal(customerProofMeta, &parsed); err == nil {
				proofMeta = parsed
			}
		}

		payment := map[string]any{
			"id":            paymentID.Int64,
			"status":        paymentStatus.String,
			"paymentMethod": paymentMethod.String,
			"amount":        utils.NumericToFloat64(paymentAmount),
			"paidAt": func() any {
				if paidAt.Valid {
					return paidAt.Time
				}
				return nil
			}(),
			"paidByUserId": func() any {
				if paidByUserID.Valid {
					return paidByUserID.Int64
				}
				return nil
			}(),
			"customerPaidAt": func() any {
				if customerPaidAt.Valid {
					return customerPaidAt.Time
				}
				return nil
			}(),
			"customerProofUrl": func() any {
				if customerProofURL.Valid {
					return customerProofURL.String
				}
				return nil
			}(),
			"customerProofUploadedAt": func() any {
				if customerProofUploaded.Valid {
					return customerProofUploaded.Time
				}
				return nil
			}(),
			"customerPaymentNote": func() any {
				if customerPaymentNote.Valid {
					return customerPaymentNote.String
				}
				return nil
			}(),
			"customerProofMeta": proofMeta,
		}
		if paidByID.Valid {
			payment["paidBy"] = map[string]any{
				"id":    paidByID.Int64,
				"name":  paidByName.String,
				"email": paidByEmail.String,
			}
		}
		data["payment"] = payment
	} else {
		data["payment"] = nil
	}

	if reservationID.Valid {
		data["reservation"] = map[string]any{
			"id":              reservationID.Int64,
			"partySize":       reservationParty.Int32,
			"reservationDate": reservationDate.String,
			"reservationTime": reservationTime.String,
			"tableNumber": func() any {
				if reservationTable.Valid {
					return reservationTable.String
				}
				return nil
			}(),
			"status": reservationStatus.String,
			"createdAt": func() any {
				if reservationCreated.Valid {
					return reservationCreated.Time
				}
				return nil
			}(),
			"updatedAt": func() any {
				if reservationUpdated.Valid {
					return reservationUpdated.Time
				}
				return nil
			}(),
		}
	} else {
		data["reservation"] = nil
	}

	if customerID.Valid {
		data["customer"] = map[string]any{
			"id":    customerID.Int64,
			"name":  customerName.String,
			"email": customerEmail.String,
			"phone": func() any {
				if customerPhone.Valid {
					return customerPhone.String
				}
				return nil
			}(),
		}
	} else {
		data["customer"] = nil
	}

	if driverID.Valid {
		data["deliveryDriver"] = map[string]any{
			"id":    driverID.Int64,
			"name":  driverName.String,
			"email": ptrString(driverEmail),
			"phone": ptrString(driverPhone),
		}
	} else {
		data["deliveryDriver"] = nil
	}

	items, err := h.fetchOrderItemsWithAddons(ctx, orderID)
	if err != nil {
		return nil, err
	}
	data["orderItems"] = items

	discounts, err := h.fetchOrderDiscounts(ctx, orderID)
	if err == nil {
		data["orderDiscounts"] = discounts
	}

	return data, nil
}

func (h *Handler) fetchOrderItemsWithAddons(ctx context.Context, orderID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
		select oi.id, oi.menu_id, oi.menu_name, oi.menu_price, oi.quantity, oi.subtotal, oi.notes, m.id, m.name, m.image_url
		from order_items oi
		left join menus m on m.id = oi.menu_id
		where oi.order_id = $1
		order by oi.id asc
	`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	itemIDs := make([]int64, 0)
	for rows.Next() {
		var (
			itemID      int64
			menuID      pgtype.Int8
			menuName    string
			menuPrice   pgtype.Numeric
			quantity    int32
			subtotal    pgtype.Numeric
			notes       pgtype.Text
			menuIDRef   pgtype.Int8
			menuNameRef pgtype.Text
			menuImage   pgtype.Text
		)
		if err := rows.Scan(&itemID, &menuID, &menuName, &menuPrice, &quantity, &subtotal, &notes, &menuIDRef, &menuNameRef, &menuImage); err != nil {
			return nil, err
		}
		item := map[string]any{
			"id": itemID,
			"menuId": func() any {
				if menuID.Valid {
					return menuID.Int64
				}
				return nil
			}(),
			"menuName":  menuName,
			"menuPrice": utils.NumericToFloat64(menuPrice),
			"quantity":  quantity,
			"subtotal":  utils.NumericToFloat64(subtotal),
			"notes": func() any {
				if notes.Valid {
					return notes.String
				}
				return nil
			}(),
			"menu": func() any {
				if menuIDRef.Valid {
					return map[string]any{
						"id":       menuIDRef.Int64,
						"name":     nullIfEmptyText(menuNameRef),
						"imageUrl": nullIfEmptyText(menuImage),
					}
				}
				return nil
			}(),
		}
		items = append(items, item)
		itemIDs = append(itemIDs, itemID)
	}

	addonsMap := make(map[int64][]map[string]any)
	if len(itemIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select oia.id, oia.order_item_id, oia.addon_item_id, oia.addon_name, oia.addon_price, oia.quantity, oia.subtotal, ai.id, ai.name
			from order_item_addons oia
			left join addon_items ai on ai.id = oia.addon_item_id
			where oia.order_item_id = any($1)
		`, itemIDs)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				addonID        int64
				orderItemID    int64
				addonItemID    pgtype.Int8
				addonName      string
				addonPrice     pgtype.Numeric
				quantity       int32
				subtotal       pgtype.Numeric
				addonItemIDRef pgtype.Int8
				addonItemName  pgtype.Text
			)
			if err := rows.Scan(&addonID, &orderItemID, &addonItemID, &addonName, &addonPrice, &quantity, &subtotal, &addonItemIDRef, &addonItemName); err != nil {
				return nil, err
			}
			addonsMap[orderItemID] = append(addonsMap[orderItemID], map[string]any{
				"id": addonID,
				"addonItemId": func() any {
					if addonItemID.Valid {
						return addonItemID.Int64
					}
					return nil
				}(),
				"addonName":  addonName,
				"addonPrice": utils.NumericToFloat64(addonPrice),
				"quantity":   quantity,
				"subtotal":   utils.NumericToFloat64(subtotal),
				"addonItem": func() any {
					if addonItemIDRef.Valid {
						return map[string]any{
							"id":   addonItemIDRef.Int64,
							"name": nullIfEmptyText(addonItemName),
						}
					}
					return nil
				}(),
			})
		}
	}

	for _, item := range items {
		id := item["id"].(int64)
		item["addons"] = addonsMap[id]
	}

	return items, nil
}

func (h *Handler) fetchOrderDiscounts(ctx context.Context, orderID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
		select id, source, label, discount_amount
		from order_discounts
		where order_id = $1
	`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id     int64
			source string
			label  pgtype.Text
			amount pgtype.Numeric
		)
		if err := rows.Scan(&id, &source, &label, &amount); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":             id,
			"source":         source,
			"label":          nullIfEmptyText(label),
			"discountAmount": utils.NumericToFloat64(amount),
		})
	}

	return items, nil
}

func (h *Handler) recordOrderPayment(ctx context.Context, merchantID, orderID int64, paymentMethod string, amount float64, userID int64, note *string) (map[string]any, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var orderExists bool
	if err := tx.QueryRow(ctx, `select exists(select 1 from orders where id = $1 and merchant_id = $2)`, orderID, merchantID).Scan(&orderExists); err != nil || !orderExists {
		return nil, err
	}

	var paymentID pgtype.Int8
	if err := tx.QueryRow(ctx, `select id from payments where order_id = $1`, orderID).Scan(&paymentID); err != nil && err != pgx.ErrNoRows {
		return nil, err
	}

	now := time.Now()
	if paymentID.Valid {
		_, err = tx.Exec(ctx, `
			update payments
			set status = 'COMPLETED', payment_method = $1, paid_at = $2, paid_by_user_id = $3, notes = $4, amount = $5
			where order_id = $6
		`, paymentMethod, now, userID, note, amount, orderID)
		if err != nil {
			return nil, err
		}
	} else {
		_, err = tx.Exec(ctx, `
			insert into payments (order_id, amount, payment_method, status, paid_at, paid_by_user_id, notes)
			values ($1,$2,$3,'COMPLETED',$4,$5,$6)
		`, orderID, amount, paymentMethod, now, userID, note)
		if err != nil {
			return nil, err
		}
	}

	if err := h.acceptOrderIfPendingAfterPayment(ctx, tx, orderID, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	data, err := h.fetchMerchantOrderDetail(ctx, merchantID, orderID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"order":   data,
		"payment": data["payment"],
	}, nil
}

func toString(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}
