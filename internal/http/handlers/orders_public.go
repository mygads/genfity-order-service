package handlers

import (
	"context"
	"net/http"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicOrderDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	query := `
		select
		  o.id, o.order_number, o.status, o.order_type, o.table_number,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.created_at, o.updated_at, o.placed_at, o.completed_at,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount, o.delivery_distance_km, o.delivery_delivered_at,
		  o.edited_at,
		  m.name, m.currency, m.code,
		  c.name,
		  p.status, p.payment_method, p.amount, p.paid_at,
		  r.status, r.party_size, r.reservation_date, r.reservation_time, r.table_number
		from orders o
		join merchants m on m.id = o.merchant_id
		left join customers c on c.id = o.customer_id
		left join payments p on p.order_id = o.id
		left join reservations r on r.order_id = o.id
		where o.order_number = $1
		limit 1
	`

	var (
		detail           OrderDetail
		merchantName     string
		merchantCurrency string
		merchantCode     string
		customerName     pgtype.Text
		pStatus          pgtype.Text
		pMethod          pgtype.Text
		pAmount          pgtype.Numeric
		pPaidAt          pgtype.Timestamptz
		rStatus          pgtype.Text
		rParty           pgtype.Int4
		rDate            pgtype.Text
		rTime            pgtype.Text
		rTable           pgtype.Text
		subtotal         pgtype.Numeric
		taxAmount        pgtype.Numeric
		serviceCharge    pgtype.Numeric
		packagingFee     pgtype.Numeric
		discountAmount   pgtype.Numeric
		totalAmount      pgtype.Numeric
		deliveryFee      pgtype.Numeric
		deliveryDistance pgtype.Numeric
	)

	if err := h.DB.QueryRow(ctx, query, orderNumber).Scan(
		&detail.ID,
		&detail.OrderNumber,
		&detail.Status,
		&detail.OrderType,
		&detail.TableNumber,
		&subtotal,
		&taxAmount,
		&serviceCharge,
		&packagingFee,
		&discountAmount,
		&totalAmount,
		&detail.CreatedAt,
		&detail.UpdatedAt,
		&detail.PlacedAt,
		&detail.CompletedAt,
		&detail.DeliveryStatus,
		&detail.DeliveryUnit,
		&detail.DeliveryAddress,
		&deliveryFee,
		&deliveryDistance,
		&detail.DeliveryDeliveredAt,
		&detail.EditedAt,
		&merchantName,
		&merchantCurrency,
		&merchantCode,
		&customerName,
		&pStatus,
		&pMethod,
		&pAmount,
		&pPaidAt,
		&rStatus,
		&rParty,
		&rDate,
		&rTime,
		&rTable,
	); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode, detail.OrderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	detail.Subtotal = utils.NumericToFloat64(subtotal)
	detail.TaxAmount = utils.NumericToFloat64(taxAmount)
	detail.ServiceChargeAmount = utils.NumericToFloat64(serviceCharge)
	detail.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
	detail.DiscountAmount = utils.NumericToFloat64(discountAmount)
	detail.TotalAmount = utils.NumericToFloat64(totalAmount)
	detail.DeliveryFeeAmount = utils.NumericToFloat64(deliveryFee)
	if deliveryDistance.Valid {
		d := utils.NumericToFloat64(deliveryDistance)
		detail.DeliveryDistanceKm = &d
	}

	detail.CustomerName = customerName.String
	detail.ChangedByAdmin = detail.EditedAt != nil
	detail.Merchant.Name = merchantName
	detail.Merchant.Currency = merchantCurrency
	detail.Merchant.Code = merchantCode

	if pStatus.Valid || pMethod.Valid || pAmount.Valid || pPaidAt.Valid {
		status := pStatus.String
		method := pMethod.String
		amount := utils.NumericToFloat64(pAmount)
		paidAt := pPaidAt.Time
		var paidAtPtr *time.Time
		if pPaidAt.Valid {
			paidAtPtr = &paidAt
		}
		detail.Payment = &struct {
			Status        *string    `json:"status"`
			PaymentMethod *string    `json:"paymentMethod"`
			Amount        *float64   `json:"amount"`
			PaidAt        *time.Time `json:"paidAt"`
		}{
			Status:        &status,
			PaymentMethod: &method,
			Amount:        &amount,
			PaidAt:        paidAtPtr,
		}
	}

	if rStatus.Valid {
		detail.Reservation = &struct {
			Status          string  `json:"status"`
			PartySize       int32   `json:"partySize"`
			ReservationDate string  `json:"reservationDate"`
			ReservationTime string  `json:"reservationTime"`
			TableNumber     *string `json:"tableNumber"`
		}{
			Status:          rStatus.String,
			PartySize:       rParty.Int32,
			ReservationDate: rDate.String,
			ReservationTime: rTime.String,
		}
		if rTable.Valid {
			detail.Reservation.TableNumber = &rTable.String
		}
	}

	// Load order items + addons
	items, err := h.fetchOrderItems(ctx, detail.ID)
	if err != nil {
		h.Logger.Error("order items fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve order")
		return
	}
	detail.OrderItems = items

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       detail,
		"message":    "Order retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) fetchOrderItems(ctx context.Context, orderID int64) ([]OrderItem, error) {
	query := `
		select id, menu_name, menu_price, quantity, subtotal, notes
		from order_items
		where order_id = $1
		order by id asc
	`

	rows, err := h.DB.Query(ctx, query, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OrderItem, 0)
	itemIDs := make([]int64, 0)
	for rows.Next() {
		var (
			item      OrderItem
			menuPrice pgtype.Numeric
			subtotal  pgtype.Numeric
		)
		if err := rows.Scan(&item.ID, &item.MenuName, &menuPrice, &item.Quantity, &subtotal, &item.Notes); err != nil {
			return nil, err
		}
		item.MenuPrice = utils.NumericToFloat64(menuPrice)
		item.Subtotal = utils.NumericToFloat64(subtotal)
		items = append(items, item)
		itemIDs = append(itemIDs, item.ID)
	}

	if len(itemIDs) == 0 {
		return items, nil
	}

	addonsByItem, err := h.fetchOrderItemAddons(ctx, itemIDs)
	if err != nil {
		return nil, err
	}

	for i := range items {
		if addons, ok := addonsByItem[items[i].ID]; ok {
			items[i].Addons = addons
		}
	}

	return items, nil
}

func (h *Handler) fetchOrderItemAddons(ctx context.Context, itemIDs []int64) (map[int64][]OrderItemAddon, error) {
	query := `
		select order_item_id, id, addon_name, addon_price, quantity
		from order_item_addons
		where order_item_id = any($1)
	`

	rows, err := h.DB.Query(ctx, query, itemIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addons := make(map[int64][]OrderItemAddon)
	for rows.Next() {
		var (
			itemID int64
			addon  OrderItemAddon
			price  pgtype.Numeric
		)
		if err := rows.Scan(&itemID, &addon.ID, &addon.Name, &price, &addon.Quantity); err != nil {
			return nil, err
		}
		addon.Price = utils.NumericToFloat64(price)
		addons[itemID] = append(addons[itemID], addon)
	}

	return addons, nil
}
