package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type kitchenOrderItem struct {
	ID        int64               `json:"id"`
	MenuName  string              `json:"menuName"`
	MenuPrice float64             `json:"menuPrice"`
	Quantity  int32               `json:"quantity"`
	Subtotal  float64             `json:"subtotal"`
	Notes     *string             `json:"notes"`
	Addons    []kitchenOrderAddon `json:"addons"`
}

type kitchenOrderAddon struct {
	AddonName  string  `json:"addonName"`
	AddonPrice float64 `json:"addonPrice"`
	Quantity   int32   `json:"quantity"`
	Subtotal   float64 `json:"subtotal"`
}

type orderListItemWithItems struct {
	OrderListItem
	OrderItems []kitchenOrderItem `json:"orderItems,omitempty"`
}

func (h *Handler) MerchantOrdersList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	query := r.URL.Query()
	statusParam := strings.TrimSpace(query.Get("status"))
	paymentStatus := strings.TrimSpace(query.Get("paymentStatus"))
	orderType := strings.TrimSpace(query.Get("orderType"))
	startDate := strings.TrimSpace(query.Get("startDate"))
	endDate := strings.TrimSpace(query.Get("endDate"))
	sinceParam := strings.TrimSpace(query.Get("since"))
	includeItems := strings.EqualFold(strings.TrimSpace(query.Get("includeItems")), "true")

	page := 1
	if raw := strings.TrimSpace(query.Get("page")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			page = parsed
		}
	}
	limit := 20
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 200 {
		limit = 200
	}

	whereClauses := []string{"o.merchant_id = $1"}
	args := []any{*authCtx.MerchantID}

	if statusParam != "" {
		statuses := make([]string, 0)
		for _, raw := range strings.Split(statusParam, ",") {
			trimmed := strings.TrimSpace(raw)
			if trimmed != "" {
				statuses = append(statuses, trimmed)
			}
		}
		if len(statuses) > 0 {
			whereClauses = append(whereClauses, "o.status = any($"+strconv.Itoa(len(args)+1)+")")
			args = append(args, statuses)
		}
	}

	if paymentStatus != "" {
		whereClauses = append(whereClauses, "p.status = $"+strconv.Itoa(len(args)+1))
		args = append(args, paymentStatus)
	}

	if orderType != "" {
		whereClauses = append(whereClauses, "o.order_type = $"+strconv.Itoa(len(args)+1))
		args = append(args, orderType)
	}

	if startDate != "" {
		parsed, err := parseDateTimeParam(startDate)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid startDate")
			return
		}
		whereClauses = append(whereClauses, "o.placed_at >= $"+strconv.Itoa(len(args)+1))
		args = append(args, parsed)
	}

	if endDate != "" {
		parsed, err := parseDateTimeParam(endDate)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid endDate")
			return
		}
		whereClauses = append(whereClauses, "o.placed_at <= $"+strconv.Itoa(len(args)+1))
		args = append(args, parsed)
	}

	if sinceParam != "" {
		sinceMillis, err := strconv.ParseInt(sinceParam, 10, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid since")
			return
		}
		sinceTime := time.Unix(0, sinceMillis*int64(time.Millisecond))
		whereClauses = append(whereClauses, "o.updated_at >= $"+strconv.Itoa(len(args)+1))
		args = append(args, sinceTime)
	}

	whereSQL := strings.Join(whereClauses, " and ")

	countQuery := `
		select count(*)
		from orders o
		left join payments p on p.order_id = o.id
		where ` + whereSQL
	var total int64
	if err := h.DB.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
		return
	}

	limitOffset := ""
	if limit > 0 {
		limitOffset = " limit $" + strconv.Itoa(len(args)+1) + " offset $" + strconv.Itoa(len(args)+2)
		args = append(args, limit, (page-1)*limit)
	}

	listQuery := `
		select
		  o.id, o.merchant_id, o.customer_id, o.order_number, o.order_type, o.table_number,
		  o.status, o.is_scheduled, o.scheduled_date, o.scheduled_time,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount,
		  o.delivery_distance_km, o.delivery_delivered_at,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.notes, o.admin_note, o.kitchen_notes, o.placed_at, o.updated_at, o.edited_at, o.edited_by_user_id,
		  p.id, p.status, p.payment_method, p.paid_at,
		  r.id, r.party_size, r.reservation_date, r.reservation_time, r.table_number, r.status,
		  c.id, c.name, c.phone, c.email,
		  d.id, d.name, d.email, d.phone,
		  coalesce(oi_count.order_items_count, 0)
		from orders o
		left join payments p on p.order_id = o.id
		left join reservations r on r.order_id = o.id
		left join customers c on c.id = o.customer_id
		left join users d on d.id = o.delivery_driver_user_id
		left join (
			select order_id, count(*) as order_items_count
			from order_items
			group by order_id
		) oi_count on oi_count.order_id = o.id
		where ` + whereSQL + `
		order by o.placed_at desc
	` + limitOffset

	rows, err := h.DB.Query(ctx, listQuery, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
		return
	}
	defer rows.Close()

	items := make([]orderListItemWithItems, 0)
	orderIDs := make([]int64, 0)
	for rows.Next() {
		var order OrderListItem
		var (
			customerID          pgtype.Int8
			customerName        pgtype.Text
			customerPhone       pgtype.Text
			customerEmail       pgtype.Text
			paymentID           pgtype.Int8
			paymentStatus       pgtype.Text
			paymentMethod       pgtype.Text
			paidAt              pgtype.Timestamptz
			reservationID       pgtype.Int8
			reservationParty    pgtype.Int4
			reservationDate     pgtype.Text
			reservationTime     pgtype.Text
			reservationTable    pgtype.Text
			reservationStatus   pgtype.Text
			driverID            pgtype.Int8
			driverName          pgtype.Text
			driverEmail         pgtype.Text
			driverPhone         pgtype.Text
			scheduledDate       pgtype.Text
			scheduledTime       pgtype.Text
			deliveryStatus      pgtype.Text
			deliveryUnit        pgtype.Text
			deliveryAddress     pgtype.Text
			notes               pgtype.Text
			adminNote           pgtype.Text
			kitchenNotes        pgtype.Text
			editedAt            pgtype.Timestamptz
			editedBy            pgtype.Int8
			subtotal            pgtype.Numeric
			taxAmount           pgtype.Numeric
			serviceCharge       pgtype.Numeric
			packagingFee        pgtype.Numeric
			discountAmount      pgtype.Numeric
			totalAmount         pgtype.Numeric
			deliveryFee         pgtype.Numeric
			deliveryDistance    pgtype.Numeric
			deliveryDeliveredAt pgtype.Timestamptz
			orderItemsCount     pgtype.Int8
		)

		if err := rows.Scan(
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
			&deliveryStatus,
			&deliveryUnit,
			&deliveryAddress,
			&deliveryFee,
			&deliveryDistance,
			&deliveryDeliveredAt,
			&subtotal,
			&taxAmount,
			&serviceCharge,
			&packagingFee,
			&discountAmount,
			&totalAmount,
			&notes,
			&adminNote,
			&kitchenNotes,
			&order.PlacedAt,
			&order.UpdatedAt,
			&editedAt,
			&editedBy,
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
			&customerEmail,
			&driverID,
			&driverName,
			&driverEmail,
			&driverPhone,
			&orderItemsCount,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
			return
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
		if deliveryDeliveredAt.Valid {
			order.DeliveryDeliveredAt = &deliveryDeliveredAt.Time
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
			if customerEmail.Valid {
				order.Customer.Email = &customerEmail.String
			}
		}

		if driverID.Valid {
			order.DeliveryDriver = &DeliveryDriverSummary{
				ID:   driverID.Int64,
				Name: driverName.String,
			}
			if driverEmail.Valid {
				order.DeliveryDriver.Email = &driverEmail.String
			}
			if driverPhone.Valid {
				order.DeliveryDriver.Phone = &driverPhone.String
			}
		}

		if !includeItems {
			order.Count = &OrderCount{OrderItems: orderItemsCount.Int64}
		}

		items = append(items, orderListItemWithItems{OrderListItem: order})
		orderIDs = append(orderIDs, order.ID)
	}

	if includeItems && len(orderIDs) > 0 {
		itemRows, err := h.DB.Query(ctx, `
			select id, order_id, menu_name, menu_price, quantity, subtotal, notes
			from order_items
			where order_id = any($1)
			order by id asc
		`, orderIDs)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
			return
		}
		defer itemRows.Close()

		itemMap := make(map[int64][]kitchenOrderItem)
		itemIDs := make([]int64, 0)
		for itemRows.Next() {
			var (
				itemID    int64
				orderID   int64
				menuName  string
				menuPrice pgtype.Numeric
				quantity  int32
				subtotal  pgtype.Numeric
				notes     pgtype.Text
			)
			if err := itemRows.Scan(&itemID, &orderID, &menuName, &menuPrice, &quantity, &subtotal, &notes); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
				return
			}
			item := kitchenOrderItem{
				ID:        itemID,
				MenuName:  menuName,
				MenuPrice: utils.NumericToFloat64(menuPrice),
				Quantity:  quantity,
				Subtotal:  utils.NumericToFloat64(subtotal),
			}
			if notes.Valid {
				item.Notes = &notes.String
			}
			itemMap[orderID] = append(itemMap[orderID], item)
			itemIDs = append(itemIDs, itemID)
		}

		addonMap := make(map[int64][]kitchenOrderAddon)
		if len(itemIDs) > 0 {
			addonRows, err := h.DB.Query(ctx, `
				select order_item_id, addon_name, addon_price, quantity, subtotal
				from order_item_addons
				where order_item_id = any($1)
			`, itemIDs)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
				return
			}
			defer addonRows.Close()
			for addonRows.Next() {
				var (
					orderItemID int64
					name        string
					price       pgtype.Numeric
					quantity    int32
					subtotal    pgtype.Numeric
				)
				if err := addonRows.Scan(&orderItemID, &name, &price, &quantity, &subtotal); err != nil {
					response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch orders")
					return
				}
				addonMap[orderItemID] = append(addonMap[orderItemID], kitchenOrderAddon{
					AddonName:  name,
					AddonPrice: utils.NumericToFloat64(price),
					Quantity:   quantity,
					Subtotal:   utils.NumericToFloat64(subtotal),
				})
			}
		}

		for idx := range items {
			orderID := items[idx].ID
			orderItems := itemMap[orderID]
			if len(orderItems) > 0 {
				for i := range orderItems {
					orderItems[i].Addons = addonMap[orderItems[i].ID]
				}
				items[idx].OrderItems = orderItems
			}
		}
	}

	payload := make([]orderListItemWithItems, 0, len(items))
	for _, item := range items {
		payload = append(payload, item)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
		"total":   total,
	})
}

func parseDateTimeParam(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02", value); err == nil {
		return parsed, nil
	}
	return time.Time{}, fmt.Errorf("invalid datetime")
}
