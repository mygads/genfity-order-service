package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type POSOrderHistoryAddon struct {
	AddonName  string  `json:"addonName"`
	AddonPrice float64 `json:"addonPrice"`
	Quantity   int32   `json:"quantity"`
	Subtotal   float64 `json:"subtotal"`
}

type POSOrderHistoryItem struct {
	ID        string                 `json:"id"`
	MenuName  string                 `json:"menuName"`
	Quantity  int32                  `json:"quantity"`
	UnitPrice float64                `json:"unitPrice"`
	Subtotal  float64                `json:"subtotal"`
	Notes     *string                `json:"notes,omitempty"`
	Addons    []POSOrderHistoryAddon `json:"addons"`
}

type POSOrderHistoryEntry struct {
	ID                  string                `json:"id"`
	OrderNumber         string                `json:"orderNumber"`
	OrderType           string                `json:"orderType"`
	Status              string                `json:"status"`
	PaymentStatus       string                `json:"paymentStatus"`
	PaymentMethod       *string               `json:"paymentMethod,omitempty"`
	TableNumber         *string               `json:"tableNumber,omitempty"`
	CustomerName        *string               `json:"customerName,omitempty"`
	CustomerPhone       *string               `json:"customerPhone,omitempty"`
	Subtotal            float64               `json:"subtotal"`
	TaxAmount           float64               `json:"taxAmount"`
	ServiceChargeAmount float64               `json:"serviceChargeAmount"`
	PackagingFeeAmount  float64               `json:"packagingFeeAmount"`
	TotalAmount         float64               `json:"totalAmount"`
	DiscountAmount      *float64              `json:"discountAmount,omitempty"`
	AmountPaid          *float64              `json:"amountPaid,omitempty"`
	ChangeAmount        float64               `json:"changeAmount"`
	CreatedAt           string                `json:"createdAt"`
	PaidAt              *string               `json:"paidAt,omitempty"`
	Items               []POSOrderHistoryItem `json:"items"`
}

func (h *Handler) MerchantPOSOrderHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant context not found")
		return
	}

	query := r.URL.Query()
	todayOnly := query.Get("today") == "true"
	limit := parseIntWithBounds(query.Get("limit"), 200, 1, 500)

	args := []any{*authCtx.MerchantID}
	where := "where o.merchant_id = $1"
	if todayOnly {
		start, end := getTodayRange()
		where += " and o.placed_at >= $2 and o.placed_at < $3"
		args = append(args, start, end)
	}

	args = append(args, limit)
	limitPlaceholder := len(args)

	querySQL := `
		select o.id, o.order_number, o.order_type, o.status, o.table_number,
		       o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.total_amount, o.discount_amount, o.placed_at,
		       c.name, c.phone,
		       p.status, p.payment_method, p.amount, p.paid_at, p.metadata
		from orders o
		left join customers c on c.id = o.customer_id
		left join payments p on p.order_id = o.id
		` + where + `
		order by o.placed_at desc
		limit $` + strconv.Itoa(limitPlaceholder)

	rows, err := h.DB.Query(ctx, querySQL, args...)
	if err != nil {
		h.Logger.Error("pos history query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS order history")
		return
	}
	defer rows.Close()

	entries := make([]POSOrderHistoryEntry, 0)
	orderIDs := make([]int64, 0)
	orderIndex := make(map[int64]int)

	for rows.Next() {
		var (
			orderID        int64
			orderNumber    string
			orderType      string
			status         string
			tableNumber    pgtype.Text
			subtotal       pgtype.Numeric
			taxAmount      pgtype.Numeric
			serviceCharge  pgtype.Numeric
			packagingFee   pgtype.Numeric
			totalAmount    pgtype.Numeric
			discountAmount pgtype.Numeric
			placedAt       time.Time
			customerName   pgtype.Text
			customerPhone  pgtype.Text
			paymentStatus  pgtype.Text
			paymentMethod  pgtype.Text
			paymentAmount  pgtype.Numeric
			paidAt         pgtype.Timestamptz
			metadata       []byte
		)

		if err := rows.Scan(
			&orderID,
			&orderNumber,
			&orderType,
			&status,
			&tableNumber,
			&subtotal,
			&taxAmount,
			&serviceCharge,
			&packagingFee,
			&totalAmount,
			&discountAmount,
			&placedAt,
			&customerName,
			&customerPhone,
			&paymentStatus,
			&paymentMethod,
			&paymentAmount,
			&paidAt,
			&metadata,
		); err != nil {
			h.Logger.Error("pos history scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS order history")
			return
		}

		entry := POSOrderHistoryEntry{
			ID:                  strconv.FormatInt(orderID, 10),
			OrderNumber:         orderNumber,
			OrderType:           orderType,
			Status:              status,
			PaymentStatus:       "UNPAID",
			Subtotal:            utils.NumericToFloat64(subtotal),
			TaxAmount:           utils.NumericToFloat64(taxAmount),
			ServiceChargeAmount: utils.NumericToFloat64(serviceCharge),
			PackagingFeeAmount:  utils.NumericToFloat64(packagingFee),
			TotalAmount:         utils.NumericToFloat64(totalAmount),
			ChangeAmount:        0,
			CreatedAt:           placedAt.Format(time.RFC3339),
			Items:               []POSOrderHistoryItem{},
		}

		if tableNumber.Valid {
			entry.TableNumber = &tableNumber.String
		}
		if customerName.Valid {
			entry.CustomerName = &customerName.String
		}
		if customerPhone.Valid {
			entry.CustomerPhone = &customerPhone.String
		}
		if discountAmount.Valid {
			discount := utils.NumericToFloat64(discountAmount)
			entry.DiscountAmount = &discount
		}

		var requestedPaymentMethod *string
		if len(metadata) > 0 {
			var meta map[string]any
			if err := json.Unmarshal(metadata, &meta); err == nil {
				if value, ok := parseFloatFromAny(meta["paidAmount"]); ok {
					entry.AmountPaid = &value
				}
				if value, ok := parseFloatFromAny(meta["changeAmount"]); ok {
					entry.ChangeAmount = value
				}
				if value, ok := meta["requestedPaymentMethod"].(string); ok && value != "" {
					requestedPaymentMethod = &value
				}
			}
		}

		if paymentAmount.Valid && entry.AmountPaid == nil {
			paid := utils.NumericToFloat64(paymentAmount)
			entry.AmountPaid = &paid
		}
		if paymentMethod.Valid && paymentMethod.String != "" {
			method := paymentMethod.String
			entry.PaymentMethod = &method
		}
		if requestedPaymentMethod != nil {
			entry.PaymentMethod = requestedPaymentMethod
		}
		if paymentStatus.Valid && paymentStatus.String == "COMPLETED" {
			entry.PaymentStatus = "PAID"
		}
		if paidAt.Valid {
			paidAtValue := paidAt.Time.Format(time.RFC3339)
			entry.PaidAt = &paidAtValue
		}

		orderIndex[orderID] = len(entries)
		orderIDs = append(orderIDs, orderID)
		entries = append(entries, entry)
	}

	if len(orderIDs) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       entries,
			"statusCode": 200,
		})
		return
	}

	items, itemIndex, err := h.fetchPOSOrderItems(ctx, orderIDs)
	if err != nil {
		h.Logger.Error("pos history items failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS order history")
		return
	}

	addons, err := h.fetchPOSOrderItemAddons(ctx, itemIndex)
	if err != nil {
		h.Logger.Error("pos history addons failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS order history")
		return
	}

	for orderID, orderItems := range items {
		idx, ok := orderIndex[orderID]
		if !ok {
			continue
		}

		for i := range orderItems {
			if list, ok := addons[orderItems[i].ID]; ok {
				orderItems[i].Addons = list
			}
		}

		entries[idx].Items = orderItems
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       entries,
		"statusCode": 200,
	})
}

func (h *Handler) fetchPOSOrderItems(ctx context.Context, orderIDs []int64) (map[int64][]POSOrderHistoryItem, map[int64]int64, error) {
	rows, err := h.DB.Query(ctx, `
		select oi.id, oi.order_id, oi.menu_name, oi.quantity, oi.menu_price, oi.subtotal, oi.notes
		from order_items oi
		where oi.order_id = any($1)
	`, orderIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	itemsByOrder := make(map[int64][]POSOrderHistoryItem)
	itemIndex := make(map[int64]int64)

	for rows.Next() {
		var (
			itemID    int64
			orderID   int64
			menuName  string
			quantity  int32
			menuPrice pgtype.Numeric
			subtotal  pgtype.Numeric
			notes     pgtype.Text
		)
		if err := rows.Scan(&itemID, &orderID, &menuName, &quantity, &menuPrice, &subtotal, &notes); err != nil {
			return nil, nil, err
		}

		item := POSOrderHistoryItem{
			ID:        strconv.FormatInt(itemID, 10),
			MenuName:  menuName,
			Quantity:  quantity,
			UnitPrice: utils.NumericToFloat64(menuPrice),
			Subtotal:  utils.NumericToFloat64(subtotal),
			Addons:    []POSOrderHistoryAddon{},
		}
		if notes.Valid {
			item.Notes = &notes.String
		}

		itemsByOrder[orderID] = append(itemsByOrder[orderID], item)
		itemIndex[itemID] = orderID
	}

	return itemsByOrder, itemIndex, nil
}

func (h *Handler) fetchPOSOrderItemAddons(ctx context.Context, itemIndex map[int64]int64) (map[string][]POSOrderHistoryAddon, error) {
	if len(itemIndex) == 0 {
		return map[string][]POSOrderHistoryAddon{}, nil
	}

	itemIDs := make([]int64, 0, len(itemIndex))
	for itemID := range itemIndex {
		itemIDs = append(itemIDs, itemID)
	}

	rows, err := h.DB.Query(ctx, `
		select order_item_id, addon_name, addon_price, quantity, subtotal
		from order_item_addons
		where order_item_id = any($1)
	`, itemIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addonsByItem := make(map[string][]POSOrderHistoryAddon)
	for rows.Next() {
		var (
			orderItemID int64
			addonName   string
			addonPrice  pgtype.Numeric
			quantity    int32
			subtotal    pgtype.Numeric
		)
		if err := rows.Scan(&orderItemID, &addonName, &addonPrice, &quantity, &subtotal); err != nil {
			return nil, err
		}

		addon := POSOrderHistoryAddon{
			AddonName:  addonName,
			AddonPrice: utils.NumericToFloat64(addonPrice),
			Quantity:   quantity,
			Subtotal:   utils.NumericToFloat64(subtotal),
		}

		key := strconv.FormatInt(orderItemID, 10)
		addonsByItem[key] = append(addonsByItem[key], addon)
	}

	return addonsByItem, nil
}

func getTodayRange() (time.Time, time.Time) {
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	end := start.Add(24 * time.Hour)
	return start, end
}

func parseIntWithBounds(value string, fallback int, min int, max int) int {
	parsed := fallback
	if value != "" {
		if val, err := strconv.Atoi(value); err == nil {
			parsed = val
		}
	}
	if parsed < min {
		parsed = min
	}
	if parsed > max {
		parsed = max
	}
	return parsed
}

func parseFloatFromAny(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if parsed, err := v.Float64(); err == nil {
			return parsed, true
		}
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}
