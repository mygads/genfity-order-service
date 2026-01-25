package handlers

import (
	"context"
	"math"
	"net/http"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicOrderGroupDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	orderQuery := `
		select o.id, o.order_number, o.status, o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.total_amount,
		       m.code
		from orders o
		join merchants m on m.id = o.merchant_id
		where o.order_number = $1
		limit 1
	`

	var (
		orderID       int64
		status        string
		orderSubtotal pgtype.Numeric
		taxAmount     pgtype.Numeric
		serviceCharge pgtype.Numeric
		packagingFee  pgtype.Numeric
		totalAmount   pgtype.Numeric
		merchantCode  string
	)

	if err := h.DB.QueryRow(ctx, orderQuery, orderNumber).Scan(
		&orderID,
		&orderNumber,
		&status,
		&orderSubtotal,
		&taxAmount,
		&serviceCharge,
		&packagingFee,
		&totalAmount,
		&merchantCode,
	); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode, orderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	var sessionID pgtype.Int8
	if err := h.DB.QueryRow(ctx, "select id from group_order_sessions where order_id = $1", orderID).Scan(&sessionID); err != nil || !sessionID.Valid {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"isGroupOrder": false,
				"order": map[string]any{
					"id":                  orderID,
					"orderNumber":         orderNumber,
					"status":              status,
					"subtotal":            utils.NumericToFloat64(orderSubtotal),
					"taxAmount":           utils.NumericToFloat64(taxAmount),
					"serviceChargeAmount": utils.NumericToFloat64(serviceCharge),
					"packagingFeeAmount":  utils.NumericToFloat64(packagingFee),
					"totalAmount":         utils.NumericToFloat64(totalAmount),
				},
			},
			"message": "This is not a group order",
		})
		return
	}

	participants, err := h.fetchGroupOrderParticipantBasics(ctx, sessionID.Int64)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch group order details")
		return
	}

	details, err := h.fetchGroupOrderDetails(ctx, sessionID.Int64)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch group order details")
		return
	}

	participantMap := make(map[int64]map[string]any)
	for _, p := range participants {
		participantMap[p.ID] = map[string]any{
			"id":           p.ID,
			"name":         p.Name,
			"isHost":       p.IsHost,
			"items":        []map[string]any{},
			"itemSubtotal": float64(0),
		}
	}

	for _, d := range details {
		participant := participantMap[d.ParticipantID]
		if participant == nil {
			continue
		}
		items := participant["items"].([]map[string]any)
		item := map[string]any{
			"id":        d.OrderItemID,
			"menuName":  d.MenuName,
			"menuPrice": d.MenuPrice,
			"quantity":  d.Quantity,
			"subtotal":  d.ItemSubtotal,
			"notes":     d.Notes,
			"addons":    d.Addons,
		}
		items = append(items, item)
		participant["items"] = items
		participant["itemSubtotal"] = participant["itemSubtotal"].(float64) + d.ItemSubtotal
	}

	participantsArray := make([]map[string]any, 0, len(participantMap))
	for _, p := range participantMap {
		participantsArray = append(participantsArray, p)
	}

	subtotal := utils.NumericToFloat64(orderSubtotal)
	tax := utils.NumericToFloat64(taxAmount)
	serviceChargeAmount := utils.NumericToFloat64(serviceCharge)
	packagingFeeAmount := utils.NumericToFloat64(packagingFee)

	splitBill := make([]map[string]any, 0, len(participantsArray))
	for _, p := range participantsArray {
		itemSubtotal := p["itemSubtotal"].(float64)
		shareRatio := 0.0
		if subtotal > 0 {
			shareRatio = itemSubtotal / subtotal
		}
		taxShare := round2(tax * shareRatio)
		serviceShare := round2(serviceChargeAmount * shareRatio)
		packagingShare := round2(packagingFeeAmount * shareRatio)
		total := round2(itemSubtotal + taxShare + serviceShare + packagingShare)

		splitBill = append(splitBill, map[string]any{
			"participantId":      p["id"],
			"participantName":    p["name"],
			"isHost":             p["isHost"],
			"itemCount":          len(p["items"].([]map[string]any)),
			"subtotal":           itemSubtotal,
			"taxShare":           taxShare,
			"serviceChargeShare": serviceShare,
			"packagingFeeShare":  packagingShare,
			"total":              total,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"isGroupOrder": true,
			"session": map[string]any{
				"id": sessionID.Int64,
			},
			"participants": participantsArray,
			"splitBill":    splitBill,
		},
	})
}

type participantBasic struct {
	ID     int64
	Name   string
	IsHost bool
}

type groupOrderDetailRow struct {
	ParticipantID int64
	OrderItemID   int64
	MenuName      string
	MenuPrice     float64
	Quantity      int32
	Notes         *string
	ItemSubtotal  float64
	Addons        []map[string]any
}

func (h *Handler) fetchGroupOrderParticipantBasics(ctx context.Context, sessionID int64) ([]participantBasic, error) {
	query := `select id, name, is_host from group_order_participants where session_id = $1 order by joined_at asc`
	rows, err := h.DB.Query(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	participants := make([]participantBasic, 0)
	for rows.Next() {
		var p participantBasic
		if err := rows.Scan(&p.ID, &p.Name, &p.IsHost); err != nil {
			return nil, err
		}
		participants = append(participants, p)
	}
	return participants, nil
}

func (h *Handler) fetchGroupOrderDetails(ctx context.Context, sessionID int64) ([]groupOrderDetailRow, error) {
	query := `
		select d.participant_id, d.order_item_id, d.item_subtotal,
		       oi.menu_name, oi.menu_price, oi.quantity, oi.notes
		from group_order_details d
		join order_items oi on oi.id = d.order_item_id
		where d.session_id = $1
	`
	rows, err := h.DB.Query(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowsData := make([]groupOrderDetailRow, 0)
	itemIDs := make([]int64, 0)
	for rows.Next() {
		var row groupOrderDetailRow
		var itemSubtotal pgtype.Numeric
		var menuPrice pgtype.Numeric
		if err := rows.Scan(&row.ParticipantID, &row.OrderItemID, &itemSubtotal, &row.MenuName, &menuPrice, &row.Quantity, &row.Notes); err != nil {
			return nil, err
		}
		row.ItemSubtotal = utils.NumericToFloat64(itemSubtotal)
		row.MenuPrice = utils.NumericToFloat64(menuPrice)
		rowsData = append(rowsData, row)
		itemIDs = append(itemIDs, row.OrderItemID)
	}

	addonsMap, err := h.fetchOrderItemAddons(ctx, itemIDs)
	if err != nil {
		return nil, err
	}

	for i := range rowsData {
		addons := addonsMap[rowsData[i].OrderItemID]
		addonPayloads := make([]map[string]any, 0, len(addons))
		for _, addon := range addons {
			addonPayloads = append(addonPayloads, map[string]any{
				"name":     addon.Name,
				"price":    addon.Price,
				"quantity": addon.Quantity,
			})
		}
		rowsData[i].Addons = addonPayloads
	}

	return rowsData, nil
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}
