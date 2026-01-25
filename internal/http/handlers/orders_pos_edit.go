package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/internal/voucher"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type posEditSettings struct {
	Enabled bool
}

func parsePosEditOrderSettings(raw []byte) posEditSettings {
	settings := posEditSettings{Enabled: false}
	if len(raw) == 0 {
		return settings
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return settings
	}
	pos, _ := data["pos"].(map[string]any)
	editOrder, _ := pos["editOrder"].(map[string]any)
	if editOrder != nil {
		if enabled, ok := editOrder["enabled"].(bool); ok {
			settings.Enabled = enabled
		}
	}
	return settings
}

func (h *Handler) MerchantPOSOrderGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid orderId")
		return
	}

	merchant, editEnabled, err := h.ensurePosEditEnabled(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}
	if !editEnabled.Enabled {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Edit order is disabled for this merchant.")
		return
	}

	order, err := h.fetchPosEditOrder(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if order["status"] != "PENDING" && order["status"] != "ACCEPTED" {
		response.Error(w, http.StatusBadRequest, "ORDER_NOT_EDITABLE", "Only PENDING or ACCEPTED orders can be edited")
		return
	}
	if order["orderType"] != "DINE_IN" && order["orderType"] != "TAKEAWAY" {
		response.Error(w, http.StatusBadRequest, "ORDER_TYPE_NOT_SUPPORTED", "Only dine-in or takeaway orders can be edited in POS")
		return
	}

	_ = merchant
	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       order,
		"statusCode": 200,
	})
}

func (h *Handler) MerchantPOSOrderUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid orderId")
		return
	}

	merchant, editEnabled, err := h.ensurePosEditEnabled(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}
	if !editEnabled.Enabled {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Edit order is disabled for this merchant.")
		return
	}

	existingOrder, err := h.loadPosEditOrder(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if existingOrder.Status != "PENDING" && existingOrder.Status != "ACCEPTED" {
		response.Error(w, http.StatusBadRequest, "ORDER_NOT_EDITABLE", "Only PENDING or ACCEPTED orders can be edited.")
		return
	}
	if existingOrder.OrderType != "DINE_IN" && existingOrder.OrderType != "TAKEAWAY" {
		response.Error(w, http.StatusBadRequest, "ORDER_TYPE_NOT_SUPPORTED", "Only dine-in or takeaway orders can be edited in POS.")
		return
	}
	if existingOrder.PaymentStatus == "COMPLETED" {
		response.Error(w, http.StatusBadRequest, "ORDER_ALREADY_PAID", "Paid orders cannot be edited.")
		return
	}

	var body posOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Invalid request body")
		return
	}
	if body.OrderType != "DINE_IN" && body.OrderType != "TAKEAWAY" {
		response.Error(w, http.StatusBadRequest, "INVALID_ORDER_TYPE", "Invalid order type. Must be DINE_IN or TAKEAWAY.")
		return
	}
	if body.OrderType != existingOrder.OrderType {
		response.Error(w, http.StatusBadRequest, "ORDER_TYPE_MISMATCH", "Order type cannot be changed in edit mode.")
		return
	}
	if len(body.Items) == 0 {
		response.Error(w, http.StatusBadRequest, "EMPTY_ITEMS", "Order must have at least one item.")
		return
	}
	for i := range body.Items {
		if strings.EqualFold(body.Items[i].Type, "CUSTOM") {
			body.Items[i].Type = "CUSTOM"
		} else {
			body.Items[i].Type = "MENU"
		}
	}
	if body.OrderType == "DINE_IN" && merchant.RequireTableNumberForDineIn {
		if body.TableNumber == nil || strings.TrimSpace(*body.TableNumber) == "" {
			response.Error(w, http.StatusBadRequest, "TABLE_NUMBER_REQUIRED", "Table number is required for dine-in orders.")
			return
		}
	}

	customerID, err := h.resolvePOSCustomer(ctx, body.Customer)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}
	if customerID == nil {
		customerID = existingOrder.CustomerID
	}

	orderItems, subtotal, err := h.buildPOSOrderItemsForEdit(ctx, merchant, parsePosCustomItemsSettings(merchant.Features, merchant.Currency), body.Items, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}

	fees := h.computePOSFees(merchant, body.OrderType, subtotal)

	// Recalculate discounts based on existing order discounts
	discounts, discountAmount, err := h.recalculateOrderDiscounts(ctx, merchant, existingOrder, subtotal, orderItems, customerID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}
	if len(existingOrder.OrderDiscounts) == 0 {
		discountAmount = existingOrder.DiscountAmount
	}

	totalAmount := round2(subtotal + fees.taxAmount + fees.serviceChargeAmount + fees.packagingFeeAmount - discountAmount)
	if totalAmount < 0 {
		totalAmount = 0
	}

	stockAdjustments, err := h.preparePOSStockAdjustments(ctx, existingOrder, orderItems)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}

	if err := h.applyPosOrderUpdate(ctx, merchant, existingOrder, body, customerID, orderItems, discounts, subtotal, fees, discountAmount, totalAmount, &authCtx.UserID, stockAdjustments); err != nil {
		if strings.Contains(err.Error(), "Insufficient stock") {
			response.Error(w, http.StatusBadRequest, "INSUFFICIENT_STOCK", err.Error())
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order")
		return
	}

	updatedOrder, err := h.fetchPOSOrderDetails(ctx, orderID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update order")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updatedOrder,
		"message":    "Order updated successfully",
		"statusCode": 200,
	})
}

type posEditOrder struct {
	ID              int64
	OrderType       string
	Status          string
	PaymentStatus   string
	CustomerID      *int64
	DiscountAmount  float64
	IsScheduled     bool
	StockDeductedAt *time.Time
	OrderDiscounts  []posOrderDiscount
	Items           []posOrderItemSnapshot
}

type posOrderItemSnapshot struct {
	MenuID   int64
	Quantity int32
	Addons   []posAddonSnapshot
	MenuName string
	IsCustom bool
}

type posAddonSnapshot struct {
	AddonID  int64
	Quantity int32
}

type posOrderDiscount struct {
	Source              string
	Label               string
	DiscountType        string
	DiscountValue       *float64
	DiscountAmount      float64
	VoucherTemplateID   *int64
	VoucherCode         *string
	VoucherCodeID       *int64
	AppliedByUserID     *int64
	AppliedByCustomerID *int64
}

func (h *Handler) ensurePosEditEnabled(ctx context.Context, merchantID int64) (merchantPOSConfig, posEditSettings, error) {
	merchant, _, err := h.loadPOSMerchant(ctx, merchantID)
	if err != nil {
		return merchantPOSConfig{}, posEditSettings{}, err
	}
	settings := parsePosEditOrderSettings(merchant.Features)
	return merchant, settings, nil
}

func (h *Handler) fetchPosEditOrder(ctx context.Context, merchantID int64, orderID int64) (map[string]any, error) {
	var (
		orderNumber   string
		orderType     string
		status        string
		tableNumber   pgtype.Text
		notes         pgtype.Text
		customerID    pgtype.Int8
		customerName  pgtype.Text
		customerEmail pgtype.Text
		customerPhone pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `
		select o.order_number, o.order_type, o.status, o.table_number, o.notes,
		       c.id, c.name, c.email, c.phone
		from orders o
		left join customers c on c.id = o.customer_id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, merchantID).Scan(
		&orderNumber,
		&orderType,
		&status,
		&tableNumber,
		&notes,
		&customerID,
		&customerName,
		&customerEmail,
		&customerPhone,
	); err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0)
	itemRows, err := h.DB.Query(ctx, `
		select oi.id, oi.menu_id, oi.menu_name, oi.menu_price, oi.quantity, oi.notes,
		       m.name, m.image_url
		from order_items oi
		left join menus m on m.id = oi.menu_id
		where oi.order_id = $1
	`, orderID)
	if err == nil {
		defer itemRows.Close()
		for itemRows.Next() {
			var (
				itemID         int64
				menuID         int64
				menuName       string
				menuPrice      pgtype.Numeric
				quantity       int32
				note           pgtype.Text
				menuRecordName pgtype.Text
				imageURL       pgtype.Text
			)
			if err := itemRows.Scan(&itemID, &menuID, &menuName, &menuPrice, &quantity, &note, &menuRecordName, &imageURL); err != nil {
				continue
			}

			isCustom := menuRecordName.Valid && menuRecordName.String == posCustomPlaceholderMenuName
			entry := map[string]any{
				"type":      "MENU",
				"menuId":    menuID,
				"menuName":  menuName,
				"menuPrice": utils.NumericToFloat64(menuPrice),
				"quantity":  quantity,
				"addons":    []map[string]any{},
				"imageUrl":  valueOrNil(imageURL),
			}
			if note.Valid {
				entry["notes"] = note.String
			}

			if isCustom {
				entry["type"] = "CUSTOM"
				entry["menuId"] = nil
				entry["customName"] = menuName
				entry["customPrice"] = utils.NumericToFloat64(menuPrice)
				entry["addons"] = []map[string]any{}
				entry["imageUrl"] = nil
			} else {
				addons := make([]map[string]any, 0)
				addonRows, err := h.DB.Query(ctx, `
					select addon_item_id, addon_name, addon_price, quantity
					from order_item_addons
					where order_item_id = $1
				`, itemID)
				if err == nil {
					for addonRows.Next() {
						var addonID int64
						var addonName string
						var addonPrice pgtype.Numeric
						var addonQty int32
						if err := addonRows.Scan(&addonID, &addonName, &addonPrice, &addonQty); err != nil {
							continue
						}
						addons = append(addons, map[string]any{
							"addonItemId": addonID,
							"addonName":   addonName,
							"addonPrice":  utils.NumericToFloat64(addonPrice),
							"quantity":    addonQty,
						})
					}
					addonRows.Close()
				}
				entry["addons"] = addons
			}
			items = append(items, entry)
		}
	}

	return map[string]any{
		"id":          orderID,
		"orderNumber": orderNumber,
		"status":      status,
		"orderType":   orderType,
		"tableNumber": valueOrNil(tableNumber),
		"notes":       valueOrNil(notes),
		"customer": map[string]any{
			"id":    valueOrNilInt(customerID),
			"name":  valueOrNil(customerName),
			"email": valueOrNil(customerEmail),
			"phone": valueOrNil(customerPhone),
		},
		"items": items,
	}, nil
}

func (h *Handler) loadPosEditOrder(ctx context.Context, merchantID int64, orderID int64) (posEditOrder, error) {
	var (
		orderType       string
		status          string
		paymentStatus   pgtype.Text
		customerID      pgtype.Int8
		discountAmount  pgtype.Numeric
		isScheduled     bool
		stockDeductedAt pgtype.Timestamptz
	)
	if err := h.DB.QueryRow(ctx, `
		select o.order_type, o.status, p.status, o.customer_id, o.discount_amount, o.is_scheduled, o.stock_deducted_at
		from orders o
		left join payments p on p.order_id = o.id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, merchantID).Scan(&orderType, &status, &paymentStatus, &customerID, &discountAmount, &isScheduled, &stockDeductedAt); err != nil {
		return posEditOrder{}, err
	}

	order := posEditOrder{ID: orderID, OrderType: orderType, Status: status, IsScheduled: isScheduled}
	if paymentStatus.Valid {
		order.PaymentStatus = paymentStatus.String
	}
	if customerID.Valid {
		value := customerID.Int64
		order.CustomerID = &value
	}
	if discountAmount.Valid {
		order.DiscountAmount = utils.NumericToFloat64(discountAmount)
	}
	if stockDeductedAt.Valid {
		value := stockDeductedAt.Time
		order.StockDeductedAt = &value
	}

	// Load order items
	itemRows, _ := h.DB.Query(ctx, `
		select oi.id, oi.menu_id, oi.quantity, oi.menu_name, m.name
		from order_items oi
		left join menus m on m.id = oi.menu_id
		where oi.order_id = $1
	`, orderID)
	if itemRows != nil {
		defer itemRows.Close()
		for itemRows.Next() {
			var (
				itemID         int64
				menuID         int64
				quantity       int32
				menuName       string
				menuRecordName pgtype.Text
			)
			if err := itemRows.Scan(&itemID, &menuID, &quantity, &menuName, &menuRecordName); err != nil {
				continue
			}
			isCustom := menuRecordName.Valid && menuRecordName.String == posCustomPlaceholderMenuName
			item := posOrderItemSnapshot{MenuID: menuID, Quantity: quantity, MenuName: menuName, IsCustom: isCustom}
			addonRows, _ := h.DB.Query(ctx, `select addon_item_id, quantity from order_item_addons where order_item_id = $1`, itemID)
			if addonRows != nil {
				for addonRows.Next() {
					var addonID int64
					var addonQty int32
					if err := addonRows.Scan(&addonID, &addonQty); err != nil {
						continue
					}
					item.Addons = append(item.Addons, posAddonSnapshot{AddonID: addonID, Quantity: addonQty})
				}
				addonRows.Close()
			}
			order.Items = append(order.Items, item)
		}
	}

	// Load discounts
	rows, _ := h.DB.Query(ctx, `
		select d.source, d.label, d.discount_type, d.discount_value, d.discount_amount,
		       d.voucher_template_id, d.voucher_code_id, d.applied_by_user_id, d.applied_by_customer_id,
		       vc.code
		from order_discounts d
		left join order_voucher_codes vc on vc.id = d.voucher_code_id
		where d.order_id = $1
	`, orderID)
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var (
				source              string
				label               string
				discountType        string
				discountValue       pgtype.Numeric
				discountAmount      pgtype.Numeric
				voucherTemplateID   pgtype.Int8
				voucherCodeID       pgtype.Int8
				appliedByUserID     pgtype.Int8
				appliedByCustomerID pgtype.Int8
			)
			var voucherCode pgtype.Text
			if err := rows.Scan(&source, &label, &discountType, &discountValue, &discountAmount, &voucherTemplateID, &voucherCodeID, &appliedByUserID, &appliedByCustomerID, &voucherCode); err != nil {
				continue
			}
			d := posOrderDiscount{
				Source:         source,
				Label:          label,
				DiscountType:   discountType,
				DiscountAmount: utils.NumericToFloat64(discountAmount),
			}
			if discountValue.Valid {
				value := utils.NumericToFloat64(discountValue)
				d.DiscountValue = &value
			}
			if voucherTemplateID.Valid {
				value := voucherTemplateID.Int64
				d.VoucherTemplateID = &value
			}
			if voucherCodeID.Valid {
				value := voucherCodeID.Int64
				d.VoucherCodeID = &value
			}
			if voucherCode.Valid {
				value := voucherCode.String
				d.VoucherCode = &value
			}
			if appliedByUserID.Valid {
				value := appliedByUserID.Int64
				d.AppliedByUserID = &value
			}
			if appliedByCustomerID.Valid {
				value := appliedByCustomerID.Int64
				d.AppliedByCustomerID = &value
			}
			order.OrderDiscounts = append(order.OrderDiscounts, d)
		}
	}

	return order, nil
}

type posAppliedDiscount struct {
	Source              string
	Label               string
	DiscountType        string
	DiscountValue       *float64
	DiscountAmount      float64
	VoucherTemplateID   *int64
	VoucherCodeID       *int64
	AppliedByUserID     *int64
	AppliedByCustomerID *int64
}

type posStockMenu struct {
	ID         int64
	Name       string
	TrackStock bool
	StockQty   pgtype.Int4
}

type posStockAddon struct {
	ID         int64
	Name       string
	TrackStock bool
	StockQty   pgtype.Int4
}

type posStockAdjustment struct {
	ShouldAdjust bool
	Menus        []posStockMenu
	Addons       []posStockAddon
	OldMenuQty   map[int64]int32
	NewMenuQty   map[int64]int32
	OldAddonQty  map[int64]int32
	NewAddonQty  map[int64]int32
}

func (h *Handler) buildPOSOrderItemsForEdit(ctx context.Context, merchant merchantPOSConfig, settings posCustomSettings, items []posOrderItem, userID int64) ([]posOrderItemData, float64, error) {
	menuItems := make([]posOrderItem, 0)
	customItems := make([]posOrderItem, 0)
	for _, item := range items {
		if strings.EqualFold(item.Type, "CUSTOM") {
			customItems = append(customItems, item)
		} else {
			menuItems = append(menuItems, item)
		}
	}

	menuIDs := make([]int64, 0)
	addonIDs := make([]int64, 0)
	for _, item := range menuItems {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			return nil, 0, errInvalid("Invalid menuId")
		}
		menuIDs = append(menuIDs, menuID)
		for _, addon := range item.Addons {
			addonID, ok := parseNumericID(addon.AddonItemID)
			if ok {
				addonIDs = append(addonIDs, addonID)
			}
		}
	}

	menus, addons, promoMap, err := h.loadPOSMenuData(ctx, merchant.ID, menuIDs, addonIDs)
	if err != nil {
		return nil, 0, err
	}

	var placeholderMenuID *int64
	if len(customItems) > 0 {
		id, err := h.getOrCreateCustomPlaceholderMenu(ctx, merchant.ID, userID)
		if err != nil {
			return nil, 0, err
		}
		placeholderMenuID = &id
	}

	orderItems := make([]posOrderItemData, 0)
	var subtotal float64

	for _, item := range items {
		if strings.EqualFold(item.Type, "CUSTOM") {
			if !settings.Enabled {
				return nil, 0, errInvalid("Custom items are disabled for this merchant.")
			}
			if len(item.Addons) > 0 {
				return nil, 0, errInvalid("Custom items do not support addons.")
			}
			name := strings.TrimSpace(item.CustomName)
			if name == "" {
				return nil, 0, errInvalid("Custom item name is required.")
			}
			if len(name) > settings.MaxNameLength {
				return nil, 0, errInvalid("Custom item name is too long.")
			}
			if item.CustomPrice == nil || !isValidPrice(*item.CustomPrice) {
				return nil, 0, errInvalid("Custom item price must be a valid number.")
			}
			if *item.CustomPrice > settings.MaxPrice {
				return nil, 0, errInvalid("Custom item price is too high.")
			}
			if item.Quantity <= 0 {
				return nil, 0, errInvalid("Invalid quantity.")
			}
			price := round2(*item.CustomPrice)
			itemTotal := round2(price * float64(item.Quantity))
			subtotal = round2(subtotal + itemTotal)
			orderItems = append(orderItems, posOrderItemData{
				MenuID:    *placeholderMenuID,
				MenuName:  name,
				MenuPrice: price,
				Quantity:  item.Quantity,
				Subtotal:  itemTotal,
				Notes:     item.Notes,
				Addons:    []posAddonData{},
				IsCustom:  true,
			})
			continue
		}

		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			return nil, 0, errInvalid("Invalid menuId")
		}
		menu, ok := menus[menuID]
		if !ok {
			return nil, 0, errInvalid("Menu item not found")
		}
		if !menu.IsActive || menu.DeletedAt.Valid {
			return nil, 0, errInvalid("Menu item is not available")
		}
		if item.Quantity <= 0 {
			return nil, 0, errInvalid("Invalid quantity.")
		}

		price := menu.Price
		if promo, ok := promoMap[menuID]; ok {
			price = promo
		}
		menuPrice := round2(price)
		itemTotal := round2(menuPrice * float64(item.Quantity))

		addonData := make([]posAddonData, 0)
		for _, addon := range item.Addons {
			addonID, ok := parseNumericID(addon.AddonItemID)
			if !ok {
				continue
			}
			addonItem, ok := addons[addonID]
			if !ok {
				continue
			}
			if !addonItem.IsActive || addonItem.DeletedAt.Valid {
				continue
			}
			qty := addon.Quantity
			if qty <= 0 {
				qty = 1
			}
			addonPrice := round2(addonItem.Price)
			addonSubtotal := round2(addonPrice * float64(qty))
			itemTotal = round2(itemTotal + addonSubtotal)
			addonData = append(addonData, posAddonData{
				AddonItemID: addonItem.ID,
				AddonName:   addonItem.Name,
				AddonPrice:  addonPrice,
				Quantity:    qty,
				Subtotal:    addonSubtotal,
			})
		}

		subtotal = round2(subtotal + itemTotal)
		orderItems = append(orderItems, posOrderItemData{
			MenuID:    menu.ID,
			MenuName:  menu.Name,
			MenuPrice: menuPrice,
			Quantity:  item.Quantity,
			Subtotal:  itemTotal,
			Notes:     item.Notes,
			Addons:    addonData,
			IsCustom:  false,
		})
	}

	return orderItems, subtotal, nil
}

func (h *Handler) recalculateOrderDiscounts(ctx context.Context, merchant merchantPOSConfig, order posEditOrder, subtotal float64, items []posOrderItemData, customerID *int64) ([]posAppliedDiscount, float64, error) {
	if len(order.OrderDiscounts) == 0 {
		return nil, 0, nil
	}
	itemInputs := make([]voucher.OrderItemInput, 0, len(items))
	for _, item := range items {
		itemInputs = append(itemInputs, voucher.OrderItemInput{MenuID: item.MenuID, Subtotal: item.Subtotal})
	}
	merchantTimezone := merchant.Timezone
	if merchantTimezone == "" {
		merchantTimezone = "Australia/Sydney"
	}

	results := make([]posAppliedDiscount, 0)
	for _, discount := range order.OrderDiscounts {
		if discount.Source == "MANUAL" {
			amount := discount.DiscountAmount
			if discount.DiscountType == "PERCENTAGE" && discount.DiscountValue != nil {
				amount = round2(subtotal * (*discount.DiscountValue / 100))
			} else if discount.DiscountType == "FIXED_AMOUNT" && discount.DiscountValue != nil {
				amount = round2(*discount.DiscountValue)
			}
			if amount > 0 {
				results = append(results, posAppliedDiscount{
					Source:         "MANUAL",
					Label:          defaultString(discount.Label, "Manual discount"),
					DiscountType:   discount.DiscountType,
					DiscountValue:  discount.DiscountValue,
					DiscountAmount: amount,
				})
			}
			continue
		}

		if discount.Source != "POS_VOUCHER" && discount.Source != "CUSTOMER_VOUCHER" {
			continue
		}

		var voucherCode string
		if discount.VoucherCode != nil {
			voucherCode = *discount.VoucherCode
		}
		params := voucher.ComputeParams{
			MerchantID:              merchant.ID,
			MerchantCurrency:        merchant.Currency,
			MerchantTimezone:        merchantTimezone,
			Audience:                voucher.AudiencePOS,
			OrderType:               order.OrderType,
			Subtotal:                subtotal,
			Items:                   itemInputs,
			VoucherCode:             voucherCode,
			VoucherTemplateID:       discount.VoucherTemplateID,
			CustomerID:              customerID,
			ExcludeOrderIDFromUsage: &order.ID,
		}
		if discount.Source == "CUSTOMER_VOUCHER" {
			params.Audience = "CUSTOMER"
		}
		computed, verr := voucher.ComputeVoucherDiscount(ctx, h.DB, params)
		if verr != nil {
			return nil, 0, verr
		}
		amount := computed.DiscountAmount
		dType := string(computed.DiscountType)
		dValue := computed.DiscountValue
		dValuePtr := &dValue
		if discount.Source == "POS_VOUCHER" && discount.VoucherTemplateID != nil && voucherCode == "" && discount.DiscountValue != nil {
			eligible := computed.EligibleSubtotal
			override := *discount.DiscountValue
			if discount.DiscountType == "PERCENTAGE" {
				pct := math.Max(0, math.Min(override, 100))
				amount = round2(eligible * (pct / 100))
				if maxCap, err := h.loadVoucherMaxDiscount(ctx, computed.TemplateID); err == nil && maxCap != nil {
					if amount > *maxCap {
						amount = *maxCap
					}
				}
				dType = "PERCENTAGE"
				dValue = pct
			} else {
				amount = round2(math.Min(override, eligible))
				dType = "FIXED_AMOUNT"
				dValue = override
			}
			if amount > eligible {
				amount = eligible
			}
			dValuePtr = &dValue
		}
		if amount > 0 {
			results = append(results, posAppliedDiscount{
				Source:              discount.Source,
				Label:               computed.Label,
				DiscountType:        dType,
				DiscountValue:       dValuePtr,
				DiscountAmount:      amount,
				VoucherTemplateID:   &computed.TemplateID,
				VoucherCodeID:       computed.CodeID,
				AppliedByUserID:     discount.AppliedByUserID,
				AppliedByCustomerID: discount.AppliedByCustomerID,
			})
		}
	}

	var total float64
	for _, item := range results {
		total += item.DiscountAmount
	}
	return results, round2(total), nil
}

func (h *Handler) preparePOSStockAdjustments(ctx context.Context, existing posEditOrder, newItems []posOrderItemData) (posStockAdjustment, error) {
	shouldAdjust := !existing.IsScheduled || existing.StockDeductedAt != nil
	adjustments := posStockAdjustment{
		ShouldAdjust: shouldAdjust,
		OldMenuQty:   map[int64]int32{},
		NewMenuQty:   map[int64]int32{},
		OldAddonQty:  map[int64]int32{},
		NewAddonQty:  map[int64]int32{},
	}

	if !shouldAdjust {
		return adjustments, nil
	}

	for _, item := range existing.Items {
		if item.IsCustom {
			continue
		}
		adjustments.OldMenuQty[item.MenuID] += item.Quantity
		for _, addon := range item.Addons {
			adjustments.OldAddonQty[addon.AddonID] += addon.Quantity
		}
	}

	for _, item := range newItems {
		if item.IsCustom {
			continue
		}
		adjustments.NewMenuQty[item.MenuID] += item.Quantity
		for _, addon := range item.Addons {
			adjustments.NewAddonQty[addon.AddonItemID] += addon.Quantity
		}
	}

	menuIDs := make([]int64, 0)
	for id := range adjustments.OldMenuQty {
		menuIDs = append(menuIDs, id)
	}
	for id := range adjustments.NewMenuQty {
		if _, ok := adjustments.OldMenuQty[id]; !ok {
			menuIDs = append(menuIDs, id)
		}
	}

	addonIDs := make([]int64, 0)
	for id := range adjustments.OldAddonQty {
		addonIDs = append(addonIDs, id)
	}
	for id := range adjustments.NewAddonQty {
		if _, ok := adjustments.OldAddonQty[id]; !ok {
			addonIDs = append(addonIDs, id)
		}
	}

	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select id, name, track_stock, stock_qty
			from menus
			where id = any($1)
		`, menuIDs)
		if err != nil {
			return adjustments, err
		}
		defer rows.Close()
		for rows.Next() {
			var menu posStockMenu
			if err := rows.Scan(&menu.ID, &menu.Name, &menu.TrackStock, &menu.StockQty); err != nil {
				continue
			}
			adjustments.Menus = append(adjustments.Menus, menu)
		}
	}

	if len(addonIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select id, name, track_stock, stock_qty
			from addon_items
			where id = any($1)
		`, addonIDs)
		if err != nil {
			return adjustments, err
		}
		defer rows.Close()
		for rows.Next() {
			var addon posStockAddon
			if err := rows.Scan(&addon.ID, &addon.Name, &addon.TrackStock, &addon.StockQty); err != nil {
				continue
			}
			adjustments.Addons = append(adjustments.Addons, addon)
		}
	}

	for _, menu := range adjustments.Menus {
		if !menu.TrackStock || !menu.StockQty.Valid {
			continue
		}
		oldQty := adjustments.OldMenuQty[menu.ID]
		newQty := adjustments.NewMenuQty[menu.ID]
		delta := int32(newQty - oldQty)
		if delta > 0 && menu.StockQty.Int32 < delta {
			return adjustments, errInvalid("Insufficient stock for \"" + menu.Name + "\".")
		}
	}

	for _, addon := range adjustments.Addons {
		if !addon.TrackStock || !addon.StockQty.Valid {
			continue
		}
		oldQty := adjustments.OldAddonQty[addon.ID]
		newQty := adjustments.NewAddonQty[addon.ID]
		delta := int32(newQty - oldQty)
		if delta > 0 && addon.StockQty.Int32 < delta {
			return adjustments, errInvalid("Insufficient stock for \"" + addon.Name + "\".")
		}
	}

	return adjustments, nil
}

func (h *Handler) applyPosOrderUpdate(ctx context.Context, merchant merchantPOSConfig, existing posEditOrder, body posOrderRequest, customerID *int64, items []posOrderItemData, discounts []posAppliedDiscount, subtotal float64, fees posFees, discountAmount float64, totalAmount float64, editedByUserID *int64, stockAdjustments posStockAdjustment) error {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if stockAdjustments.ShouldAdjust {
		for _, menu := range stockAdjustments.Menus {
			if !menu.TrackStock || !menu.StockQty.Valid {
				continue
			}
			oldQty := stockAdjustments.OldMenuQty[menu.ID]
			newQty := stockAdjustments.NewMenuQty[menu.ID]
			delta := int32(newQty - oldQty)
			if delta == 0 {
				continue
			}
			if delta > 0 {
				cmd, err := tx.Exec(ctx, `
					update menus
					set stock_qty = stock_qty - $1
					where id = $2 and track_stock = true and stock_qty >= $1
				`, delta, menu.ID)
				if err != nil {
					return err
				}
				if cmd.RowsAffected() != 1 {
					return errInvalid("Insufficient stock for \"" + menu.Name + "\".")
				}
			} else {
				inc := -delta
				if _, err := tx.Exec(ctx, `
					update menus
					set stock_qty = stock_qty + $1
					where id = $2
				`, inc, menu.ID); err != nil {
					return err
				}
			}

			var stockQty pgtype.Int4
			if err := tx.QueryRow(ctx, `select stock_qty from menus where id = $1`, menu.ID).Scan(&stockQty); err == nil {
				if stockQty.Valid {
					_, _ = tx.Exec(ctx, `update menus set is_active = $1 where id = $2`, stockQty.Int32 > 0, menu.ID)
				}
			}
		}

		for _, addon := range stockAdjustments.Addons {
			if !addon.TrackStock || !addon.StockQty.Valid {
				continue
			}
			oldQty := stockAdjustments.OldAddonQty[addon.ID]
			newQty := stockAdjustments.NewAddonQty[addon.ID]
			delta := int32(newQty - oldQty)
			if delta == 0 {
				continue
			}
			if delta > 0 {
				cmd, err := tx.Exec(ctx, `
					update addon_items
					set stock_qty = stock_qty - $1
					where id = $2 and track_stock = true and stock_qty >= $1
				`, delta, addon.ID)
				if err != nil {
					return err
				}
				if cmd.RowsAffected() != 1 {
					return errInvalid("Insufficient stock for \"" + addon.Name + "\".")
				}
			} else {
				inc := -delta
				if _, err := tx.Exec(ctx, `
					update addon_items
					set stock_qty = stock_qty + $1
					where id = $2
				`, inc, addon.ID); err != nil {
					return err
				}
			}

			var stockQty pgtype.Int4
			if err := tx.QueryRow(ctx, `select stock_qty from addon_items where id = $1`, addon.ID).Scan(&stockQty); err == nil {
				if stockQty.Valid {
					_, _ = tx.Exec(ctx, `update addon_items set is_active = $1 where id = $2`, stockQty.Int32 > 0, addon.ID)
				}
			}
		}
	}

	_, err = tx.Exec(ctx, `delete from order_item_addons where order_item_id in (select id from order_items where order_id = $1)`, existing.ID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `delete from order_items where order_id = $1`, existing.ID)
	if err != nil {
		return err
	}

	for _, item := range items {
		var orderItemID int64
		if err := tx.QueryRow(ctx, `
			insert into order_items (order_id, menu_id, menu_name, menu_price, quantity, subtotal, notes)
			values ($1,$2,$3,$4,$5,$6,$7)
			returning id
		`, existing.ID, item.MenuID, item.MenuName, item.MenuPrice, item.Quantity, item.Subtotal, nullIfEmptyPtr(item.Notes)).Scan(&orderItemID); err != nil {
			return err
		}
		if len(item.Addons) > 0 {
			for _, addon := range item.Addons {
				if _, err := tx.Exec(ctx, `
					insert into order_item_addons (order_item_id, addon_item_id, addon_name, addon_price, quantity, subtotal)
					values ($1,$2,$3,$4,$5,$6)
				`, orderItemID, addon.AddonItemID, addon.AddonName, addon.AddonPrice, addon.Quantity, addon.Subtotal); err != nil {
					return err
				}
			}
		}
	}

	if len(existing.OrderDiscounts) > 0 {
		_, err = tx.Exec(ctx, `delete from order_discounts where order_id = $1`, existing.ID)
		if err != nil {
			return err
		}
		for _, discount := range discounts {
			if err := voucher.ApplyOrderDiscountTx(ctx, tx, voucher.ApplyParams{
				MerchantID:          merchant.ID,
				OrderID:             existing.ID,
				Source:              discount.Source,
				Currency:            merchant.Currency,
				Label:               discount.Label,
				DiscountType:        discount.DiscountType,
				DiscountValue:       discount.DiscountValue,
				DiscountAmount:      discount.DiscountAmount,
				VoucherTemplateID:   discount.VoucherTemplateID,
				VoucherCodeID:       discount.VoucherCodeID,
				AppliedByUserID:     discount.AppliedByUserID,
				AppliedByCustomerID: discount.AppliedByCustomerID,
			}); err != nil {
				return err
			}
		}
	}

	if _, err := tx.Exec(ctx, `update payments set amount = $1 where order_id = $2`, totalAmount, existing.ID); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		update orders
		set customer_id = $1, table_number = $2, notes = $3, subtotal = $4, tax_amount = $5,
		    service_charge_amount = $6, packaging_fee = $7, discount_amount = $8, total_amount = $9,
		    edited_at = $10, edited_by_user_id = $11
		where id = $12
	`, customerID, nullIfEmptyPtr(body.TableNumber), nullIfEmptyPtr(body.Notes), subtotal, fees.taxAmount, fees.serviceChargeAmount, fees.packagingFeeAmount, discountAmount, totalAmount, time.Now(), editedByUserID, existing.ID)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
