package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

const posCustomPlaceholderMenuName = "[POS] __CUSTOM_ITEM_PLACEHOLDER__"

type posOrderRequest struct {
	OrderType   string         `json:"orderType"`
	TableNumber *string        `json:"tableNumber"`
	Notes       *string        `json:"notes"`
	Items       []posOrderItem `json:"items"`
	Customer    *posCustomer   `json:"customer"`
}

type posOrderItem struct {
	Type        string          `json:"type"`
	MenuID      any             `json:"menuId"`
	CustomName  string          `json:"customName"`
	CustomPrice *float64        `json:"customPrice"`
	Quantity    int32           `json:"quantity"`
	Notes       *string         `json:"notes"`
	Addons      []posOrderAddon `json:"addons"`
}

type posOrderAddon struct {
	AddonItemID any   `json:"addonItemId"`
	Quantity    int32 `json:"quantity"`
}

type posCustomer struct {
	Name  string `json:"name"`
	Phone string `json:"phone"`
	Email string `json:"email"`
}

type posCustomSettings struct {
	Enabled       bool
	MaxNameLength int
	MaxPrice      float64
}

type posOrderItemData struct {
	MenuID    int64
	MenuName  string
	MenuPrice float64
	Quantity  int32
	Subtotal  float64
	Notes     *string
	Addons    []posAddonData
	IsCustom  bool
}

type posAddonData struct {
	AddonItemID int64
	AddonName   string
	AddonPrice  float64
	Quantity    int32
	Subtotal    float64
}

func (h *Handler) MerchantPOSCreateOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body posOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Invalid request body")
		return
	}

	if body.OrderType != "DINE_IN" && body.OrderType != "TAKEAWAY" {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Invalid order type. Must be DINE_IN or TAKEAWAY.")
		return
	}
	if len(body.Items) == 0 {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Order must have at least one item.")
		return
	}

	merchant, settings, err := h.loadPOSMerchant(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", "Merchant not found")
		return
	}

	customerID, err := h.resolvePOSCustomer(ctx, body.Customer)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}

	orderItems, subtotal, menuItemRefs, err := h.buildPOSOrderItems(ctx, merchant, settings, body.Items, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "POS_VALIDATION_ERROR", err.Error())
		return
	}

	fees := h.computePOSFees(merchant, body.OrderType, subtotal)
	totalAmount := round2(subtotal + fees.taxAmount + fees.serviceChargeAmount + fees.packagingFeeAmount)

	orderNumber, err := h.generatePOSOrderNumber(ctx, merchant.ID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate order number")
		return
	}

	createdOrder, err := h.createPOSOrder(ctx, merchant.ID, customerID, body, orderNumber, subtotal, fees, totalAmount, orderItems)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create order")
		return
	}

	// Best-effort stock decrement
	for _, item := range menuItemRefs {
		h.decrementPOSStock(ctx, item.MenuID, item.Quantity)
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       createdOrder,
		"message":    "Order created successfully",
		"statusCode": 201,
	})
}

type merchantPOSConfig struct {
	ID                          int64
	Code                        string
	Name                        string
	Currency                    string
	Timezone                    string
	Features                    []byte
	EnableTax                   bool
	TaxPercentage               float64
	EnableServiceCharge         bool
	ServiceChargePercent        float64
	EnablePackagingFee          bool
	PackagingFeeAmount          float64
	StockAlertEnabled           bool
	DefaultLowStockThreshold    int32
	RequireTableNumberForDineIn bool
}

func (h *Handler) loadPOSMerchant(ctx context.Context, merchantID int64) (merchantPOSConfig, posCustomSettings, error) {
	var (
		cfg            merchantPOSConfig
		features       []byte
		taxPercent     pgtype.Numeric
		servicePercent pgtype.Numeric
		packagingFee   pgtype.Numeric
	)

	err := h.DB.QueryRow(ctx, `
		select id, code, name, currency, timezone, features, enable_tax, tax_percentage,
		       enable_service_charge, service_charge_percent, enable_packaging_fee, packaging_fee_amount,
		       stock_alert_enabled, default_low_stock_threshold, require_table_number_for_dine_in
		from merchants where id = $1
	`, merchantID).Scan(
		&cfg.ID,
		&cfg.Code,
		&cfg.Name,
		&cfg.Currency,
		&cfg.Timezone,
		&features,
		&cfg.EnableTax,
		&taxPercent,
		&cfg.EnableServiceCharge,
		&servicePercent,
		&cfg.EnablePackagingFee,
		&packagingFee,
		&cfg.StockAlertEnabled,
		&cfg.DefaultLowStockThreshold,
		&cfg.RequireTableNumberForDineIn,
	)
	if err != nil {
		return merchantPOSConfig{}, posCustomSettings{}, err
	}

	cfg.Features = features
	if taxPercent.Valid {
		cfg.TaxPercentage = utils.NumericToFloat64(taxPercent)
	}
	if servicePercent.Valid {
		cfg.ServiceChargePercent = utils.NumericToFloat64(servicePercent)
	}
	if packagingFee.Valid {
		cfg.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
	}

	settings := parsePosCustomItemsSettings(cfg.Features, cfg.Currency)
	return cfg, settings, nil
}

func (h *Handler) resolvePOSCustomer(ctx context.Context, customer *posCustomer) (*int64, error) {
	if customer == nil {
		return nil, nil
	}
	name := strings.TrimSpace(customer.Name)
	phone := strings.TrimSpace(customer.Phone)
	email := strings.TrimSpace(strings.ToLower(customer.Email))

	if name == "" && phone == "" && email == "" {
		return nil, nil
	}

	if email != "" {
		var id int64
		if err := h.DB.QueryRow(ctx, `select id from customers where email = $1`, email).Scan(&id); err == nil {
			if name != "" || phone != "" {
				_, _ = h.DB.Exec(ctx, `update customers set name = coalesce(nullif($1,''), name), phone = coalesce(nullif($2,''), phone) where id = $3`, name, phone, id)
			}
			return &id, nil
		}

		var newID int64
		if err := h.DB.QueryRow(ctx, `insert into customers (name, email, phone, updated_at) values ($1,$2,$3, now()) returning id`,
			defaultString(name, "Walk-in Customer"), email, nullIfEmpty(phone)).Scan(&newID); err != nil {
			return nil, err
		}
		return &newID, nil
	}

	if phone != "" {
		var id int64
		if err := h.DB.QueryRow(ctx, `select id from customers where phone = $1`, phone).Scan(&id); err == nil {
			if name != "" {
				_, _ = h.DB.Exec(ctx, `update customers set name = coalesce(nullif($1,''), name) where id = $2`, name, id)
			}
			return &id, nil
		}
	}

	return nil, nil
}

type menuItemRef struct {
	MenuID   int64
	Quantity int32
}

func (h *Handler) buildPOSOrderItems(ctx context.Context, merchant merchantPOSConfig, settings posCustomSettings, items []posOrderItem, userID int64) ([]posOrderItemData, float64, []menuItemRef, error) {
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
			return nil, 0, nil, errInvalid("Invalid menuId")
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
		return nil, 0, nil, err
	}

	var placeholderMenuID *int64
	if len(customItems) > 0 {
		id, err := h.getOrCreateCustomPlaceholderMenu(ctx, merchant.ID, userID)
		if err != nil {
			return nil, 0, nil, err
		}
		placeholderMenuID = &id
	}

	orderItems := make([]posOrderItemData, 0)
	var subtotal float64
	menuRefs := make([]menuItemRef, 0)

	for _, item := range items {
		if strings.EqualFold(item.Type, "CUSTOM") {
			if !settings.Enabled {
				return nil, 0, nil, errInvalid("Custom items are disabled for this merchant.")
			}
			if len(item.Addons) > 0 {
				return nil, 0, nil, errInvalid("Custom items do not support addons.")
			}
			name := strings.TrimSpace(item.CustomName)
			if name == "" {
				return nil, 0, nil, errInvalid("Custom item name is required.")
			}
			if len(name) > settings.MaxNameLength {
				return nil, 0, nil, errInvalid("Custom item name is too long.")
			}
			if item.CustomPrice == nil || !isValidPrice(*item.CustomPrice) {
				return nil, 0, nil, errInvalid("Custom item price must be a valid number.")
			}
			if *item.CustomPrice > settings.MaxPrice {
				return nil, 0, nil, errInvalid("Custom item price is too high.")
			}
			if item.Quantity <= 0 {
				return nil, 0, nil, errInvalid("Invalid quantity.")
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
			return nil, 0, nil, errInvalid("Invalid menuId")
		}
		menu, ok := menus[menuID]
		if !ok {
			return nil, 0, nil, errInvalid("Menu item not found")
		}
		if !menu.IsActive || menu.DeletedAt.Valid {
			return nil, 0, nil, errInvalid("Menu item is not available")
		}
		if menu.TrackStock && (!menu.StockQty.Valid || menu.StockQty.Int32 < item.Quantity) {
			return nil, 0, nil, errInvalid("Insufficient stock")
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
		menuRefs = append(menuRefs, menuItemRef{MenuID: menu.ID, Quantity: item.Quantity})
	}

	return orderItems, subtotal, menuRefs, nil
}

type posMenuData struct {
	ID         int64
	Name       string
	IsActive   bool
	DeletedAt  pgtype.Timestamptz
	TrackStock bool
	StockQty   pgtype.Int4
	Price      float64
}

type posAddonItem struct {
	ID        int64
	Name      string
	IsActive  bool
	DeletedAt pgtype.Timestamptz
	Price     float64
}

func (h *Handler) loadPOSMenuData(ctx context.Context, merchantID int64, menuIDs []int64, addonIDs []int64) (map[int64]posMenuData, map[int64]posAddonItem, map[int64]float64, error) {
	menus := make(map[int64]posMenuData)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select id, name, is_active, deleted_at, track_stock, stock_qty, price
			from menus
			where id = any($1) and merchant_id = $2
		`, menuIDs, merchantID)
		if err != nil {
			return nil, nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var menu posMenuData
			var price pgtype.Numeric
			if err := rows.Scan(&menu.ID, &menu.Name, &menu.IsActive, &menu.DeletedAt, &menu.TrackStock, &menu.StockQty, &price); err != nil {
				continue
			}
			menu.Price = utils.NumericToFloat64(price)
			menus[menu.ID] = menu
		}
	}

	addons := make(map[int64]posAddonItem)
	if len(addonIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select id, name, is_active, deleted_at, price
			from addon_items
			where id = any($1)
		`, addonIDs)
		if err != nil {
			return nil, nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var addon posAddonItem
			var price pgtype.Numeric
			if err := rows.Scan(&addon.ID, &addon.Name, &addon.IsActive, &addon.DeletedAt, &price); err != nil {
				continue
			}
			addon.Price = utils.NumericToFloat64(price)
			addons[addon.ID] = addon
		}
	}

	promoMap := make(map[int64]float64)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select spi.menu_id, spi.promo_price
			from special_price_items spi
			join special_prices sp on sp.id = spi.special_price_id
			where spi.menu_id = any($1)
			  and sp.merchant_id = $2
			  and sp.is_active = true
			  and sp.start_date <= current_date
			  and sp.end_date >= current_date
		`, menuIDs, merchantID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var menuID int64
				var promoPrice pgtype.Numeric
				if err := rows.Scan(&menuID, &promoPrice); err != nil {
					continue
				}
				promoMap[menuID] = utils.NumericToFloat64(promoPrice)
			}
		}
	}

	return menus, addons, promoMap, nil
}

type posFees struct {
	taxAmount           float64
	serviceChargeAmount float64
	packagingFeeAmount  float64
}

func (h *Handler) computePOSFees(merchant merchantPOSConfig, orderType string, subtotal float64) posFees {
	var fees posFees
	if merchant.EnableTax && merchant.TaxPercentage > 0 {
		fees.taxAmount = round2(subtotal * (merchant.TaxPercentage / 100))
	}
	if merchant.EnableServiceCharge && merchant.ServiceChargePercent > 0 {
		fees.serviceChargeAmount = round2(subtotal * (merchant.ServiceChargePercent / 100))
	}
	if orderType == "TAKEAWAY" && merchant.EnablePackagingFee && merchant.PackagingFeeAmount > 0 {
		fees.packagingFeeAmount = round2(merchant.PackagingFeeAmount)
	}
	return fees
}

func (h *Handler) generatePOSOrderNumber(ctx context.Context, merchantID int64) (string, error) {
	characters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for attempt := 0; attempt < 10; attempt++ {
		var orderNumber strings.Builder
		for i := 0; i < 4; i++ {
			orderNumber.WriteByte(characters[rand.Intn(len(characters))])
		}
		value := orderNumber.String()
		start := time.Now().Truncate(24 * time.Hour)
		end := start.Add(24*time.Hour - time.Millisecond)
		var exists bool
		_ = h.DB.QueryRow(ctx, `
			select exists(
				select 1 from orders where merchant_id = $1 and order_number = $2 and placed_at >= $3 and placed_at <= $4
			)
		`, merchantID, value, start, end).Scan(&exists)
		if !exists {
			return value, nil
		}
	}
	return strings.ToUpper(time.Now().Format("1504")), nil
}

func (h *Handler) createPOSOrder(ctx context.Context, merchantID int64, customerID *int64, body posOrderRequest, orderNumber string, subtotal float64, fees posFees, totalAmount float64, items []posOrderItemData) (map[string]any, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var orderID int64
	if err := tx.QueryRow(ctx, `
		insert into orders (
			merchant_id, customer_id, order_number, order_type, table_number, status,
			subtotal, tax_amount, service_charge_amount, packaging_fee, total_amount, notes,
			updated_at
		)
		values (
			$1,$2,$3,$4,$5,'ACCEPTED',
			$6,$7,$8,$9,$10,$11,
			now()
		)
		returning id
	`, merchantID, customerID, orderNumber, body.OrderType, nullIfEmptyPtr(body.TableNumber), subtotal, fees.taxAmount, fees.serviceChargeAmount, fees.packagingFeeAmount, totalAmount, nullIfEmptyPtr(body.Notes)).Scan(&orderID); err != nil {
		return nil, err
	}

	for _, item := range items {
		var orderItemID int64
		if err := tx.QueryRow(ctx, `
			insert into order_items (order_id, menu_id, menu_name, menu_price, quantity, subtotal, notes, updated_at)
			values ($1,$2,$3,$4,$5,$6,$7, now())
			returning id
		`, orderID, item.MenuID, item.MenuName, item.MenuPrice, item.Quantity, item.Subtotal, nullIfEmptyPtr(item.Notes)).Scan(&orderItemID); err != nil {
			return nil, err
		}

		if len(item.Addons) > 0 {
			for _, addon := range item.Addons {
				if _, err := tx.Exec(ctx, `
					insert into order_item_addons (order_item_id, addon_item_id, addon_name, addon_price, quantity, subtotal, updated_at)
					values ($1,$2,$3,$4,$5,$6, now())
				`, orderItemID, addon.AddonItemID, addon.AddonName, addon.AddonPrice, addon.Quantity, addon.Subtotal); err != nil {
					return nil, err
				}
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		insert into payments (order_id, amount, payment_method, status, updated_at)
		values ($1,$2,'CASH_ON_COUNTER','PENDING', now())
	`, orderID, totalAmount); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return h.fetchPOSOrderDetails(ctx, orderID)
}

func (h *Handler) decrementPOSStock(ctx context.Context, menuID int64, quantity int32) {
	var trackStock bool
	var stockQty pgtype.Int4
	if err := h.DB.QueryRow(ctx, `select track_stock, stock_qty from menus where id = $1`, menuID).Scan(&trackStock, &stockQty); err != nil {
		return
	}
	if trackStock && stockQty.Valid {
		newQty := stockQty.Int32 - quantity
		_, _ = h.DB.Exec(ctx, `update menus set stock_qty = $1, is_active = $2 where id = $3`, newQty, newQty > 0, menuID)
	}
}

func (h *Handler) getOrCreateCustomPlaceholderMenu(ctx context.Context, merchantID int64, userID int64) (int64, error) {
	var id int64
	if err := h.DB.QueryRow(ctx, `
		select id from menus where merchant_id = $1 and name = $2 and deleted_at is not null limit 1
	`, merchantID, posCustomPlaceholderMenuName).Scan(&id); err == nil {
		return id, nil
	}

	deletedAt := time.Now()
	if err := h.DB.QueryRow(ctx, `
		insert into menus (merchant_id, name, description, price, is_active, track_stock, stock_qty, deleted_at, deleted_by_user_id, created_by_user_id, updated_at)
		values ($1,$2,$3,0,false,false,null,$4,$5,$5, now())
		returning id
	`, merchantID, posCustomPlaceholderMenuName, "Internal placeholder for POS custom items", deletedAt, userID).Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (h *Handler) fetchPOSOrderDetails(ctx context.Context, orderID int64) (map[string]any, error) {
	var (
		orderNumber      string
		orderType        string
		status           string
		tableNumber      pgtype.Text
		subtotal         pgtype.Numeric
		taxAmount        pgtype.Numeric
		totalAmount      pgtype.Numeric
		notes            pgtype.Text
		merchantID       int64
		merchantCode     string
		merchantName     string
		merchantCurrency string
		customerID       pgtype.Int8
		customerName     pgtype.Text
		customerEmail    pgtype.Text
		customerPhone    pgtype.Text
		paymentID        pgtype.Int8
		paymentStatus    pgtype.Text
		paymentMethod    pgtype.Text
		paymentAmount    pgtype.Numeric
	)

	query := `
		select o.order_number, o.order_type, o.status, o.table_number, o.subtotal, o.tax_amount, o.total_amount, o.notes,
		       m.id, m.code, m.name, m.currency,
		       c.id, c.name, c.email, c.phone,
		       p.id, p.status, p.payment_method, p.amount
		from orders o
		join merchants m on m.id = o.merchant_id
		left join customers c on c.id = o.customer_id
		left join payments p on p.order_id = o.id
		where o.id = $1
	`

	if err := h.DB.QueryRow(ctx, query, orderID).Scan(
		&orderNumber,
		&orderType,
		&status,
		&tableNumber,
		&subtotal,
		&taxAmount,
		&totalAmount,
		&notes,
		&merchantID,
		&merchantCode,
		&merchantName,
		&merchantCurrency,
		&customerID,
		&customerName,
		&customerEmail,
		&customerPhone,
		&paymentID,
		&paymentStatus,
		&paymentMethod,
		&paymentAmount,
	); err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0)
	itemRows, err := h.DB.Query(ctx, `
		select oi.id, oi.menu_name, oi.quantity, oi.menu_price, oi.subtotal, oi.notes
		from order_items oi where oi.order_id = $1
	`, orderID)
	if err == nil {
		defer itemRows.Close()
		for itemRows.Next() {
			var (
				itemID       int64
				menuName     string
				quantity     int32
				menuPrice    pgtype.Numeric
				subtotalItem pgtype.Numeric
				note         pgtype.Text
			)
			if err := itemRows.Scan(&itemID, &menuName, &quantity, &menuPrice, &subtotalItem, &note); err != nil {
				continue
			}
			addons := make([]map[string]any, 0)
			addonRows, err := h.DB.Query(ctx, `
				select addon_name, addon_price, quantity, subtotal
				from order_item_addons where order_item_id = $1
			`, itemID)
			if err == nil {
				for addonRows.Next() {
					var addonName string
					var addonPrice pgtype.Numeric
					var addonQty int32
					var addonSubtotal pgtype.Numeric
					if err := addonRows.Scan(&addonName, &addonPrice, &addonQty, &addonSubtotal); err != nil {
						continue
					}
					addons = append(addons, map[string]any{
						"addonName":  addonName,
						"addonPrice": utils.NumericToFloat64(addonPrice),
						"quantity":   addonQty,
						"subtotal":   utils.NumericToFloat64(addonSubtotal),
					})
				}
				addonRows.Close()
			}

			entry := map[string]any{
				"id":        itemID,
				"menuName":  menuName,
				"quantity":  quantity,
				"unitPrice": utils.NumericToFloat64(menuPrice),
				"subtotal":  utils.NumericToFloat64(subtotalItem),
				"addons":    addons,
			}
			if note.Valid {
				entry["notes"] = note.String
			}
			items = append(items, entry)
		}
	}

	result := map[string]any{
		"id":          orderID,
		"orderNumber": orderNumber,
		"orderType":   orderType,
		"status":      status,
		"tableNumber": valueOrNil(tableNumber),
		"subtotal":    utils.NumericToFloat64(subtotal),
		"taxAmount":   utils.NumericToFloat64(taxAmount),
		"totalAmount": utils.NumericToFloat64(totalAmount),
		"notes":       valueOrNil(notes),
		"orderItems":  items,
		"customer": map[string]any{
			"id":    valueOrNilInt(customerID),
			"name":  valueOrNil(customerName),
			"email": valueOrNil(customerEmail),
			"phone": valueOrNil(customerPhone),
		},
		"payment": map[string]any{
			"id":            valueOrNilInt(paymentID),
			"status":        valueOrNil(paymentStatus),
			"paymentMethod": valueOrNil(paymentMethod),
			"amount":        utils.NumericToFloat64(paymentAmount),
		},
		"merchant": map[string]any{
			"id":       merchantID,
			"code":     merchantCode,
			"name":     merchantName,
			"currency": merchantCurrency,
		},
	}

	return result, nil
}

func valueOrNil(value pgtype.Text) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func valueOrNilInt(value pgtype.Int8) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}

func parsePosCustomItemsSettings(raw []byte, currency string) posCustomSettings {
	settings := posCustomSettings{Enabled: false, MaxNameLength: 80, MaxPrice: defaultMaxPrice(currency)}
	if len(raw) == 0 {
		return settings
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return settings
	}
	pos, _ := data["pos"].(map[string]any)
	custom, _ := pos["customItems"].(map[string]any)
	if custom == nil {
		return settings
	}
	if enabled, ok := custom["enabled"].(bool); ok {
		settings.Enabled = enabled
	}
	if maxName, ok := asFloat(custom["maxNameLength"]); ok && maxName > 0 {
		settings.MaxNameLength = int(math.Floor(maxName))
	}
	if maxPrice, ok := asFloat(custom["maxPrice"]); ok && maxPrice > 0 {
		settings.MaxPrice = maxPrice
	}
	return settings
}

func defaultMaxPrice(currency string) float64 {
	switch strings.ToUpper(currency) {
	case "AUD":
		return 5000
	case "USD":
		return 5000
	case "SGD":
		return 5000
	case "MYR":
		return 10000
	default:
		return 10000000
	}
}

func asFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func isValidPrice(value float64) bool {
	return !math.IsNaN(value) && value > 0
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullIfEmptyPtr(value *string) any {
	if value == nil {
		return nil
	}
	return nullIfEmpty(*value)
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func errInvalid(message string) error {
	return errors.New(message)
}
