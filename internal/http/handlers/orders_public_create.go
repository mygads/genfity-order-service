package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/internal/voucher"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type publicOrderRequest struct {
	MerchantCode           string            `json:"merchantCode"`
	CustomerName           string            `json:"customerName"`
	CustomerEmail          string            `json:"customerEmail"`
	CustomerPhone          *string           `json:"customerPhone"`
	OrderType              string            `json:"orderType"`
	ScheduledTime          *string           `json:"scheduledTime"`
	TableNumber            *string           `json:"tableNumber"`
	DeliveryUnit           *string           `json:"deliveryUnit"`
	DeliveryBuildingName   *string           `json:"deliveryBuildingName"`
	DeliveryBuildingNumber *string           `json:"deliveryBuildingNumber"`
	DeliveryFloor          *string           `json:"deliveryFloor"`
	DeliveryInstructions   *string           `json:"deliveryInstructions"`
	DeliveryAddress        *string           `json:"deliveryAddress"`
	DeliveryStreetLine     *string           `json:"deliveryStreetLine"`
	DeliverySuburb         *string           `json:"deliverySuburb"`
	DeliveryCity           *string           `json:"deliveryCity"`
	DeliveryState          *string           `json:"deliveryState"`
	DeliveryPostcode       *string           `json:"deliveryPostcode"`
	DeliveryCountry        *string           `json:"deliveryCountry"`
	DeliveryLatitude       *float64          `json:"deliveryLatitude"`
	DeliveryLongitude      *float64          `json:"deliveryLongitude"`
	Items                  []publicOrderItem `json:"items"`
	Notes                  *string           `json:"notes"`
	PaymentMethod          *string           `json:"paymentMethod"`
	VoucherCode            *string           `json:"voucherCode"`
}

type publicOrderItem struct {
	MenuID   any                `json:"menuId"`
	Quantity int32              `json:"quantity"`
	Notes    *string            `json:"notes"`
	Addons   []publicOrderAddon `json:"addons"`
}

type publicOrderAddon struct {
	AddonItemID any   `json:"addonItemId"`
	Quantity    int32 `json:"quantity"`
}

type publicOrderMerchant struct {
	ID                          int64
	Code                        string
	Name                        string
	Currency                    string
	Timezone                    string
	IsActive                    bool
	IsScheduledOrderEnabled     bool
	CustomerVouchersEnabled     bool
	EnableTax                   bool
	TaxPercentage               float64
	EnableServiceCharge         bool
	ServiceChargePercent        float64
	EnablePackagingFee          bool
	PackagingFeeAmount          float64
	IsPerDayModeScheduleEnabled bool
	IsDineInEnabled             bool
	IsTakeawayEnabled           bool
	IsDeliveryEnabled           bool
	DineInScheduleStart         *string
	DineInScheduleEnd           *string
	TakeawayScheduleStart       *string
	TakeawayScheduleEnd         *string
	DeliveryScheduleStart       *string
	DeliveryScheduleEnd         *string
}

type publicOrderCreateResponse struct {
	OrderDetail
	TrackingToken string `json:"trackingToken"`
}

func (h *Handler) PublicOrderCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body publicOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantCode := strings.TrimSpace(body.MerchantCode)
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	orderType := strings.ToUpper(strings.TrimSpace(body.OrderType))
	if orderType != "DINE_IN" && orderType != "TAKEAWAY" && orderType != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Valid order type is required (DINE_IN, TAKEAWAY, or DELIVERY)")
		return
	}

	if strings.TrimSpace(body.CustomerName) == "" || strings.TrimSpace(body.CustomerEmail) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Customer name and email are required")
		return
	}

	if len(body.Items) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order must have at least one item")
		return
	}

	merchant, availability, deliveryCfg, err := h.loadPublicOrderMerchant(ctx, merchantCode)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_INACTIVE", "Merchant is currently not accepting orders")
		return
	}
	if !merchant.IsActive {
		response.Error(w, http.StatusBadRequest, "MERCHANT_INACTIVE", "Merchant is currently not accepting orders")
		return
	}

	scheduledTime := ""
	if body.ScheduledTime != nil {
		scheduledTime = strings.TrimSpace(*body.ScheduledTime)
	}
	isScheduled := scheduledTime != ""
	if isScheduled {
		if !merchant.IsScheduledOrderEnabled {
			response.Error(w, http.StatusBadRequest, "SCHEDULED_ORDERS_DISABLED", "Scheduled orders are not enabled for this merchant.")
			return
		}
		if !isValidHHMM(scheduledTime) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "scheduledTime must be in HH:MM format")
			return
		}
		currentHHMM := currentTimeHHMMInTZ(availability.timezone)
		if scheduledTime < currentHHMM {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Scheduled time must be later than current time ("+currentHHMM+")")
			return
		}
	}

	timeHHMM := currentTimeHHMMInTZ(availability.timezone)
	if isScheduled {
		timeHHMM = scheduledTime
	}

	if !isStoreOpenWithSpecialHoursAtTime(availability, timeHHMM) || !isModeAvailableWithSchedulesAtTime(orderType, availability, timeHHMM) {
		response.Error(w, http.StatusBadRequest, "MODE_UNAVAILABLE", "This order mode is currently unavailable")
		return
	}

	customerID, err := h.resolveGroupOrderCustomer(ctx, body.CustomerName, body.CustomerEmail, body.CustomerPhone)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create customer")
		return
	}

	orderItems, subtotal, menuRefs, voucherItems, err := h.buildPublicOrderItems(ctx, merchant.ID, body.Items)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	taxAmount := 0.0
	if merchant.EnableTax && merchant.TaxPercentage > 0 {
		taxAmount = round2(subtotal * (merchant.TaxPercentage / 100))
	}
	serviceChargeAmount := 0.0
	if merchant.EnableServiceCharge && merchant.ServiceChargePercent > 0 {
		serviceChargeAmount = round2(subtotal * (merchant.ServiceChargePercent / 100))
	}
	packagingFeeAmount := 0.0
	if (orderType == "TAKEAWAY" || orderType == "DELIVERY") && merchant.EnablePackagingFee && merchant.PackagingFeeAmount > 0 {
		packagingFeeAmount = round2(merchant.PackagingFeeAmount)
	}

	deliveryFeeAmount := 0.0
	var deliveryDistance *float64
	if orderType == "DELIVERY" {
		if body.DeliveryAddress == nil || strings.TrimSpace(*body.DeliveryAddress) == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Delivery address is required")
			return
		}
		if body.DeliveryLatitude == nil || body.DeliveryLongitude == nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Delivery coordinates are required")
			return
		}

		fee, distance, code, msg := h.calculateGroupOrderDeliveryFee(ctx, deliveryCfg, *body.DeliveryLatitude, *body.DeliveryLongitude)
		if code != "" {
			response.Error(w, http.StatusBadRequest, code, msg)
			return
		}
		deliveryFeeAmount = fee
		deliveryDistance = &distance
	}

	totalAmount := round2(subtotal + taxAmount + serviceChargeAmount + packagingFeeAmount + deliveryFeeAmount)

	voucherCode := ""
	if body.VoucherCode != nil {
		voucherCode = strings.TrimSpace(*body.VoucherCode)
	}
	voucherCode = strings.ToUpper(voucherCode)

	var voucherDiscount *voucher.DiscountResult
	if voucherCode != "" {
		if !merchant.CustomerVouchersEnabled {
			response.Error(w, http.StatusBadRequest, "VOUCHERS_DISABLED", "Customer vouchers are not enabled for this merchant")
			return
		}
		params := voucher.ComputeParams{
			MerchantID:       merchant.ID,
			MerchantCurrency: merchant.Currency,
			MerchantTimezone: merchant.Timezone,
			Audience:         voucher.Audience("CUSTOMER"),
			OrderType:        orderType,
			Subtotal:         subtotal,
			Items:            voucherItems,
			VoucherCode:      voucherCode,
			CustomerID:       customerID,
		}
		computed, verr := voucher.ComputeVoucherDiscount(ctx, h.DB, params)
		if verr != nil {
			writeVoucherError(w, verr)
			return
		}
		voucherDiscount = computed
	}

	discountAmount := 0.0
	if voucherDiscount != nil {
		discountAmount = voucherDiscount.DiscountAmount
	}

	totalAfterDiscount := round2(math.Max(0, totalAmount-discountAmount))

	paymentMethod := resolvePublicPaymentMethod(orderType, body.PaymentMethod)
	if paymentMethod == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payment method")
		return
	}

	orderNumber, err := h.generatePOSOrderNumber(ctx, merchant.ID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate order number")
		return
	}

	orderID, err := h.createPublicOrder(ctx, merchant, orderType, orderNumber, customerID, body, orderItems, subtotal, taxAmount, serviceChargeAmount, packagingFeeAmount, deliveryFeeAmount, deliveryDistance, totalAfterDiscount, isScheduled, scheduledTime, paymentMethod, voucherDiscount)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create order")
		return
	}

	if !isScheduled {
		for _, item := range menuRefs {
			h.decrementPOSStock(ctx, item.MenuID, item.Quantity)
		}
	}

	detail, err := h.fetchPublicOrderDetailByID(ctx, orderID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve order")
		return
	}

	token := utils.CreateOrderTrackingToken(h.Config.OrderTrackingTokenSecret, merchant.Code, detail.OrderNumber)
	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": publicOrderCreateResponse{
			OrderDetail:   detail,
			TrackingToken: token,
		},
		"message":    "Order created successfully",
		"statusCode": 201,
	})
}

func (h *Handler) buildPublicOrderItems(ctx context.Context, merchantID int64, items []publicOrderItem) ([]posOrderItemData, float64, []menuItemRef, []voucher.OrderItemInput, error) {
	menuIDs := make([]int64, 0)
	addonIDs := make([]int64, 0)

	for _, item := range items {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			return nil, 0, nil, nil, errInvalid("Invalid menuId")
		}
		menuIDs = append(menuIDs, menuID)
		for _, addon := range item.Addons {
			addonID, ok := parseNumericID(addon.AddonItemID)
			if ok {
				addonIDs = append(addonIDs, addonID)
			}
		}
	}

	_, menuMap, err := h.fetchGroupOrderMenus(ctx, merchantID, menuIDs)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	addonMap, err := h.fetchGroupOrderAddons(ctx, addonIDs)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	promoMap := h.fetchGroupOrderPromoPrices(ctx, menuIDs, merchantID)

	orderItems := make([]posOrderItemData, 0)
	voucherItems := make([]voucher.OrderItemInput, 0)
	menuRefs := make([]menuItemRef, 0)
	var subtotal float64

	for _, item := range items {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			return nil, 0, nil, nil, errInvalid("Invalid menuId")
		}
		menu, ok := menuMap[menuID]
		if !ok {
			return nil, 0, nil, nil, errInvalid("Menu item not found")
		}
		if !menu.IsActive || menu.DeletedAt.Valid {
			return nil, 0, nil, nil, errInvalid("Menu item is not available")
		}
		if menu.TrackStock && (!menu.StockQty.Valid || menu.StockQty.Int32 < item.Quantity) {
			return nil, 0, nil, nil, errInvalid("Insufficient stock")
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
			addonItem, ok := addonMap[addonID]
			if !ok || !addonItem.IsActive || addonItem.DeletedAt.Valid {
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
		voucherItems = append(voucherItems, voucher.OrderItemInput{MenuID: menu.ID, Subtotal: itemTotal})
	}

	return orderItems, subtotal, menuRefs, voucherItems, nil
}

func (h *Handler) loadPublicOrderMerchant(ctx context.Context, code string) (publicOrderMerchant, availableTimesMerchant, groupOrderDeliveryConfig, error) {
	var (
		merchant              publicOrderMerchant
		isOpen                bool
		isManualOverride      bool
		featuresBytes         []byte
		taxPercent            pgtype.Numeric
		servicePercent        pgtype.Numeric
		packagingFee          pgtype.Numeric
		dineInScheduleStart   pgtype.Text
		dineInScheduleEnd     pgtype.Text
		takeawayScheduleStart pgtype.Text
		takeawayScheduleEnd   pgtype.Text
		deliveryScheduleStart pgtype.Text
		deliveryScheduleEnd   pgtype.Text
		deliveryConfig        groupOrderDeliveryConfig
		isPerDayModeSchedule  bool
	)

	err := h.DB.QueryRow(ctx, `
        select id, code, name, currency, timezone, is_active, is_open, is_manual_override,
               is_scheduled_order_enabled, is_per_day_mode_schedule_enabled,
               is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
               dine_in_schedule_start, dine_in_schedule_end,
               takeaway_schedule_start, takeaway_schedule_end,
               delivery_schedule_start, delivery_schedule_end,
               enable_tax, tax_percentage,
               enable_service_charge, service_charge_percent,
               enable_packaging_fee, packaging_fee_amount,
               features,
               is_active, is_delivery_enabled, enforce_delivery_zones, latitude, longitude, delivery_max_distance_km,
               delivery_fee_base, delivery_fee_per_km, delivery_fee_min, delivery_fee_max
        from merchants
        where code = $1
    `, code).Scan(
		&merchant.ID,
		&merchant.Code,
		&merchant.Name,
		&merchant.Currency,
		&merchant.Timezone,
		&merchant.IsActive,
		&isOpen,
		&isManualOverride,
		&merchant.IsScheduledOrderEnabled,
		&isPerDayModeSchedule,
		&merchant.IsDineInEnabled,
		&merchant.IsTakeawayEnabled,
		&merchant.IsDeliveryEnabled,
		&dineInScheduleStart,
		&dineInScheduleEnd,
		&takeawayScheduleStart,
		&takeawayScheduleEnd,
		&deliveryScheduleStart,
		&deliveryScheduleEnd,
		&merchant.EnableTax,
		&taxPercent,
		&merchant.EnableServiceCharge,
		&servicePercent,
		&merchant.EnablePackagingFee,
		&packagingFee,
		&featuresBytes,
		&deliveryConfig.IsActive,
		&deliveryConfig.IsDeliveryEnabled,
		&deliveryConfig.EnforceDeliveryZones,
		&deliveryConfig.Latitude,
		&deliveryConfig.Longitude,
		&deliveryConfig.DeliveryMaxDistance,
		&deliveryConfig.DeliveryFeeBase,
		&deliveryConfig.DeliveryFeePerKm,
		&deliveryConfig.DeliveryFeeMin,
		&deliveryConfig.DeliveryFeeMax,
	)
	if err != nil {
		return publicOrderMerchant{}, availableTimesMerchant{}, groupOrderDeliveryConfig{}, err
	}

	deliveryConfig.MerchantID = merchant.ID
	merchant.Timezone = timezoneOrDefault(merchant.Timezone)
	if strings.TrimSpace(merchant.Currency) == "" {
		merchant.Currency = "AUD"
	}

	if taxPercent.Valid {
		merchant.TaxPercentage = utils.NumericToFloat64(taxPercent)
	}
	if servicePercent.Valid {
		merchant.ServiceChargePercent = utils.NumericToFloat64(servicePercent)
	}
	if packagingFee.Valid {
		merchant.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
	}

	merchant.DineInScheduleStart = textPtr(dineInScheduleStart)
	merchant.DineInScheduleEnd = textPtr(dineInScheduleEnd)
	merchant.TakeawayScheduleStart = textPtr(takeawayScheduleStart)
	merchant.TakeawayScheduleEnd = textPtr(takeawayScheduleEnd)
	merchant.DeliveryScheduleStart = textPtr(deliveryScheduleStart)
	merchant.DeliveryScheduleEnd = textPtr(deliveryScheduleEnd)
	merchant.IsPerDayModeScheduleEnabled = isPerDayModeSchedule

	if len(featuresBytes) > 0 {
		var features map[string]any
		if err := json.Unmarshal(featuresBytes, &features); err == nil {
			if ovRaw, ok := features["orderVouchers"]; ok {
				if ov, ok := ovRaw.(map[string]any); ok {
					customerEnabledRaw, _ := ov["customerEnabled"].(bool)
					posDiscountsEnabledRaw, _ := ov["posDiscountsEnabled"].(bool)
					merchant.CustomerVouchersEnabled = customerEnabledRaw && posDiscountsEnabledRaw
				}
			}
		}
	}

	openingHours := make([]merchantStatusOpeningHour, 0)
	hoursRows, err := h.DB.Query(ctx, `
        select id, day_of_week, is_closed, open_time, close_time
        from merchant_opening_hours
        where merchant_id = $1
        order by day_of_week asc
    `, merchant.ID)
	if err == nil {
		defer hoursRows.Close()
		for hoursRows.Next() {
			var hrow merchantStatusOpeningHour
			var openTime pgtype.Text
			var closeTime pgtype.Text
			if err := hoursRows.Scan(&hrow.ID, &hrow.DayOfWeek, &hrow.IsClosed, &openTime, &closeTime); err == nil {
				if openTime.Valid {
					hrow.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					hrow.CloseTime = &closeTime.String
				}
				openingHours = append(openingHours, hrow)
			}
		}
	}

	modeSchedules := make([]merchantStatusModeSchedule, 0)
	modeRows, err := h.DB.Query(ctx, `
        select id, mode, day_of_week, start_time, end_time, is_active
        from merchant_mode_schedules
        where merchant_id = $1
        order by mode asc, day_of_week asc
    `, merchant.ID)
	if err == nil {
		defer modeRows.Close()
		for modeRows.Next() {
			var m merchantStatusModeSchedule
			if err := modeRows.Scan(&m.ID, &m.Mode, &m.DayOfWeek, &m.StartTime, &m.EndTime, &m.IsActive); err == nil {
				modeSchedules = append(modeSchedules, m)
			}
		}
	}

	var todaySpecial *merchantStatusSpecialHour
	dateISO := currentDateISOInTZ(merchant.Timezone)
	if dateISO != "" {
		if dateValue, err := time.Parse("2006-01-02", dateISO); err == nil {
			var special merchantStatusSpecialHour
			var name pgtype.Text
			var openTime pgtype.Text
			var closeTime pgtype.Text
			var isDineIn pgtype.Bool
			var isTakeaway pgtype.Bool
			var isDelivery pgtype.Bool
			var dineInStart pgtype.Text
			var dineInEnd pgtype.Text
			var takeawayStart pgtype.Text
			var takeawayEnd pgtype.Text
			var deliveryStart pgtype.Text
			var deliveryEnd pgtype.Text
			if err := h.DB.QueryRow(ctx, `
                select id, date, name, is_closed, open_time, close_time,
                       is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
                       dine_in_start_time, dine_in_end_time,
                       takeaway_start_time, takeaway_end_time,
                       delivery_start_time, delivery_end_time
                from merchant_special_hours
                where merchant_id = $1 and date = $2
            `, merchant.ID, dateValue).Scan(
				&special.ID,
				&special.Date,
				&name,
				&special.IsClosed,
				&openTime,
				&closeTime,
				&isDineIn,
				&isTakeaway,
				&isDelivery,
				&dineInStart,
				&dineInEnd,
				&takeawayStart,
				&takeawayEnd,
				&deliveryStart,
				&deliveryEnd,
			); err == nil {
				if name.Valid {
					special.Name = &name.String
				}
				if openTime.Valid {
					special.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					special.CloseTime = &closeTime.String
				}
				if isDineIn.Valid {
					v := isDineIn.Bool
					special.IsDineInEnabled = &v
				}
				if isTakeaway.Valid {
					v := isTakeaway.Bool
					special.IsTakeawayEnabled = &v
				}
				if isDelivery.Valid {
					v := isDelivery.Bool
					special.IsDeliveryEnabled = &v
				}
				if dineInStart.Valid {
					special.DineInStartTime = &dineInStart.String
				}
				if dineInEnd.Valid {
					special.DineInEndTime = &dineInEnd.String
				}
				if takeawayStart.Valid {
					special.TakeawayStartTime = &takeawayStart.String
				}
				if takeawayEnd.Valid {
					special.TakeawayEndTime = &takeawayEnd.String
				}
				if deliveryStart.Valid {
					special.DeliveryStartTime = &deliveryStart.String
				}
				if deliveryEnd.Valid {
					special.DeliveryEndTime = &deliveryEnd.String
				}
				todaySpecial = &special
			}
		}
	}

	availability := availableTimesMerchant{
		isOpen:                      isOpen,
		isManualOverride:            isManualOverride,
		timezone:                    merchant.Timezone,
		isPerDayModeScheduleEnabled: isPerDayModeSchedule,
		isDineInEnabled:             merchant.IsDineInEnabled,
		isTakeawayEnabled:           merchant.IsTakeawayEnabled,
		isDeliveryEnabled:           merchant.IsDeliveryEnabled,
		dineInScheduleStart:         merchant.DineInScheduleStart,
		dineInScheduleEnd:           merchant.DineInScheduleEnd,
		takeawayScheduleStart:       merchant.TakeawayScheduleStart,
		takeawayScheduleEnd:         merchant.TakeawayScheduleEnd,
		deliveryScheduleStart:       merchant.DeliveryScheduleStart,
		deliveryScheduleEnd:         merchant.DeliveryScheduleEnd,
		openingHours:                openingHours,
		modeSchedules:               modeSchedules,
		todaySpecialHour:            todaySpecial,
	}

	return merchant, availability, deliveryConfig, nil
}

func (h *Handler) createPublicOrder(
	ctx context.Context,
	merchant publicOrderMerchant,
	orderType string,
	orderNumber string,
	customerID *int64,
	body publicOrderRequest,
	items []posOrderItemData,
	subtotal float64,
	taxAmount float64,
	serviceChargeAmount float64,
	packagingFeeAmount float64,
	deliveryFeeAmount float64,
	deliveryDistance *float64,
	totalAmount float64,
	isScheduled bool,
	scheduledTime string,
	paymentMethod string,
	voucherDiscount *voucher.DiscountResult,
) (int64, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	scheduledDate := ""
	if isScheduled {
		scheduledDate = currentDateISOInTZ(merchant.Timezone)
	}

	var orderID int64
	deliveryStatus := deliveryStatusForOrder(orderType)
	deliveryUnit := (*string)(nil)
	deliveryAddress := (*string)(nil)
	deliveryBuildingName := (*string)(nil)
	deliveryBuildingNumber := (*string)(nil)
	deliveryFloor := (*string)(nil)
	deliveryInstructions := (*string)(nil)
	deliveryStreetLine := (*string)(nil)
	deliverySuburb := (*string)(nil)
	deliveryCity := (*string)(nil)
	deliveryState := (*string)(nil)
	deliveryPostcode := (*string)(nil)
	deliveryCountry := (*string)(nil)
	deliveryLatitude := (*float64)(nil)
	deliveryLongitude := (*float64)(nil)

	if orderType == "DELIVERY" {
		deliveryUnit = body.DeliveryUnit
		deliveryAddress = body.DeliveryAddress
		deliveryBuildingName = body.DeliveryBuildingName
		deliveryBuildingNumber = body.DeliveryBuildingNumber
		deliveryFloor = body.DeliveryFloor
		deliveryInstructions = body.DeliveryInstructions
		deliveryStreetLine = body.DeliveryStreetLine
		deliverySuburb = body.DeliverySuburb
		deliveryCity = body.DeliveryCity
		deliveryState = body.DeliveryState
		deliveryPostcode = body.DeliveryPostcode
		deliveryCountry = body.DeliveryCountry
		deliveryLatitude = body.DeliveryLatitude
		deliveryLongitude = body.DeliveryLongitude
	}

	if err := tx.QueryRow(ctx, `
        insert into orders (
            merchant_id, customer_id, order_number, order_type, table_number, status,
            is_scheduled, scheduled_date, scheduled_time, stock_deducted_at,
            delivery_status, delivery_unit, delivery_address, delivery_fee_amount, delivery_distance_km,
            delivery_building_name, delivery_building_number, delivery_floor, delivery_instructions,
            delivery_street_line, delivery_suburb, delivery_city, delivery_state, delivery_postcode, delivery_country,
            delivery_latitude, delivery_longitude,
            subtotal, tax_amount, service_charge_amount, packaging_fee, discount_amount, total_amount, notes,
			updated_at
        ) values (
            $1,$2,$3,$4,$5,'PENDING',
            $6,$7,$8,$9,
            $10,$11,$12,$13,$14,
            $15,$16,$17,$18,
            $19,$20,$21,$22,$23,$24,
            $25,$26,
			$27,$28,$29,$30,$31,$32,$33,
			now()
        )
        returning id
    `,
		merchant.ID,
		customerID,
		orderNumber,
		orderType,
		nullIfEmptyPtr(body.TableNumber),
		isScheduled,
		nullIfEmpty(scheduledDate),
		nullIfEmpty(scheduledTime),
		nilIfScheduled(isScheduled),
		nullIfEmptyPtr(deliveryStatus),
		nullIfEmptyPtr(deliveryUnit),
		nullIfEmptyPtr(deliveryAddress),
		deliveryFeeAmount,
		deliveryDistance,
		nullIfEmptyPtr(deliveryBuildingName),
		nullIfEmptyPtr(deliveryBuildingNumber),
		nullIfEmptyPtr(deliveryFloor),
		nullIfEmptyPtr(deliveryInstructions),
		nullIfEmptyPtr(deliveryStreetLine),
		nullIfEmptyPtr(deliverySuburb),
		nullIfEmptyPtr(deliveryCity),
		nullIfEmptyPtr(deliveryState),
		nullIfEmptyPtr(deliveryPostcode),
		nullIfEmptyPtr(deliveryCountry),
		deliveryLatitude,
		deliveryLongitude,
		subtotal,
		taxAmount,
		serviceChargeAmount,
		packagingFeeAmount,
		0,
		totalAmount,
		nullIfEmptyPtr(body.Notes),
	).Scan(&orderID); err != nil {
		return 0, err
	}

	for _, item := range items {
		var orderItemID int64
		if err := tx.QueryRow(ctx, `
			insert into order_items (order_id, menu_id, menu_name, menu_price, quantity, subtotal, notes, updated_at)
			values ($1,$2,$3,$4,$5,$6,$7, now())
            returning id
        `, orderID, item.MenuID, item.MenuName, item.MenuPrice, item.Quantity, item.Subtotal, nullIfEmptyPtr(item.Notes)).Scan(&orderItemID); err != nil {
			return 0, err
		}

		if len(item.Addons) > 0 {
			for _, addon := range item.Addons {
				if _, err := tx.Exec(ctx, `
					insert into order_item_addons (order_item_id, addon_item_id, addon_name, addon_price, quantity, subtotal, updated_at)
					values ($1,$2,$3,$4,$5,$6, now())
                `, orderItemID, addon.AddonItemID, addon.AddonName, addon.AddonPrice, addon.Quantity, addon.Subtotal); err != nil {
					return 0, err
				}
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		insert into payments (order_id, amount, payment_method, status, updated_at)
		values ($1,$2,$3,'PENDING', now())
    `, orderID, totalAmount, paymentMethod); err != nil {
		return 0, err
	}

	if voucherDiscount != nil {
		value := voucherDiscount.DiscountValue
		verr := voucher.ApplyOrderDiscountTx(ctx, tx, voucher.ApplyParams{
			MerchantID:          merchant.ID,
			OrderID:             orderID,
			Source:              "CUSTOMER_VOUCHER",
			Currency:            merchant.Currency,
			Label:               voucherDiscount.Label,
			DiscountType:        string(voucherDiscount.DiscountType),
			DiscountValue:       &value,
			DiscountAmount:      voucherDiscount.DiscountAmount,
			VoucherTemplateID:   &voucherDiscount.TemplateID,
			VoucherCodeID:       voucherDiscount.CodeID,
			AppliedByCustomerID: customerID,
		})
		if verr != nil {
			return 0, verr
		}

		if _, err := tx.Exec(ctx, `update orders set total_amount = $1 where id = $2`, totalAmount, orderID); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return orderID, nil
}

func (h *Handler) fetchPublicOrderDetailByID(ctx context.Context, orderID int64) (OrderDetail, error) {
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
        where o.id = $1
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

	if err := h.DB.QueryRow(ctx, query, orderID).Scan(
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
		return OrderDetail{}, err
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

	items, err := h.fetchOrderItems(ctx, detail.ID)
	if err != nil {
		return OrderDetail{}, err
	}
	detail.OrderItems = items

	return detail, nil
}

func isValidHHMM(value string) bool {
	if len(value) != 5 || value[2] != ':' {
		return false
	}
	_, err := time.Parse("15:04", value)
	return err == nil
}

func resolvePublicPaymentMethod(orderType string, requested *string) string {
	paymentMethod := ""
	if requested != nil {
		paymentMethod = strings.TrimSpace(strings.ToUpper(*requested))
	}

	if orderType == "DELIVERY" {
		if paymentMethod == "" {
			return "CASH_ON_DELIVERY"
		}
		if paymentMethod == "CASH_ON_DELIVERY" || paymentMethod == "ONLINE" {
			return paymentMethod
		}
		return ""
	}

	if paymentMethod == "" {
		return "CASH_ON_COUNTER"
	}
	if paymentMethod == "CASH_ON_COUNTER" || paymentMethod == "CARD_ON_COUNTER" {
		return paymentMethod
	}
	return ""
}

func deliveryStatusForOrder(orderType string) *string {
	if orderType == "DELIVERY" {
		status := "PENDING_ASSIGNMENT"
		return &status
	}
	return nil
}

func nilIfScheduled(isScheduled bool) *time.Time {
	if isScheduled {
		return nil
	}
	now := time.Now()
	return &now
}
