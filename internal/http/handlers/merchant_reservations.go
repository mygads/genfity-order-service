package handlers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type reservationListCustomer struct {
	ID    int64   `json:"id"`
	Name  string  `json:"name"`
	Email string  `json:"email"`
	Phone *string `json:"phone"`
}

type reservationListOrder struct {
	ID          int64      `json:"id"`
	Status      string     `json:"status"`
	OrderNumber string     `json:"orderNumber"`
	PlacedAt    *time.Time `json:"placedAt"`
}

type merchantReservationAcceptRequest struct {
	TableNumber *string `json:"tableNumber"`
}

type reservationPreorderPayload struct {
	Items []reservationPreorderItem `json:"items"`
}

type reservationPreorderItem struct {
	MenuID   any                        `json:"menuId"`
	Quantity int                        `json:"quantity"`
	Notes    *string                    `json:"notes"`
	Addons   []reservationPreorderAddon `json:"addons"`
}

type reservationPreorderAddon struct {
	AddonItemID any `json:"addonItemId"`
	Quantity    int `json:"quantity"`
}

type reservationMenuRow struct {
	ID         int64
	Name       string
	Price      pgtype.Numeric
	IsActive   bool
	TrackStock bool
	StockQty   pgtype.Int4
}

type reservationAddonRow struct {
	ID         int64
	Name       string
	Price      pgtype.Numeric
	IsActive   bool
	TrackStock bool
	StockQty   pgtype.Int4
}

type reservationMerchantStatus struct {
	isOpen                      bool
	isManualOverride            bool
	timezone                    string
	isPerDayModeScheduleEnabled bool
	isDineInEnabled             bool
	isTakeawayEnabled           bool
	isDeliveryEnabled           bool
	dineInScheduleStart         *string
	dineInScheduleEnd           *string
	takeawayScheduleStart       *string
	takeawayScheduleEnd         *string
	deliveryScheduleStart       *string
	deliveryScheduleEnd         *string
	openingHours                []merchantStatusOpeningHour
	modeSchedules               []merchantStatusModeSchedule
}

type reservationSpecialHour struct {
	Name              *string
	IsClosed          bool
	OpenTime          *string
	CloseTime         *string
	IsDineInEnabled   *bool
	IsTakeawayEnabled *bool
	IsDeliveryEnabled *bool
	DineInStartTime   *string
	DineInEndTime     *string
	TakeawayStartTime *string
	TakeawayEndTime   *string
	DeliveryStartTime *string
	DeliveryEndTime   *string
}

func (h *Handler) MerchantReservationsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			if parsed < 1 {
				parsed = 1
			}
			if parsed > 200 {
				parsed = 200
			}
			limit = parsed
		}
	}

	query := `
		select r.id, r.status, r.party_size, r.reservation_date, r.reservation_time, r.table_number, r.notes,
		       r.preorder, r.accepted_at, r.cancelled_at, r.created_at,
		       c.id, c.name, c.email, c.phone,
		       o.id, o.status, o.order_number, o.placed_at
		from reservations r
		left join customers c on c.id = r.customer_id
		left join orders o on o.id = r.order_id
		where r.merchant_id = $1
		order by r.reservation_date desc, r.reservation_time desc, r.created_at desc
		limit $2
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID, limit)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservations")
		return
	}
	defer rows.Close()

	results := make([]map[string]any, 0)
	for rows.Next() {
		var (
			reservationID   int64
			status          string
			partySize       int32
			reservationDate pgtype.Text
			reservationTime pgtype.Text
			tableNumber     pgtype.Text
			notes           pgtype.Text
			preorder        []byte
			acceptedAt      pgtype.Timestamptz
			cancelledAt     pgtype.Timestamptz
			createdAt       pgtype.Timestamptz
			customerID      pgtype.Int8
			customerName    pgtype.Text
			customerEmail   pgtype.Text
			customerPhone   pgtype.Text
			orderID         pgtype.Int8
			orderStatus     pgtype.Text
			orderNumber     pgtype.Text
			orderPlacedAt   pgtype.Timestamptz
		)

		if err := rows.Scan(
			&reservationID,
			&status,
			&partySize,
			&reservationDate,
			&reservationTime,
			&tableNumber,
			&notes,
			&preorder,
			&acceptedAt,
			&cancelledAt,
			&createdAt,
			&customerID,
			&customerName,
			&customerEmail,
			&customerPhone,
			&orderID,
			&orderStatus,
			&orderNumber,
			&orderPlacedAt,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservations")
			return
		}

		var preorderPayload any
		if len(preorder) > 0 {
			_ = json.Unmarshal(preorder, &preorderPayload)
		}

		row := map[string]any{
			"id":              reservationID,
			"status":          status,
			"displayStatus":   reservationDisplayStatus(status, textOrNil(orderStatus)),
			"partySize":       partySize,
			"reservationDate": nullIfEmptyText(reservationDate),
			"reservationTime": nullIfEmptyText(reservationTime),
			"tableNumber":     nullIfEmptyText(tableNumber),
			"notes":           nullIfEmptyText(notes),
			"preorder":        preorderPayload,
			"acceptedAt": func() any {
				if acceptedAt.Valid {
					return acceptedAt.Time
				}
				return nil
			}(),
			"cancelledAt": func() any {
				if cancelledAt.Valid {
					return cancelledAt.Time
				}
				return nil
			}(),
			"createdAt": func() any {
				if createdAt.Valid {
					return createdAt.Time
				}
				return nil
			}(),
		}

		if customerID.Valid {
			customer := reservationListCustomer{
				ID:    customerID.Int64,
				Name:  customerName.String,
				Email: customerEmail.String,
			}
			if customerPhone.Valid {
				customer.Phone = &customerPhone.String
			}
			row["customer"] = customer
		} else {
			row["customer"] = nil
		}

		if orderID.Valid {
			order := reservationListOrder{
				ID:          orderID.Int64,
				Status:      orderStatus.String,
				OrderNumber: orderNumber.String,
			}
			if orderPlacedAt.Valid {
				order.PlacedAt = &orderPlacedAt.Time
			}
			row["order"] = order
		} else {
			row["order"] = nil
		}

		results = append(results, row)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    results,
		"count":   len(results),
	})
}

func (h *Handler) MerchantReservationsActive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	query := `
		select r.id, r.status, r.party_size, r.reservation_date, r.reservation_time, r.table_number, r.notes,
		       r.preorder, r.accepted_at, r.cancelled_at, r.created_at,
		       c.id, c.name, c.email, c.phone
		from reservations r
		left join customers c on c.id = r.customer_id
		where r.merchant_id = $1 and r.status = 'PENDING'
		order by r.reservation_date asc, r.reservation_time asc, r.created_at asc
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservations")
		return
	}
	defer rows.Close()

	results := make([]map[string]any, 0)
	for rows.Next() {
		var (
			reservationID   int64
			status          string
			partySize       int32
			reservationDate pgtype.Text
			reservationTime pgtype.Text
			tableNumber     pgtype.Text
			notes           pgtype.Text
			preorder        []byte
			acceptedAt      pgtype.Timestamptz
			cancelledAt     pgtype.Timestamptz
			createdAt       pgtype.Timestamptz
			customerID      pgtype.Int8
			customerName    pgtype.Text
			customerEmail   pgtype.Text
			customerPhone   pgtype.Text
		)

		if err := rows.Scan(
			&reservationID,
			&status,
			&partySize,
			&reservationDate,
			&reservationTime,
			&tableNumber,
			&notes,
			&preorder,
			&acceptedAt,
			&cancelledAt,
			&createdAt,
			&customerID,
			&customerName,
			&customerEmail,
			&customerPhone,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservations")
			return
		}

		var preorderPayload any
		if len(preorder) > 0 {
			_ = json.Unmarshal(preorder, &preorderPayload)
		}

		row := map[string]any{
			"id":              reservationID,
			"status":          status,
			"displayStatus":   reservationDisplayStatus(status, nil),
			"partySize":       partySize,
			"reservationDate": nullIfEmptyText(reservationDate),
			"reservationTime": nullIfEmptyText(reservationTime),
			"tableNumber":     nullIfEmptyText(tableNumber),
			"notes":           nullIfEmptyText(notes),
			"preorder":        preorderPayload,
			"acceptedAt": func() any {
				if acceptedAt.Valid {
					return acceptedAt.Time
				}
				return nil
			}(),
			"cancelledAt": func() any {
				if cancelledAt.Valid {
					return cancelledAt.Time
				}
				return nil
			}(),
			"createdAt": func() any {
				if createdAt.Valid {
					return createdAt.Time
				}
				return nil
			}(),
		}

		if customerID.Valid {
			customer := reservationListCustomer{
				ID:    customerID.Int64,
				Name:  customerName.String,
				Email: customerEmail.String,
			}
			if customerPhone.Valid {
				customer.Phone = &customerPhone.String
			}
			row["customer"] = customer
		} else {
			row["customer"] = nil
		}

		results = append(results, row)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    results,
		"count":   len(results),
	})
}

func (h *Handler) MerchantReservationPreorder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	reservationID, err := readPathInt64(r, "reservationId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation ID is required")
		return
	}

	var (
		status          string
		reservationDate pgtype.Text
		reservationTime pgtype.Text
		notes           pgtype.Text
		preorder        []byte
		customerName    pgtype.Text
		customerEmail   pgtype.Text
		customerPhone   pgtype.Text
		orderID         pgtype.Int8
		orderStatus     pgtype.Text
		orderNumber     pgtype.Text
	)

	err = h.DB.QueryRow(ctx, `
		select r.status, r.reservation_date, r.reservation_time, r.preorder, r.notes,
		       c.name, c.email, c.phone,
		       o.id, o.status, o.order_number
		from reservations r
		left join customers c on c.id = r.customer_id
		left join orders o on o.id = r.order_id
		where r.id = $1 and r.merchant_id = $2
		limit 1
	`, reservationID, *authCtx.MerchantID).Scan(
		&status,
		&reservationDate,
		&reservationTime,
		&preorder,
		&notes,
		&customerName,
		&customerEmail,
		&customerPhone,
		&orderID,
		&orderStatus,
		&orderNumber,
	)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Reservation not found")
		return
	}

	var preorderPayload reservationPreorderPayload
	if len(preorder) > 0 {
		_ = json.Unmarshal(preorder, &preorderPayload)
	}

	resolvedItems := h.resolveReservationPreorder(ctx, *authCtx.MerchantID, preorderPayload.Items)

	payload := map[string]any{
		"reservation": map[string]any{
			"id":              reservationID,
			"status":          status,
			"reservationDate": nullIfEmptyText(reservationDate),
			"reservationTime": nullIfEmptyText(reservationTime),
			"notes":           nullIfEmptyText(notes),
			"customer": func() any {
				if customerName.Valid || customerEmail.Valid || customerPhone.Valid {
					return map[string]any{
						"name":  nullIfEmptyText(customerName),
						"email": nullIfEmptyText(customerEmail),
						"phone": nullIfEmptyText(customerPhone),
					}
				}
				return nil
			}(),
			"order": func() any {
				if orderID.Valid {
					return map[string]any{
						"id":          orderID.Int64,
						"status":      orderStatus.String,
						"orderNumber": orderNumber.String,
					}
				}
				return nil
			}(),
		},
		"preorder": map[string]any{
			"items": resolvedItems,
		},
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
	})
}

func (h *Handler) MerchantReservationCancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	reservationID, err := readPathInt64(r, "reservationId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation ID is required")
		return
	}

	var status string
	if err := h.DB.QueryRow(ctx, `
		select status from reservations where id = $1 and merchant_id = $2
	`, reservationID, *authCtx.MerchantID).Scan(&status); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Reservation not found")
		return
	}

	if status != "PENDING" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Only pending reservations can be cancelled")
		return
	}

	var updatedID int64
	if err := h.DB.QueryRow(ctx, `
		update reservations set status = 'CANCELLED', cancelled_at = $1 where id = $2 returning id
	`, time.Now(), reservationID).Scan(&updatedID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to cancel reservation")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    map[string]any{"id": updatedID},
		"message": "Reservation cancelled",
	})
}

func (h *Handler) MerchantReservationAccept(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	reservationID, err := readPathInt64(r, "reservationId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation ID is required")
		return
	}

	var body merchantReservationAcceptRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	requestedTable, err := safeString(body.TableNumber, 50)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		reservationStatus   string
		reservationDate     pgtype.Text
		reservationTime     pgtype.Text
		reservationNotes    pgtype.Text
		reservationPreorder []byte
		reservationCustomer pgtype.Int8
		merchantCode        pgtype.Text
		merchantTimezone    pgtype.Text
		enableTax           bool
		taxPercentage       pgtype.Numeric
		enableServiceCharge bool
		serviceChargePct    pgtype.Numeric
		enablePackagingFee  bool
		packagingFeeAmount  pgtype.Numeric
		requireTableNumber  bool
		isOpen              bool
		isManualOverride    bool
		isPerDayModeEnabled bool
		isDineInEnabled     bool
		isTakeawayEnabled   bool
		isDeliveryEnabled   bool
		dineInStart         pgtype.Text
		dineInEnd           pgtype.Text
		takeawayStart       pgtype.Text
		takeawayEnd         pgtype.Text
		deliveryStart       pgtype.Text
		deliveryEnd         pgtype.Text
	)

	err = tx.QueryRow(ctx, `
		select r.status, r.reservation_date, r.reservation_time, r.notes, r.preorder, r.customer_id,
		       m.code, m.timezone,
		       m.enable_tax, m.tax_percentage,
		       m.enable_service_charge, m.service_charge_percent,
		       m.enable_packaging_fee, m.packaging_fee_amount,
		       m.require_table_number_for_dine_in,
		       m.is_open, m.is_manual_override,
		       m.is_per_day_mode_schedule_enabled,
		       m.is_dine_in_enabled, m.is_takeaway_enabled, m.is_delivery_enabled,
		       m.dine_in_schedule_start, m.dine_in_schedule_end,
		       m.takeaway_schedule_start, m.takeaway_schedule_end,
		       m.delivery_schedule_start, m.delivery_schedule_end
		from reservations r
		join merchants m on m.id = r.merchant_id
		where r.id = $1 and r.merchant_id = $2
		limit 1
	`, reservationID, *authCtx.MerchantID).Scan(
		&reservationStatus,
		&reservationDate,
		&reservationTime,
		&reservationNotes,
		&reservationPreorder,
		&reservationCustomer,
		&merchantCode,
		&merchantTimezone,
		&enableTax,
		&taxPercentage,
		&enableServiceCharge,
		&serviceChargePct,
		&enablePackagingFee,
		&packagingFeeAmount,
		&requireTableNumber,
		&isOpen,
		&isManualOverride,
		&isPerDayModeEnabled,
		&isDineInEnabled,
		&isTakeawayEnabled,
		&isDeliveryEnabled,
		&dineInStart,
		&dineInEnd,
		&takeawayStart,
		&takeawayEnd,
		&deliveryStart,
		&deliveryEnd,
	)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation not found")
		return
	}

	if reservationStatus != "PENDING" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation is not pending")
		return
	}

	if requireTableNumber && (requestedTable == nil || strings.TrimSpace(*requestedTable) == "") {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "tableNumber is required")
		return
	}

	timezone := "Australia/Sydney"
	if merchantTimezone.Valid && strings.TrimSpace(merchantTimezone.String) != "" {
		timezone = merchantTimezone.String
	}

	today := utils.CurrentDateInTimezone(timezone)
	nowTime := utils.CurrentTimeInTimezone(timezone)

	if reservationDate.Valid {
		if reservationDate.String < today || (reservationDate.String == today && reservationTime.String < nowTime) {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation time is in the past")
			return
		}
	}

	openingHours, modeSchedules, specialHour, err := h.fetchReservationSchedules(ctx, tx, *authCtx.MerchantID, reservationDate.String)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant not found")
		return
	}

	merchantStatus := reservationMerchantStatus{
		isOpen:                      isOpen,
		isManualOverride:            isManualOverride,
		timezone:                    timezone,
		isPerDayModeScheduleEnabled: isPerDayModeEnabled,
		isDineInEnabled:             isDineInEnabled,
		isTakeawayEnabled:           isTakeawayEnabled,
		isDeliveryEnabled:           isDeliveryEnabled,
		openingHours:                openingHours,
		modeSchedules:               modeSchedules,
		dineInScheduleStart:         textPtr(dineInStart),
		dineInScheduleEnd:           textPtr(dineInEnd),
		takeawayScheduleStart:       textPtr(takeawayStart),
		takeawayScheduleEnd:         textPtr(takeawayEnd),
		deliveryScheduleStart:       textPtr(deliveryStart),
		deliveryScheduleEnd:         textPtr(deliveryEnd),
	}

	storeRes := isStoreOpenForDateTime(merchantStatus, reservationDate.String, reservationTime.String, specialHour)
	if !storeRes.isOpen {
		message := storeRes.reason
		if message == "" {
			message = "Store is closed at the reservation time"
		}
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", message)
		return
	}

	modeRes := isModeAvailableForDateTime("DINE_IN", merchantStatus, reservationDate.String, reservationTime.String, specialHour)
	if !modeRes.available {
		message := modeRes.reason
		if message == "" {
			message = "Dine-in is not available at the reservation time"
		}
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", message)
		return
	}

	if !isValidHHMM(reservationTime.String) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid reservation time")
		return
	}

	orderNumber, err := generateReservationOrderNumber(ctx, tx, *authCtx.MerchantID, strings.ToUpper(textOrDefault(merchantCode, "ORD")))
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
		return
	}

	now := time.Now()

	var preorderPayload reservationPreorderPayload
	if len(reservationPreorder) > 0 {
		_ = json.Unmarshal(reservationPreorder, &preorderPayload)
	}

	if len(preorderPayload.Items) == 0 {
		var orderID int64
		if err := tx.QueryRow(ctx, `
			insert into orders (
				merchant_id, customer_id, order_number, order_type, table_number, status,
				subtotal, tax_amount, service_charge_amount, packaging_fee, total_amount,
				notes, is_scheduled, scheduled_date, scheduled_time, stock_deducted_at
			) values ($1,$2,$3,'DINE_IN',$4,'ACCEPTED',$5,$6,$7,$8,$9,$10,false,null,null,null)
			returning id
		`, *authCtx.MerchantID, reservationCustomer, orderNumber, nullIfEmptyPtr(requestedTable), 0, 0, 0, 0, 0, nullIfEmptyPtr(textPtr(reservationNotes))).Scan(&orderID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
			return
		}

		if _, err := tx.Exec(ctx, `
			update reservations set status = 'ACCEPTED', accepted_at = $1, table_number = $2, order_id = $3 where id = $4
		`, now, nullIfEmptyPtr(requestedTable), orderID, reservationID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
			return
		}

		if err := tx.Commit(ctx); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"reservation": map[string]any{
					"id":     reservationID,
					"status": "ACCEPTED",
				},
				"order": map[string]any{
					"id":          orderID,
					"orderNumber": orderNumber,
					"status":      "ACCEPTED",
				},
			},
			"message": "Reservation accepted",
		})
		return
	}

	orderID, err := h.acceptReservationWithPreorder(ctx, tx, reservationID, *authCtx.MerchantID, reservationCustomer, orderNumber, reservationNotes, requestedTable, preorderPayload.Items, enableTax, taxPercentage, enableServiceCharge, serviceChargePct)
	if err != nil {
		if strings.HasPrefix(err.Error(), "VALIDATION_ERROR:") {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", strings.TrimPrefix(err.Error(), "VALIDATION_ERROR:"))
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
		return
	}

	if _, err := tx.Exec(ctx, `
		update reservations set status = 'ACCEPTED', accepted_at = $1, table_number = $2, order_id = $3 where id = $4
	`, now, nullIfEmptyPtr(requestedTable), orderID, reservationID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to accept reservation")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"reservation": map[string]any{
				"id":     reservationID,
				"status": "ACCEPTED",
			},
			"order": map[string]any{
				"id":          orderID,
				"orderNumber": orderNumber,
				"status":      "ACCEPTED",
			},
		},
		"message": "Reservation accepted",
	})
}

func safeString(value *string, maxLen int) (*string, error) {
	if value == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil, nil
	}
	if maxLen > 0 && len(trimmed) > maxLen {
		return nil, fmt.Errorf("Value is too long (max %d chars)", maxLen)
	}
	return &trimmed, nil
}

func reservationDisplayStatus(status string, orderStatus *string) string {
	status = strings.ToUpper(strings.TrimSpace(status))
	if status == "CANCELLED" {
		return "CANCELLED"
	}
	if status == "PENDING" {
		return "PENDING"
	}
	if orderStatus == nil {
		return "ACCEPTED"
	}
	os := strings.ToUpper(strings.TrimSpace(*orderStatus))
	if os == "COMPLETED" {
		return "COMPLETED"
	}
	if os == "IN_PROGRESS" || os == "READY" {
		return "IN_PROGRESS"
	}
	if os == "CANCELLED" {
		return "CANCELLED"
	}
	return "ACCEPTED"
}

func textOrNil(value pgtype.Text) *string {
	if value.Valid {
		return &value.String
	}
	return nil
}

func (h *Handler) fetchReservationSchedules(ctx context.Context, tx pgx.Tx, merchantID int64, dateISO string) ([]merchantStatusOpeningHour, []merchantStatusModeSchedule, *reservationSpecialHour, error) {
	openingHours := make([]merchantStatusOpeningHour, 0)
	rows, err := tx.Query(ctx, `
		select id, day_of_week, is_closed, open_time, close_time
		from merchant_opening_hours
		where merchant_id = $1
		order by day_of_week asc
	`, merchantID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var hrow merchantStatusOpeningHour
			var openTime pgtype.Text
			var closeTime pgtype.Text
			if err := rows.Scan(&hrow.ID, &hrow.DayOfWeek, &hrow.IsClosed, &openTime, &closeTime); err == nil {
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
	modeRows, err := tx.Query(ctx, `
		select id, mode, day_of_week, start_time, end_time, is_active
		from merchant_mode_schedules
		where merchant_id = $1
		order by mode asc, day_of_week asc
	`, merchantID)
	if err == nil {
		defer modeRows.Close()
		for modeRows.Next() {
			var m merchantStatusModeSchedule
			if err := modeRows.Scan(&m.ID, &m.Mode, &m.DayOfWeek, &m.StartTime, &m.EndTime, &m.IsActive); err == nil {
				modeSchedules = append(modeSchedules, m)
			}
		}
	}

	var special *reservationSpecialHour
	if dateISO != "" {
		if dateValue, err := time.Parse("2006-01-02", dateISO); err == nil {
			var row reservationSpecialHour
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

			if err := tx.QueryRow(ctx, `
				select name, is_closed, open_time, close_time,
				       is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled,
				       dine_in_start_time, dine_in_end_time,
				       takeaway_start_time, takeaway_end_time,
				       delivery_start_time, delivery_end_time
				from merchant_special_hours
				where merchant_id = $1 and date = $2
				limit 1
			`, merchantID, dateValue).Scan(
				&name,
				&row.IsClosed,
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
					row.Name = &name.String
				}
				if openTime.Valid {
					row.OpenTime = &openTime.String
				}
				if closeTime.Valid {
					row.CloseTime = &closeTime.String
				}
				if isDineIn.Valid {
					v := isDineIn.Bool
					row.IsDineInEnabled = &v
				}
				if isTakeaway.Valid {
					v := isTakeaway.Bool
					row.IsTakeawayEnabled = &v
				}
				if isDelivery.Valid {
					v := isDelivery.Bool
					row.IsDeliveryEnabled = &v
				}
				if dineInStart.Valid {
					row.DineInStartTime = &dineInStart.String
				}
				if dineInEnd.Valid {
					row.DineInEndTime = &dineInEnd.String
				}
				if takeawayStart.Valid {
					row.TakeawayStartTime = &takeawayStart.String
				}
				if takeawayEnd.Valid {
					row.TakeawayEndTime = &takeawayEnd.String
				}
				if deliveryStart.Valid {
					row.DeliveryStartTime = &deliveryStart.String
				}
				if deliveryEnd.Valid {
					row.DeliveryEndTime = &deliveryEnd.String
				}
				special = &row
			}
		}
	}

	return openingHours, modeSchedules, special, nil
}

func isStoreOpenForDateTime(merchant reservationMerchantStatus, dateISO, timeHHMM string, special *reservationSpecialHour) storeStatusResult {
	if merchant.isManualOverride {
		isOpen := merchant.isOpen
		reason := "Manually Open"
		if !isOpen {
			reason = "Manually Closed"
		}
		return storeStatusResult{isOpen: isOpen, reason: reason}
	}

	dayOfWeek := dayOfWeekFromISODate(dateISO)

	if special != nil {
		if special.IsClosed {
			reason := "Closed Today"
			if special.Name != nil && strings.TrimSpace(*special.Name) != "" {
				reason = "Closed for " + strings.TrimSpace(*special.Name)
			}
			return storeStatusResult{isOpen: false, reason: reason}
		}
		if special.OpenTime != nil && special.CloseTime != nil {
			isWithin := timeHHMM >= *special.OpenTime && timeHHMM <= *special.CloseTime
			reason := "Currently Closed"
			if isWithin {
				reason = ""
			}
			return storeStatusResult{isOpen: isWithin, reason: reason}
		}
	}

	if len(merchant.openingHours) > 0 {
		var today *merchantStatusOpeningHour
		for i := range merchant.openingHours {
			if merchant.openingHours[i].DayOfWeek == dayOfWeek {
				today = &merchant.openingHours[i]
				break
			}
		}
		if today == nil {
			return storeStatusResult{isOpen: false, reason: "Closed Today"}
		}
		if today.IsClosed {
			return storeStatusResult{isOpen: false, reason: "Closed Today"}
		}
		if today.OpenTime != nil && today.CloseTime != nil {
			isWithin := timeHHMM >= *today.OpenTime && timeHHMM <= *today.CloseTime
			reason := "Currently Closed"
			if isWithin {
				reason = ""
			}
			return storeStatusResult{isOpen: isWithin, reason: reason}
		}
	}

	return storeStatusResult{isOpen: true}
}

func isModeAvailableForDateTime(modeType string, merchant reservationMerchantStatus, dateISO, timeHHMM string, special *reservationSpecialHour) modeAvailabilityResult {
	dayOfWeek := dayOfWeekFromISODate(dateISO)

	if merchant.isManualOverride {
		if !merchant.isOpen {
			return modeAvailabilityResult{available: false, reason: "Store is manually closed"}
		}
	}

	isEnabled := false
	switch modeType {
	case "DINE_IN":
		isEnabled = merchant.isDineInEnabled
	case "TAKEAWAY":
		isEnabled = merchant.isTakeawayEnabled
	case "DELIVERY":
		isEnabled = merchant.isDeliveryEnabled
	}
	if !isEnabled {
		label := modeLabel(modeType)
		return modeAvailabilityResult{available: false, reason: label + " is not available"}
	}

	if merchant.isManualOverride && merchant.isOpen {
		return modeAvailabilityResult{available: true}
	}

	if special != nil {
		var modeEnabled *bool
		var start *string
		var end *string
		switch modeType {
		case "DINE_IN":
			modeEnabled = special.IsDineInEnabled
			start = special.DineInStartTime
			end = special.DineInEndTime
		case "TAKEAWAY":
			modeEnabled = special.IsTakeawayEnabled
			start = special.TakeawayStartTime
			end = special.TakeawayEndTime
		case "DELIVERY":
			modeEnabled = special.IsDeliveryEnabled
			start = special.DeliveryStartTime
			end = special.DeliveryEndTime
		}
		if modeEnabled != nil && !*modeEnabled {
			label := modeLabel(modeType)
			return modeAvailabilityResult{available: false, reason: label + " not available today"}
		}
		if start != nil && end != nil {
			isWithin := timeHHMM >= *start && timeHHMM <= *end
			reason := "Available " + *start + " - " + *end
			if isWithin {
				reason = ""
			}
			return modeAvailabilityResult{available: isWithin, reason: reason}
		}
	}

	if merchant.isPerDayModeScheduleEnabled && len(merchant.modeSchedules) > 0 {
		for _, schedule := range merchant.modeSchedules {
			if schedule.Mode == modeType && schedule.DayOfWeek == dayOfWeek {
				if !schedule.IsActive {
					label := modeLabel(modeType)
					return modeAvailabilityResult{available: false, reason: label + " not available today"}
				}
				isWithin := timeHHMM >= schedule.StartTime && timeHHMM <= schedule.EndTime
				reason := "Available " + schedule.StartTime + " - " + schedule.EndTime
				if isWithin {
					reason = ""
				}
				return modeAvailabilityResult{available: isWithin, reason: reason}
			}
		}
	}

	var globalStart *string
	var globalEnd *string
	switch modeType {
	case "DINE_IN":
		globalStart = merchant.dineInScheduleStart
		globalEnd = merchant.dineInScheduleEnd
	case "TAKEAWAY":
		globalStart = merchant.takeawayScheduleStart
		globalEnd = merchant.takeawayScheduleEnd
	case "DELIVERY":
		globalStart = merchant.deliveryScheduleStart
		globalEnd = merchant.deliveryScheduleEnd
	}
	if globalStart != nil && globalEnd != nil {
		isWithin := timeHHMM >= *globalStart && timeHHMM <= *globalEnd
		reason := "Available " + *globalStart + " - " + *globalEnd
		if isWithin {
			reason = ""
		}
		return modeAvailabilityResult{available: isWithin, reason: reason}
	}

	return modeAvailabilityResult{available: true}
}

type storeStatusResult struct {
	isOpen bool
	reason string
}

type modeAvailabilityResult struct {
	available bool
	reason    string
}

func modeLabel(modeType string) string {
	switch modeType {
	case "DINE_IN":
		return "Dine In"
	case "TAKEAWAY":
		return "Takeaway"
	case "DELIVERY":
		return "Delivery"
	default:
		return "Mode"
	}
}

func dayOfWeekFromISODate(dateISO string) int {
	if dateISO == "" {
		return int(time.Now().Weekday())
	}
	parsed, err := time.Parse("2006-01-02", dateISO)
	if err != nil {
		return int(time.Now().Weekday())
	}
	return int(parsed.Weekday())
}

func generateReservationOrderNumber(ctx context.Context, tx pgx.Tx, merchantID int64, merchantCode string) (string, error) {
	const maxRetries = 10
	if merchantCode == "" {
		merchantCode = "ORD"
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		candidate := merchantCode + "-" + randomSuffix(4)
		var exists bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from orders where merchant_id = $1 and order_number = $2)`, merchantID, candidate).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	base36 := strings.ToUpper(strconv.FormatInt(time.Now().UnixMilli(), 36))
	if len(base36) > 4 {
		base36 = base36[len(base36)-4:]
	}
	return merchantCode + "-" + base36, nil
}

func randomSuffix(length int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if length <= 0 {
		return ""
	}
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			result[i] = chars[0]
			continue
		}
		result[i] = chars[n.Int64()]
	}
	return string(result)
}

func (h *Handler) resolveReservationPreorder(ctx context.Context, merchantID int64, items []reservationPreorderItem) []map[string]any {
	menuIDs := make([]int64, 0)
	addonIDs := make([]int64, 0)

	for _, item := range items {
		if id, ok := parseNumericID(item.MenuID); ok {
			menuIDs = append(menuIDs, id)
		}
		for _, addon := range item.Addons {
			if id, ok := parseNumericID(addon.AddonItemID); ok {
				addonIDs = append(addonIDs, id)
			}
		}
	}

	menuMap := make(map[int64]reservationMenuRow)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select id, name, price, is_active, track_stock, stock_qty
			from menus
			where id = any($1) and merchant_id = $2
		`, menuIDs, merchantID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var row reservationMenuRow
				if err := rows.Scan(&row.ID, &row.Name, &row.Price, &row.IsActive, &row.TrackStock, &row.StockQty); err == nil {
					menuMap[row.ID] = row
				}
			}
		}
	}

	addonMap := make(map[int64]reservationAddonRow)
	if len(addonIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select ai.id, ai.name, ai.price, ai.is_active, ai.track_stock, ai.stock_qty
			from addon_items ai
			join addon_categories ac on ac.id = ai.addon_category_id
			where ai.id = any($1) and ac.merchant_id = $2
		`, addonIDs, merchantID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var row reservationAddonRow
				if err := rows.Scan(&row.ID, &row.Name, &row.Price, &row.IsActive, &row.TrackStock, &row.StockQty); err == nil {
					addonMap[row.ID] = row
				}
			}
		}
	}

	resolved := make([]map[string]any, 0, len(items))
	for _, item := range items {
		menuID, menuOK := parseNumericID(item.MenuID)
		menuRow, menuFound := menuMap[menuID]
		menuAvailable := menuOK && menuFound && menuRow.IsActive
		menuName := any(nil)
		unitPrice := 0.0
		if menuFound {
			menuName = menuRow.Name
			unitPrice = utils.NumericToFloat64(menuRow.Price)
		}
		quantity := item.Quantity
		if quantity <= 0 {
			quantity = 1
		}

		addonPayloads := make([]map[string]any, 0)
		for _, addon := range item.Addons {
			addonID, addonOK := parseNumericID(addon.AddonItemID)
			addonRow, addonFound := addonMap[addonID]
			addonAvailable := addonOK && addonFound && addonRow.IsActive
			addonName := any(nil)
			addonPrice := 0.0
			if addonFound {
				addonName = addonRow.Name
				addonPrice = utils.NumericToFloat64(addonRow.Price)
			}
			addonQty := addon.Quantity
			if addonQty <= 0 {
				addonQty = 1
			}
			addonPayloads = append(addonPayloads, map[string]any{
				"addonItemId": func() any {
					if addonOK {
						return strconv.FormatInt(addonID, 10)
					}
					return nil
				}(),
				"addonName":   addonName,
				"quantity":    addonQty,
				"unitPrice":   addonPrice,
				"isAvailable": addonAvailable,
			})
		}

		notesValue := any(nil)
		if item.Notes != nil {
			notesValue = strings.TrimSpace(*item.Notes)
		}

		resolved = append(resolved, map[string]any{
			"menuId": func() any {
				if menuOK {
					return strconv.FormatInt(menuID, 10)
				}
				return nil
			}(),
			"menuName":    menuName,
			"quantity":    quantity,
			"notes":       notesValue,
			"unitPrice":   unitPrice,
			"isAvailable": menuAvailable,
			"addons":      addonPayloads,
		})
	}

	return resolved
}

func (h *Handler) acceptReservationWithPreorder(ctx context.Context, tx pgx.Tx, reservationID, merchantID int64, customerID pgtype.Int8, orderNumber string, notes pgtype.Text, tableNumber *string, items []reservationPreorderItem, enableTax bool, taxPercentage pgtype.Numeric, enableServiceCharge bool, serviceChargePct pgtype.Numeric) (int64, error) {
	if !customerID.Valid {
		return 0, fmt.Errorf("VALIDATION_ERROR:Customer not found")
	}

	type normalizedAddon struct {
		ID       int64
		Quantity int
	}

	type normalizedItem struct {
		MenuID   int64
		Quantity int
		Notes    *string
		Addons   []normalizedAddon
	}

	normalized := make([]normalizedItem, 0)
	for _, item := range items {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			return 0, fmt.Errorf("VALIDATION_ERROR:Some menu items are not available")
		}
		qty := item.Quantity
		if qty <= 0 {
			qty = 1
		}
		addons := make([]normalizedAddon, 0)
		for _, addon := range item.Addons {
			addonID, ok := parseNumericID(addon.AddonItemID)
			if !ok {
				return 0, fmt.Errorf("VALIDATION_ERROR:Some add-ons are not available")
			}
			addonQty := addon.Quantity
			if addonQty <= 0 {
				addonQty = 1
			}
			addons = append(addons, normalizedAddon{ID: addonID, Quantity: addonQty})
		}
		normalized = append(normalized, normalizedItem{
			MenuID:   menuID,
			Quantity: qty,
			Notes:    item.Notes,
			Addons:   addons,
		})
	}

	if len(normalized) == 0 {
		return 0, nil
	}

	menuIDList := make([]int64, 0)
	addonIDList := make([]int64, 0)
	for _, item := range normalized {
		menuIDList = append(menuIDList, item.MenuID)
		for _, addon := range item.Addons {
			addonIDList = append(addonIDList, addon.ID)
		}
	}

	menus := make(map[int64]reservationMenuRow)
	if len(menuIDList) > 0 {
		rows, err := tx.Query(ctx, `
			select id, name, price, is_active, track_stock, stock_qty
			from menus
			where id = any($1) and merchant_id = $2 and deleted_at is null
		`, menuIDList, merchantID)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		for rows.Next() {
			var row reservationMenuRow
			if err := rows.Scan(&row.ID, &row.Name, &row.Price, &row.IsActive, &row.TrackStock, &row.StockQty); err != nil {
				return 0, err
			}
			menus[row.ID] = row
		}
	}

	addons := make(map[int64]reservationAddonRow)
	if len(addonIDList) > 0 {
		rows, err := tx.Query(ctx, `
			select ai.id, ai.name, ai.price, ai.is_active, ai.track_stock, ai.stock_qty
			from addon_items ai
			join addon_categories ac on ac.id = ai.addon_category_id
			where ai.id = any($1) and ac.merchant_id = $2 and ai.deleted_at is null
		`, addonIDList, merchantID)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		for rows.Next() {
			var row reservationAddonRow
			if err := rows.Scan(&row.ID, &row.Name, &row.Price, &row.IsActive, &row.TrackStock, &row.StockQty); err != nil {
				return 0, err
			}
			addons[row.ID] = row
		}
	}

	menuRequired := make(map[int64]int)
	addonRequired := make(map[int64]int)

	type addonCreate struct {
		AddonItemID int64
		AddonName   string
		AddonPrice  float64
		Quantity    int
		Subtotal    float64
	}

	type itemCreate struct {
		MenuID    int64
		MenuName  string
		MenuPrice float64
		Quantity  int
		Subtotal  float64
		Notes     *string
		Addons    []addonCreate
	}

	itemsData := make([]itemCreate, 0)
	subtotal := 0.0

	for _, item := range normalized {
		menu, ok := menus[item.MenuID]
		if !ok || !menu.IsActive {
			return 0, fmt.Errorf("VALIDATION_ERROR:Some menu items are not available")
		}
		if menu.TrackStock && menu.StockQty.Valid {
			menuRequired[menu.ID] = menuRequired[menu.ID] + item.Quantity
		}

		menuPrice := round2(utils.NumericToFloat64(menu.Price))
		itemTotal := round2(menuPrice * float64(item.Quantity))

		addonData := make([]addonCreate, 0)
		for _, addon := range item.Addons {
			addonRow, ok := addons[addon.ID]
			if !ok || !addonRow.IsActive {
				return 0, fmt.Errorf("VALIDATION_ERROR:Some add-ons are not available")
			}
			if addonRow.TrackStock && addonRow.StockQty.Valid {
				addonRequired[addonRow.ID] = addonRequired[addonRow.ID] + addon.Quantity
			}
			addonPrice := round2(utils.NumericToFloat64(addonRow.Price))
			addonSubtotal := round2(addonPrice * float64(addon.Quantity))
			itemTotal = round2(itemTotal + addonSubtotal)
			addonData = append(addonData, addonCreate{
				AddonItemID: addonRow.ID,
				AddonName:   addonRow.Name,
				AddonPrice:  addonPrice,
				Quantity:    addon.Quantity,
				Subtotal:    addonSubtotal,
			})
		}
		subtotal = round2(subtotal + itemTotal)
		itemsData = append(itemsData, itemCreate{
			MenuID:    menu.ID,
			MenuName:  menu.Name,
			MenuPrice: menuPrice,
			Quantity:  item.Quantity,
			Subtotal:  itemTotal,
			Notes:     item.Notes,
			Addons:    addonData,
		})
	}

	for id, qty := range menuRequired {
		menu := menus[id]
		if menu.StockQty.Valid && int(menu.StockQty.Int32) < qty {
			return 0, fmt.Errorf("VALIDATION_ERROR:Insufficient stock for %s", menu.Name)
		}
		res, err := tx.Exec(ctx, `
			update menus
			set stock_qty = stock_qty - $1
			where id = $2 and track_stock = true and stock_qty >= $1
		`, qty, id)
		if err != nil {
			return 0, err
		}
		if res.RowsAffected() != 1 {
			return 0, fmt.Errorf("VALIDATION_ERROR:Insufficient stock for %s", menu.Name)
		}
	}

	for id, qty := range addonRequired {
		addon := addons[id]
		if addon.StockQty.Valid && int(addon.StockQty.Int32) < qty {
			return 0, fmt.Errorf("VALIDATION_ERROR:Insufficient stock for %s", addon.Name)
		}
		res, err := tx.Exec(ctx, `
			update addon_items
			set stock_qty = stock_qty - $1
			where id = $2 and track_stock = true and stock_qty >= $1
		`, qty, id)
		if err != nil {
			return 0, err
		}
		if res.RowsAffected() != 1 {
			return 0, fmt.Errorf("VALIDATION_ERROR:Insufficient stock for %s", addon.Name)
		}
	}

	for id := range menuRequired {
		var stockQty pgtype.Int4
		if err := tx.QueryRow(ctx, `select stock_qty from menus where id = $1`, id).Scan(&stockQty); err == nil {
			if stockQty.Valid {
				_, _ = tx.Exec(ctx, `update menus set is_active = $1 where id = $2`, stockQty.Int32 > 0, id)
			}
		}
	}
	for id := range addonRequired {
		var stockQty pgtype.Int4
		if err := tx.QueryRow(ctx, `select stock_qty from addon_items where id = $1`, id).Scan(&stockQty); err == nil {
			if stockQty.Valid {
				_, _ = tx.Exec(ctx, `update addon_items set is_active = $1 where id = $2`, stockQty.Int32 > 0, id)
			}
		}
	}

	taxAmount := 0.0
	if enableTax && taxPercentage.Valid {
		taxAmount = round2(subtotal * (utils.NumericToFloat64(taxPercentage) / 100))
	}
	serviceChargeAmount := 0.0
	if enableServiceCharge && serviceChargePct.Valid {
		serviceChargeAmount = round2(subtotal * (utils.NumericToFloat64(serviceChargePct) / 100))
	}
	packagingFeeAmount := 0.0
	totalAmount := round2(subtotal + taxAmount + serviceChargeAmount + packagingFeeAmount)

	var orderID int64
	if err := tx.QueryRow(ctx, `
		insert into orders (
			merchant_id, customer_id, order_number, order_type, table_number, status,
			subtotal, tax_amount, service_charge_amount, packaging_fee, total_amount,
			notes, is_scheduled, scheduled_date, scheduled_time, stock_deducted_at
		) values ($1,$2,$3,'DINE_IN',$4,'ACCEPTED',$5,$6,$7,$8,$9,$10,false,null,null,$11)
		returning id
	`, merchantID, customerID.Int64, orderNumber, nullIfEmptyPtr(tableNumber), subtotal, taxAmount, serviceChargeAmount, packagingFeeAmount, totalAmount, nullIfEmptyPtr(textPtr(notes)), time.Now()).Scan(&orderID); err != nil {
		return 0, err
	}

	for _, item := range itemsData {
		var orderItemID int64
		if err := tx.QueryRow(ctx, `
			insert into order_items (order_id, menu_id, menu_name, menu_price, quantity, subtotal, notes)
			values ($1,$2,$3,$4,$5,$6,$7)
			returning id
		`, orderID, item.MenuID, item.MenuName, item.MenuPrice, item.Quantity, item.Subtotal, nullIfEmptyPtr(item.Notes)).Scan(&orderItemID); err != nil {
			return 0, err
		}

		for _, addon := range item.Addons {
			if _, err := tx.Exec(ctx, `
				insert into order_item_addons (order_item_id, addon_item_id, addon_name, addon_price, quantity, subtotal)
				values ($1,$2,$3,$4,$5,$6)
			`, orderItemID, addon.AddonItemID, addon.AddonName, addon.AddonPrice, addon.Quantity, addon.Subtotal); err != nil {
				return 0, err
			}
		}
	}

	if totalAmount > 0 {
		if _, err := tx.Exec(ctx, `
			insert into payments (order_id, amount, payment_method, status)
			values ($1,$2,'CASH_ON_COUNTER','PENDING')
		`, orderID, totalAmount); err != nil {
			return 0, err
		}
	}

	return orderID, nil
}
