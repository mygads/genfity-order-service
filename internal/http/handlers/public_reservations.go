package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type publicReservationRequest struct {
	MerchantCode    string                  `json:"merchantCode"`
	CustomerName    string                  `json:"customerName"`
	CustomerEmail   string                  `json:"customerEmail"`
	CustomerPhone   *string                 `json:"customerPhone"`
	PartySize       int                     `json:"partySize"`
	ReservationDate string                  `json:"reservationDate"`
	ReservationTime string                  `json:"reservationTime"`
	Notes           *string                 `json:"notes"`
	Items           []publicReservationItem `json:"items"`
}

type publicReservationItem struct {
	MenuID   any                      `json:"menuId"`
	Quantity int32                    `json:"quantity"`
	Notes    *string                  `json:"notes"`
	Addons   []publicReservationAddon `json:"addons"`
}

type publicReservationAddon struct {
	AddonItemID any   `json:"addonItemId"`
	Quantity    int32 `json:"quantity"`
}

func (h *Handler) PublicReservationsCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var body publicReservationRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantCode := strings.TrimSpace(body.MerchantCode)
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "merchantCode is required")
		return
	}

	if strings.TrimSpace(body.CustomerName) == "" || strings.TrimSpace(body.CustomerEmail) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Customer name and email are required")
		return
	}

	partySize := body.PartySize
	if partySize <= 0 || partySize > 100 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "partySize must be between 1 and 100")
		return
	}

	reservationDate := strings.TrimSpace(body.ReservationDate)
	if !isValidYYYYMMDD(reservationDate) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "reservationDate must be YYYY-MM-DD")
		return
	}
	reservationTime := strings.TrimSpace(body.ReservationTime)
	if !isValidHHMM(reservationTime) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "reservationTime must be HH:MM")
		return
	}

	merchant, err := h.loadReservationMerchant(ctx, merchantCode)
	if err != nil || !merchant.IsActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}
	if !merchant.IsReservationEnabled {
		response.Error(w, http.StatusBadRequest, "RESERVATION_DISABLED", "Reservations are not available for this merchant")
		return
	}

	tz := merchant.Timezone
	if tz == "" {
		tz = "Australia/Sydney"
	}
	if isReservationInPast(tz, reservationDate, reservationTime) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Reservation time cannot be in the past")
		return
	}

	preorderItems := body.Items
	minItems := 0
	if merchant.ReservationMenuRequired {
		minItems = merchant.ReservationMinItemCount
		if minItems < 1 {
			minItems = 1
		}
	}
	if minItems > 0 {
		totalQty := 0
		for _, item := range preorderItems {
			qty := int(item.Quantity)
			if qty > 0 {
				totalQty += qty
			}
		}
		if totalQty < minItems {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Preorder is required (minimum "+strconv.Itoa(minItems)+" item(s))")
			return
		}
	}

	customerID, err := h.findOrCreateReservationCustomer(ctx, body.CustomerName, body.CustomerEmail, body.CustomerPhone)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "CUSTOMER_ERROR", err.Error())
		return
	}

	var preorder any
	if len(preorderItems) > 0 {
		preorder = buildReservationPreorder(preorderItems)
	}

	notes := strings.TrimSpace(defaultStringPtr(body.Notes))
	if len(notes) > 2000 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Notes is too long")
		return
	}
	var notesPtr *string
	if notes != "" {
		notesPtr = &notes
	}

	reservationID, err := h.createReservation(ctx, merchant.ID, customerID, partySize, reservationDate, reservationTime, notesPtr, preorder)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create reservation")
		return
	}

	data, err := h.fetchReservationDetail(ctx, reservationID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve reservation")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data":    data,
		"message": "Reservation created successfully",
	})
}

type reservationMerchant struct {
	ID                      int64
	Code                    string
	Name                    string
	Timezone                string
	IsActive                bool
	IsReservationEnabled    bool
	ReservationMenuRequired bool
	ReservationMinItemCount int
}

func (h *Handler) loadReservationMerchant(ctx context.Context, code string) (reservationMerchant, error) {
	var m reservationMerchant
	var minItems pgtype.Int4
	if err := h.DB.QueryRow(ctx, `
		select id, code, name, timezone, is_active, is_reservation_enabled, reservation_menu_required, reservation_min_item_count
		from merchants
		where code = $1
	`, code).Scan(
		&m.ID,
		&m.Code,
		&m.Name,
		&m.Timezone,
		&m.IsActive,
		&m.IsReservationEnabled,
		&m.ReservationMenuRequired,
		&minItems,
	); err != nil {
		return reservationMerchant{}, err
	}
	if minItems.Valid {
		m.ReservationMinItemCount = int(minItems.Int32)
	}
	return m, nil
}

func isReservationInPast(timezone, dateStr, timeStr string) bool {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	if dateStr < now.Format("2006-01-02") {
		return true
	}
	if dateStr == now.Format("2006-01-02") && timeStr < now.Format("15:04") {
		return true
	}
	return false
}

func isValidYYYYMMDD(value string) bool {
	if value == "" {
		return false
	}
	_, err := time.Parse("2006-01-02", value)
	return err == nil
}

func (h *Handler) findOrCreateReservationCustomer(ctx context.Context, name, email string, phone *string) (*int64, error) {
	trimmedEmail := strings.ToLower(strings.TrimSpace(email))
	if trimmedEmail == "" {
		return nil, errInvalid("Invalid email")
	}
	trimmedName := strings.TrimSpace(name)
	trimmedPhone := ""
	if phone != nil {
		trimmedPhone = strings.TrimSpace(*phone)
	}

	var existingID int64
	if err := h.DB.QueryRow(ctx, `select id from customers where email = $1`, trimmedEmail).Scan(&existingID); err == nil {
		if trimmedName != "" || trimmedPhone != "" {
			_, _ = h.DB.Exec(ctx, `update customers set name = coalesce(nullif($1,''), name), phone = coalesce(nullif($2,''), phone) where id = $3`, trimmedName, nullIfEmpty(trimmedPhone), existingID)
		}
		return &existingID, nil
	}

	if trimmedPhone != "" {
		var emailOwner pgtype.Text
		_ = h.DB.QueryRow(ctx, `select email from customers where phone = $1 and email <> $2 limit 1`, trimmedPhone, trimmedEmail).Scan(&emailOwner)
		if emailOwner.Valid {
			return nil, errInvalid("This phone number is already registered with a different email address.")
		}
	}

	var newID int64
	if err := h.DB.QueryRow(ctx, `
		insert into customers (name, email, phone, is_active) values ($1,$2,$3,true) returning id
	`, defaultString(trimmedName, "Guest Customer"), trimmedEmail, nullIfEmpty(trimmedPhone)).Scan(&newID); err != nil {
		return nil, err
	}
	return &newID, nil
}

func buildReservationPreorder(items []publicReservationItem) map[string]any {
	outputItems := make([]map[string]any, 0)
	for _, item := range items {
		menuID, ok := parseNumericID(item.MenuID)
		if !ok {
			continue
		}
		qty := int(item.Quantity)
		if qty <= 0 {
			qty = 1
		}
		addonItems := make([]map[string]any, 0)
		for _, addon := range item.Addons {
			addonID, ok := parseNumericID(addon.AddonItemID)
			if !ok {
				continue
			}
			addonQty := int(addon.Quantity)
			if addonQty <= 0 {
				addonQty = 1
			}
			addonItems = append(addonItems, map[string]any{
				"addonItemId": strconv.FormatInt(addonID, 10),
				"quantity":    addonQty,
			})
		}
		outputItems = append(outputItems, map[string]any{
			"menuId":   strconv.FormatInt(menuID, 10),
			"quantity": qty,
			"notes":    strings.TrimSpace(defaultStringPtr(item.Notes)),
			"addons":   addonItems,
		})
	}

	return map[string]any{"items": outputItems}
}

func (h *Handler) createReservation(ctx context.Context, merchantID int64, customerID *int64, partySize int, reservationDate, reservationTime string, notes *string, preorder any) (int64, error) {
	var preorderValue any
	if preorder != nil {
		payload, err := json.Marshal(preorder)
		if err != nil {
			return 0, err
		}
		preorderValue = string(payload)
	}

	var reservationID int64
	if err := h.DB.QueryRow(ctx, `
		insert into reservations (merchant_id, customer_id, party_size, reservation_date, reservation_time, notes, preorder, status)
		values ($1,$2,$3,$4,$5,$6,$7::jsonb,'PENDING')
		returning id
	`, merchantID, customerID, partySize, reservationDate, reservationTime, nullIfEmptyPtr(notes), preorderValue).Scan(&reservationID); err != nil {
		return 0, err
	}
	return reservationID, nil
}

func (h *Handler) fetchReservationDetail(ctx context.Context, reservationID int64) (map[string]any, error) {
	query := `
		select r.id, r.status, r.party_size, r.reservation_date, r.reservation_time, r.notes, r.preorder,
		       c.id, c.name, c.email, c.phone,
		       m.id, m.code, m.name, m.timezone
		from reservations r
		join customers c on c.id = r.customer_id
		join merchants m on m.id = r.merchant_id
		where r.id = $1
		limit 1
	`

	var (
		id              int64
		status          string
		partySize       int32
		reservationDate string
		reservationTime string
		notes           pgtype.Text
		preorder        []byte
		customerID      int64
		customerName    string
		customerEmail   string
		customerPhone   pgtype.Text
		merchantID      int64
		merchantCode    string
		merchantName    string
		merchantTZ      pgtype.Text
	)

	if err := h.DB.QueryRow(ctx, query, reservationID).Scan(
		&id,
		&status,
		&partySize,
		&reservationDate,
		&reservationTime,
		&notes,
		&preorder,
		&customerID,
		&customerName,
		&customerEmail,
		&customerPhone,
		&merchantID,
		&merchantCode,
		&merchantName,
		&merchantTZ,
	); err != nil {
		return nil, err
	}

	var preorderPayload any
	if len(preorder) > 0 {
		_ = json.Unmarshal(preorder, &preorderPayload)
	}

	data := map[string]any{
		"id":              id,
		"status":          status,
		"partySize":       partySize,
		"reservationDate": reservationDate,
		"reservationTime": reservationTime,
		"notes":           nullIfEmptyText(notes),
		"preorder":        preorderPayload,
		"customer": map[string]any{
			"id":    customerID,
			"name":  customerName,
			"email": customerEmail,
			"phone": nullIfEmptyText(customerPhone),
		},
		"merchant": map[string]any{
			"id":       merchantID,
			"code":     merchantCode,
			"name":     merchantName,
			"timezone": nullIfEmptyText(merchantTZ),
		},
	}

	return data, nil
}
