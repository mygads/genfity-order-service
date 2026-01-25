package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type GroupOrderParticipant struct {
	ID         int64            `json:"id"`
	CustomerID *int64           `json:"customerId"`
	Name       string           `json:"name"`
	DeviceID   string           `json:"deviceId"`
	IsHost     bool             `json:"isHost"`
	CartItems  json.RawMessage  `json:"cartItems"`
	Subtotal   float64          `json:"subtotal"`
	JoinedAt   time.Time        `json:"joinedAt"`
	UpdatedAt  time.Time        `json:"updatedAt"`
	Customer   *CustomerSummary `json:"customer,omitempty"`
}

type GroupOrderSession struct {
	ID              int64                   `json:"id"`
	SessionCode     string                  `json:"sessionCode"`
	OrderType       string                  `json:"orderType"`
	TableNumber     *string                 `json:"tableNumber"`
	Status          string                  `json:"status"`
	MerchantID      int64                   `json:"merchantId"`
	OrderID         *int64                  `json:"orderId"`
	MaxParticipants int32                   `json:"maxParticipants"`
	ExpiresAt       time.Time               `json:"expiresAt"`
	CreatedAt       time.Time               `json:"createdAt"`
	UpdatedAt       time.Time               `json:"updatedAt"`
	Participants    []GroupOrderParticipant `json:"participants"`
	Merchant        struct {
		ID                   int64    `json:"id"`
		Code                 string   `json:"code"`
		Name                 string   `json:"name"`
		Currency             string   `json:"currency"`
		EnableTax            bool     `json:"enableTax"`
		TaxPercentage        *float64 `json:"taxPercentage"`
		EnableServiceCharge  bool     `json:"enableServiceCharge"`
		ServiceChargePercent *float64 `json:"serviceChargePercent"`
		EnablePackagingFee   bool     `json:"enablePackagingFee"`
		PackagingFeeAmount   *float64 `json:"packagingFeeAmount"`
		IsDineInEnabled      bool     `json:"isDineInEnabled"`
		IsTakeawayEnabled    bool     `json:"isTakeawayEnabled"`
		IsDeliveryEnabled    bool     `json:"isDeliveryEnabled"`
	} `json:"merchant"`
	Order *struct {
		ID          int64   `json:"id"`
		OrderNumber string  `json:"orderNumber"`
		Status      string  `json:"status"`
		TotalAmount float64 `json:"totalAmount"`
	} `json:"order,omitempty"`
}

func (h *Handler) notifyGroupOrderUpdate(ctx context.Context, sessionCode string) {
	code := strings.TrimSpace(sessionCode)
	if code == "" {
		return
	}
	_, _ = h.DB.Exec(ctx, `select pg_notify('group_order_updates', $1)`, strings.ToUpper(code))
}

func (h *Handler) PublicGroupOrderSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}

	sessionCode := strings.ToUpper(code)

	query := `
		select
		  s.id, s.session_code, s.order_type, s.table_number, s.status, s.merchant_id, s.order_id, s.max_participants,
		  s.expires_at, s.created_at, s.updated_at,
		  m.id, m.code, m.name, m.currency, m.enable_tax, m.tax_percentage, m.enable_service_charge, m.service_charge_percent,
		  m.enable_packaging_fee, m.packaging_fee_amount, m.is_dine_in_enabled, m.is_takeaway_enabled, m.is_delivery_enabled,
		  o.id, o.order_number, o.status, o.total_amount
		from group_order_sessions s
		join merchants m on m.id = s.merchant_id
		left join orders o on o.id = s.order_id
		where s.session_code = $1
		  and s.status in ('OPEN', 'LOCKED', 'SUBMITTED')
		limit 1
	`

	var (
		session           GroupOrderSession
		merchantTax       pgtype.Numeric
		merchantService   pgtype.Numeric
		merchantPackaging pgtype.Numeric
		tableNumber       pgtype.Text
		orderID           pgtype.Int8
		orderNumber       pgtype.Text
		orderStatus       pgtype.Text
		orderTotal        pgtype.Numeric
	)

	if err := h.DB.QueryRow(ctx, query, sessionCode).Scan(
		&session.ID,
		&session.SessionCode,
		&session.OrderType,
		&tableNumber,
		&session.Status,
		&session.MerchantID,
		&orderID,
		&session.MaxParticipants,
		&session.ExpiresAt,
		&session.CreatedAt,
		&session.UpdatedAt,
		&session.Merchant.ID,
		&session.Merchant.Code,
		&session.Merchant.Name,
		&session.Merchant.Currency,
		&session.Merchant.EnableTax,
		&merchantTax,
		&session.Merchant.EnableServiceCharge,
		&merchantService,
		&session.Merchant.EnablePackagingFee,
		&merchantPackaging,
		&session.Merchant.IsDineInEnabled,
		&session.Merchant.IsTakeawayEnabled,
		&session.Merchant.IsDeliveryEnabled,
		&orderID,
		&orderNumber,
		&orderStatus,
		&orderTotal,
	); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Group order session not found or has expired")
		return
	}

	if tableNumber.Valid {
		value := tableNumber.String
		session.TableNumber = &value
	}

	if session.ExpiresAt.Before(time.Now()) && session.Status == "OPEN" {
		_, _ = h.DB.Exec(ctx, "update group_order_sessions set status = 'EXPIRED' where id = $1", session.ID)
		response.Error(w, http.StatusGone, "SESSION_EXPIRED", "This group order session has expired")
		return
	}

	if merchantTax.Valid {
		v := utils.NumericToFloat64(merchantTax)
		session.Merchant.TaxPercentage = &v
	}
	if merchantService.Valid {
		v := utils.NumericToFloat64(merchantService)
		session.Merchant.ServiceChargePercent = &v
	}
	if merchantPackaging.Valid {
		v := utils.NumericToFloat64(merchantPackaging)
		session.Merchant.PackagingFeeAmount = &v
	}

	if orderID.Valid {
		session.OrderID = &orderID.Int64
		val := utils.NumericToFloat64(orderTotal)
		orderNumberStr := orderNumber.String
		orderStatusStr := orderStatus.String
		session.Order = &struct {
			ID          int64   `json:"id"`
			OrderNumber string  `json:"orderNumber"`
			Status      string  `json:"status"`
			TotalAmount float64 `json:"totalAmount"`
		}{
			ID:          orderID.Int64,
			OrderNumber: orderNumberStr,
			Status:      orderStatusStr,
			TotalAmount: val,
		}
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, session.ID)
	if err != nil {
		h.Logger.Error("group order participants fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch session details")
		return
	}

	payload := buildGroupOrderSessionPayload(session, participants)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
	})
}

func buildGroupOrderSessionPayload(session GroupOrderSession, participants []GroupOrderParticipant) map[string]any {
	participantPayloads := buildGroupOrderParticipantPayloads(participants)
	participantCount := len(participants)
	var totalSubtotal float64
	var hostName string
	for _, p := range participants {
		totalSubtotal += p.Subtotal
		if p.IsHost {
			hostName = p.Name
		}
	}

	var orderPayload any
	var orderIDPayload any
	if session.OrderID != nil {
		orderIDPayload = strconv.FormatInt(*session.OrderID, 10)
	}
	if session.Order != nil {
		orderPayload = map[string]any{
			"id":          strconv.FormatInt(session.Order.ID, 10),
			"orderNumber": session.Order.OrderNumber,
			"status":      session.Order.Status,
			"totalAmount": session.Order.TotalAmount,
		}
	}

	merchantPayload := map[string]any{
		"id":                   strconv.FormatInt(session.Merchant.ID, 10),
		"code":                 session.Merchant.Code,
		"name":                 session.Merchant.Name,
		"currency":             session.Merchant.Currency,
		"enableTax":            session.Merchant.EnableTax,
		"taxPercentage":        session.Merchant.TaxPercentage,
		"enableServiceCharge":  session.Merchant.EnableServiceCharge,
		"serviceChargePercent": session.Merchant.ServiceChargePercent,
		"enablePackagingFee":   session.Merchant.EnablePackagingFee,
		"packagingFeeAmount":   session.Merchant.PackagingFeeAmount,
		"isDineInEnabled":      session.Merchant.IsDineInEnabled,
		"isTakeawayEnabled":    session.Merchant.IsTakeawayEnabled,
		"isDeliveryEnabled":    session.Merchant.IsDeliveryEnabled,
	}

	return map[string]any{
		"id":              strconv.FormatInt(session.ID, 10),
		"sessionCode":     session.SessionCode,
		"orderType":       session.OrderType,
		"tableNumber":     session.TableNumber,
		"status":          session.Status,
		"merchantId":      strconv.FormatInt(session.MerchantID, 10),
		"orderId":         orderIDPayload,
		"maxParticipants": session.MaxParticipants,
		"expiresAt":       session.ExpiresAt,
		"createdAt":       session.CreatedAt,
		"updatedAt":       session.UpdatedAt,
		"participants":    participantPayloads,
		"merchant":        merchantPayload,
		"order":           orderPayload,
		"summary": map[string]any{
			"participantCount": participantCount,
			"totalSubtotal":    totalSubtotal,
			"hostName":         hostName,
			"isExpired":        session.ExpiresAt.Before(time.Now()),
			"expiresIn":        maxInt64(0, session.ExpiresAt.UnixMilli()-time.Now().UnixMilli()),
		},
	}
}

func buildGroupOrderParticipantPayloads(participants []GroupOrderParticipant) []map[string]any {
	payloads := make([]map[string]any, 0, len(participants))
	for _, p := range participants {
		cartItems := p.CartItems
		if len(cartItems) == 0 {
			cartItems = json.RawMessage("[]")
		}

		entry := map[string]any{
			"id":         strconv.FormatInt(p.ID, 10),
			"customerId": nil,
			"name":       p.Name,
			"deviceId":   p.DeviceID,
			"isHost":     p.IsHost,
			"cartItems":  cartItems,
			"subtotal":   p.Subtotal,
			"joinedAt":   p.JoinedAt,
			"updatedAt":  p.UpdatedAt,
		}

		if p.CustomerID != nil {
			entry["customerId"] = strconv.FormatInt(*p.CustomerID, 10)
		}
		if p.Customer != nil {
			customerPayload := map[string]any{
				"id":   strconv.FormatInt(p.Customer.ID, 10),
				"name": p.Customer.Name,
			}
			if p.Customer.Phone != nil {
				customerPayload["phone"] = *p.Customer.Phone
			}
			entry["customer"] = customerPayload
		}

		payloads = append(payloads, entry)
	}

	return payloads
}

type groupOrderCreateRequest struct {
	MerchantCode string  `json:"merchantCode"`
	OrderType    string  `json:"orderType"`
	TableNumber  *string `json:"tableNumber"`
	HostName     string  `json:"hostName"`
	DeviceID     *string `json:"deviceId"`
	CustomerID   *string `json:"customerId"`
}

type groupOrderJoinRequest struct {
	Name       string  `json:"name"`
	DeviceID   *string `json:"deviceId"`
	CustomerID *string `json:"customerId"`
}

type groupOrderLeaveRequest struct {
	DeviceID string `json:"deviceId"`
}

type groupOrderKickRequest struct {
	DeviceID      string `json:"deviceId"`
	ParticipantID string `json:"participantId"`
	Confirmed     bool   `json:"confirmed"`
}

type groupOrderTransferRequest struct {
	DeviceID  string `json:"deviceId"`
	NewHostID string `json:"newHostId"`
}

type groupOrderCartUpdateRequest struct {
	DeviceID  string               `json:"deviceId"`
	CartItems []groupOrderCartItem `json:"cartItems"`
}

type groupOrderSubmitRequest struct {
	DeviceID               string   `json:"deviceId"`
	CustomerName           string   `json:"customerName"`
	CustomerEmail          string   `json:"customerEmail"`
	CustomerPhone          *string  `json:"customerPhone"`
	Notes                  *string  `json:"notes"`
	DeliveryUnit           *string  `json:"deliveryUnit"`
	DeliveryAddress        *string  `json:"deliveryAddress"`
	DeliveryLatitude       *float64 `json:"deliveryLatitude"`
	DeliveryLongitude      *float64 `json:"deliveryLongitude"`
	DeliveryBuildingName   *string  `json:"deliveryBuildingName"`
	DeliveryBuildingNumber *string  `json:"deliveryBuildingNumber"`
	DeliveryFloor          *string  `json:"deliveryFloor"`
	DeliveryInstructions   *string  `json:"deliveryInstructions"`
	DeliveryStreetLine     *string  `json:"deliveryStreetLine"`
	DeliverySuburb         *string  `json:"deliverySuburb"`
	DeliveryCity           *string  `json:"deliveryCity"`
	DeliveryState          *string  `json:"deliveryState"`
	DeliveryPostcode       *string  `json:"deliveryPostcode"`
	DeliveryCountry        *string  `json:"deliveryCountry"`
}

type groupOrderCartItem struct {
	CartItemID string                `json:"cartItemId"`
	MenuID     string                `json:"menuId"`
	MenuName   string                `json:"menuName"`
	Price      float64               `json:"price"`
	Quantity   int32                 `json:"quantity"`
	Addons     []groupOrderCartAddon `json:"addons"`
	Notes      string                `json:"notes"`
}

type groupOrderCartAddon struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

type groupOrderOrderItemData struct {
	MenuID          int64
	MenuName        string
	MenuPrice       float64
	Quantity        int32
	Subtotal        float64
	Notes           *string
	Addons          []groupOrderAddonData
	ParticipantID   int64
	ParticipantName string
}

type groupOrderAddonData struct {
	AddonItemID int64
	AddonName   string
	AddonPrice  float64
	Quantity    int32
	Subtotal    float64
}

const (
	groupJoinRateLimitWindow = time.Minute
	groupJoinMaxAttempts     = 3
)

func (h *Handler) PublicGroupOrderCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body groupOrderCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantCode := strings.TrimSpace(body.MerchantCode)
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}
	if strings.TrimSpace(body.HostName) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Host name is required")
		return
	}
	orderType := strings.ToUpper(strings.TrimSpace(body.OrderType))
	if orderType != "DINE_IN" && orderType != "TAKEAWAY" && orderType != "DELIVERY" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Valid order type is required (DINE_IN, TAKEAWAY, DELIVERY)")
		return
	}
	if orderType == "DINE_IN" && (body.TableNumber == nil || strings.TrimSpace(*body.TableNumber) == "") {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Table number is required for dine-in orders")
		return
	}

	var (
		merchantID int64
		isActive   bool
	)
	if err := h.DB.QueryRow(ctx, `select id, is_active from merchants where code = $1`, merchantCode).Scan(&merchantID, &isActive); err != nil || !isActive {
		response.Error(w, http.StatusBadRequest, "MERCHANT_INACTIVE", "Merchant is currently not accepting orders")
		return
	}

	deviceID := ""
	if body.DeviceID != nil && strings.TrimSpace(*body.DeviceID) != "" {
		deviceID = strings.TrimSpace(*body.DeviceID)
	} else {
		deviceID = generateGroupDeviceID()
	}

	var customerID *int64
	if body.CustomerID != nil {
		parsed, err := parseOptionalInt64(*body.CustomerID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid customer ID")
			return
		}
		customerID = parsed
	}

	sessionCode := ""
	for attempts := 0; attempts < 10; attempts++ {
		candidate := generateGroupSessionCode()
		var exists bool
		if err := h.DB.QueryRow(ctx, `
			select exists(select 1 from group_order_sessions where session_code = $1 and status in ('OPEN','LOCKED'))
		`, candidate).Scan(&exists); err == nil && !exists {
			sessionCode = candidate
			break
		}
	}
	if sessionCode == "" {
		response.Error(w, http.StatusInternalServerError, "CODE_GENERATION_FAILED", "Unable to generate unique session code. Please try again.")
		return
	}

	expiresAt := time.Now().Add(2 * time.Hour)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `
		insert into group_order_sessions (session_code, merchant_id, order_type, table_number, status, max_participants, expires_at, created_at, updated_at)
		values ($1,$2,$3,$4,'OPEN',15,$5,now(),now())
		returning id
	`, sessionCode, merchantID, orderType, nullIfEmptyPtr(body.TableNumber), expiresAt).Scan(&sessionID); err != nil {
		h.Logger.Error("group order session create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create group order session")
		return
	}

	if _, err := h.DB.Exec(ctx, `
		insert into group_order_participants (session_id, customer_id, name, device_id, is_host, cart_items, subtotal, joined_at, updated_at)
		values ($1,$2,$3,$4,true,'[]',0,now(),now())
	`, sessionID, customerID, strings.TrimSpace(body.HostName), deviceID); err != nil {
		h.Logger.Error("group order host create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create group order host")
		return
	}

	session, participants, err := h.fetchGroupOrderSessionByID(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load group order session")
		return
	}

	payload := buildGroupOrderSessionPayload(session, participants)
	payload["deviceId"] = deviceID

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data":    payload,
		"message": "Group order session created successfully",
	})
}

func (h *Handler) PublicGroupOrderJoin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(body.Name)
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Name is required to join the group")
		return
	}

	deviceID := ""
	if body.DeviceID != nil && strings.TrimSpace(*body.DeviceID) != "" {
		deviceID = strings.TrimSpace(*body.DeviceID)
	} else {
		deviceID = generateGroupDeviceID()
	}

	windowStart := time.Now().Add(-groupJoinRateLimitWindow)
	var recentAttempts int64
	_ = h.DB.QueryRow(ctx, `
		select count(*) from group_join_attempts where device_id = $1 and created_at >= $2 and success = false
	`, deviceID, windowStart).Scan(&recentAttempts)
	if recentAttempts >= groupJoinMaxAttempts {
		waitTime := int(groupJoinRateLimitWindow.Seconds())
		response.Error(w, http.StatusTooManyRequests, "RATE_LIMITED", "Too many failed attempts. Please wait "+strconv.Itoa(waitTime)+" seconds before trying again.")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	var maxParticipants int32
	if err := h.DB.QueryRow(ctx, `
		select id, max_participants from group_order_sessions
		where session_code = $1 and status = 'OPEN' and expires_at > now()
	`, sessionCode).Scan(&sessionID, &maxParticipants); err != nil {
		_, _ = h.DB.Exec(ctx, `insert into group_join_attempts (device_id, attempt_code, success) values ($1,$2,false)`, deviceID, sessionCode)
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Invalid code or session has expired. Please check the code and try again.")
		return
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load group order session")
		return
	}

	_, _ = h.DB.Exec(ctx, `insert into group_join_attempts (device_id, attempt_code, success) values ($1,$2,true)`, deviceID, sessionCode)

	for _, p := range participants {
		if p.DeviceID == deviceID {
			session, refreshedParticipants, err := h.fetchGroupOrderSessionByID(ctx, sessionID)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load group order session")
				return
			}
			payload := buildGroupOrderSessionPayload(session, refreshedParticipants)
			payload["participantId"] = strconv.FormatInt(p.ID, 10)
			payload["isReconnection"] = true
			payload["deviceId"] = deviceID
			response.JSON(w, http.StatusOK, map[string]any{
				"success": true,
				"data":    payload,
				"message": "Reconnected to group order session",
			})
			return
		}
	}

	if int32(len(participants)) >= maxParticipants {
		response.Error(w, http.StatusBadRequest, "SESSION_FULL", "This group has reached the maximum of "+strconv.Itoa(int(maxParticipants))+" participants")
		return
	}

	var customerID *int64
	if body.CustomerID != nil {
		parsed, err := parseOptionalInt64(*body.CustomerID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid customer ID")
			return
		}
		customerID = parsed
	}

	var participantID int64
	if err := h.DB.QueryRow(ctx, `
		insert into group_order_participants (session_id, customer_id, name, device_id, is_host, cart_items, subtotal)
		values ($1,$2,$3,$4,false,'[]',0)
		returning id
	`, sessionID, customerID, name, deviceID).Scan(&participantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to join group order session")
		return
	}

	session, refreshedParticipants, err := h.fetchGroupOrderSessionByID(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load group order session")
		return
	}
	payload := buildGroupOrderSessionPayload(session, refreshedParticipants)
	payload["participantId"] = strconv.FormatInt(participantID, 10)
	payload["isReconnection"] = false
	payload["deviceId"] = deviceID

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data":    payload,
		"message": "Successfully joined the group order",
	})
}

func (h *Handler) PublicGroupOrderLeave(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderLeaveRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `
		select id from group_order_sessions where session_code = $1 and status = 'OPEN'
	`, sessionCode).Scan(&sessionID); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found or already closed")
		return
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load session details")
		return
	}

	var participant *GroupOrderParticipant
	for i := range participants {
		if participants[i].DeviceID == body.DeviceID {
			participant = &participants[i]
			break
		}
	}
	if participant == nil {
		response.Error(w, http.StatusNotFound, "PARTICIPANT_NOT_FOUND", "You are not a participant in this session")
		return
	}

	if participant.IsHost {
		otherParticipants := make([]GroupOrderParticipant, 0)
		for _, p := range participants {
			if !p.IsHost {
				otherParticipants = append(otherParticipants, p)
			}
		}
		if len(otherParticipants) == 0 {
			_, _ = h.DB.Exec(ctx, `update group_order_sessions set status = 'CANCELLED' where id = $1`, sessionID)
			h.notifyGroupOrderUpdate(ctx, sessionCode)
			response.JSON(w, http.StatusOK, map[string]any{
				"success": true,
				"message": "Session cancelled as you were the only participant",
				"data": map[string]any{
					"sessionCancelled": true,
				},
			})
			return
		}

		newHost := otherParticipants[0]
		tx, err := h.DB.Begin(ctx)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to leave session")
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()

		if _, err := tx.Exec(ctx, `delete from group_order_participants where id = $1`, participant.ID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to leave session")
			return
		}
		if _, err := tx.Exec(ctx, `update group_order_participants set is_host = true where id = $1`, newHost.ID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer host")
			return
		}
		if err := tx.Commit(ctx); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to leave session")
			return
		}

		h.notifyGroupOrderUpdate(ctx, sessionCode)

		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"message": "You have left the group. " + newHost.Name + " is now the host.",
			"data": map[string]any{
				"sessionCancelled": false,
				"newHostName":      newHost.Name,
			},
		})
		return
	}

	if _, err := h.DB.Exec(ctx, `delete from group_order_participants where id = $1`, participant.ID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to leave session")
		return
	}

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "You have left the group order",
		"data": map[string]any{
			"sessionCancelled": false,
		},
	})
}

func (h *Handler) PublicGroupOrderKick(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderKickRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}
	if strings.TrimSpace(body.ParticipantID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Participant ID to kick is required")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `
		select id from group_order_sessions where session_code = $1 and status = 'OPEN'
	`, sessionCode).Scan(&sessionID); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found or already closed")
		return
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load session details")
		return
	}

	var host *GroupOrderParticipant
	var target *GroupOrderParticipant
	for i := range participants {
		if participants[i].IsHost && participants[i].DeviceID == body.DeviceID {
			host = &participants[i]
		}
		if strconv.FormatInt(participants[i].ID, 10) == body.ParticipantID {
			target = &participants[i]
		}
	}
	if host == nil {
		response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Only the host can remove participants")
		return
	}
	if target == nil {
		response.Error(w, http.StatusNotFound, "PARTICIPANT_NOT_FOUND", "Participant not found in this session")
		return
	}
	if target.IsHost {
		response.Error(w, http.StatusBadRequest, "INVALID_OPERATION", "You cannot kick yourself. Use \"Leave Group\" instead.")
		return
	}

	items, _ := decodeGroupOrderCartItems(target.CartItems)
	if len(items) > 0 && !body.Confirmed {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "CONFIRMATION_REQUIRED",
			"message": target.Name + " has items in their cart. Are you sure you want to remove them?",
			"data": map[string]any{
				"participantName":      target.Name,
				"itemCount":            len(items),
				"requiresConfirmation": true,
			},
		})
		return
	}

	if _, err := h.DB.Exec(ctx, `delete from group_order_participants where id = $1`, target.ID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove participant")
		return
	}

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": target.Name + " has been removed from the group",
		"data": map[string]any{
			"kickedParticipant": map[string]any{
				"id":   strconv.FormatInt(target.ID, 10),
				"name": target.Name,
			},
		},
	})
}

func (h *Handler) PublicGroupOrderTransferHost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}
	if strings.TrimSpace(body.NewHostID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "New host ID is required")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `select id from group_order_sessions where session_code = $1 and status = 'OPEN'`, sessionCode).Scan(&sessionID); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found or already closed")
		return
	}
	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load session details")
		return
	}
	var currentHost *GroupOrderParticipant
	var newHost *GroupOrderParticipant
	for i := range participants {
		if participants[i].IsHost && participants[i].DeviceID == body.DeviceID {
			currentHost = &participants[i]
		}
		if strconv.FormatInt(participants[i].ID, 10) == body.NewHostID && !participants[i].IsHost {
			newHost = &participants[i]
		}
	}
	if currentHost == nil {
		response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Only the current host can transfer host role")
		return
	}
	if newHost == nil {
		response.Error(w, http.StatusNotFound, "PARTICIPANT_NOT_FOUND", "Selected participant not found or is already the host")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer host")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `update group_order_participants set is_host = false where id = $1`, currentHost.ID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer host")
		return
	}
	if _, err := tx.Exec(ctx, `update group_order_participants set is_host = true where id = $1`, newHost.ID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer host")
		return
	}
	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer host")
		return
	}

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	updatedSession, updatedParticipants, err := h.fetchGroupOrderSessionByID(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load updated session")
		return
	}
	payload := buildGroupOrderSessionPayload(updatedSession, updatedParticipants)
	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
		"message": newHost.Name + " is now the host",
	})
}

func (h *Handler) PublicGroupOrderCancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderLeaveRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `select id from group_order_sessions where session_code = $1 and status = 'OPEN'`, sessionCode).Scan(&sessionID); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found or already closed")
		return
	}
	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load session details")
		return
	}
	var hostFound bool
	for _, p := range participants {
		if p.IsHost && p.DeviceID == body.DeviceID {
			hostFound = true
			break
		}
	}
	if !hostFound {
		response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Only the host can cancel the session")
		return
	}

	if _, err := h.DB.Exec(ctx, `update group_order_sessions set status = 'CANCELLED' where id = $1`, sessionID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to cancel session")
		return
	}

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Group order session cancelled",
	})
}

func (h *Handler) PublicGroupOrderUpdateCart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderCartUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}

	sessionCode := strings.ToUpper(code)
	var sessionID int64
	if err := h.DB.QueryRow(ctx, `
		select id from group_order_sessions where session_code = $1 and status = 'OPEN' and expires_at > now()
	`, sessionCode).Scan(&sessionID); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found, closed, or expired")
		return
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, sessionID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load session details")
		return
	}
	var participant *GroupOrderParticipant
	for i := range participants {
		if participants[i].DeviceID == body.DeviceID {
			participant = &participants[i]
			break
		}
	}
	if participant == nil {
		response.Error(w, http.StatusForbidden, "PARTICIPANT_NOT_FOUND", "You are not a participant in this session")
		return
	}

	var subtotal float64
	for _, item := range body.CartItems {
		itemTotal := float64(item.Quantity) * item.Price
		if len(item.Addons) > 0 {
			for _, addon := range item.Addons {
				itemTotal += float64(item.Quantity) * addon.Price
			}
		}
		subtotal += itemTotal
	}
	finalSubtotal := round2(subtotal)

	encoded, _ := json.Marshal(body.CartItems)
	if _, err := h.DB.Exec(ctx, `
		update group_order_participants set cart_items = $1, subtotal = $2 where id = $3
	`, encoded, finalSubtotal, participant.ID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update cart")
		return
	}

	h.notifyGroupOrderUpdate(ctx, sessionCode)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"participantId": strconv.FormatInt(participant.ID, 10),
			"cartItems":     body.CartItems,
			"subtotal":      finalSubtotal,
			"itemCount":     len(body.CartItems),
		},
		"message": "Cart updated successfully",
	})
}

func (h *Handler) PublicGroupOrderSubmit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	code := readPathString(r, "code")
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Session code is required")
		return
	}
	var body groupOrderSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	if strings.TrimSpace(body.DeviceID) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Device ID is required")
		return
	}
	if strings.TrimSpace(body.CustomerName) == "" || strings.TrimSpace(body.CustomerEmail) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Customer name and email are required for order submission")
		return
	}

	sessionCode := strings.ToUpper(code)
	var session GroupOrderSession
	var tableNumber pgtype.Text
	if err := h.DB.QueryRow(ctx, `
		select id, session_code, order_type, table_number, status, merchant_id, max_participants, expires_at, created_at, updated_at
		from group_order_sessions
		where session_code = $1 and status = 'OPEN' and expires_at > now()
	`, sessionCode).Scan(
		&session.ID,
		&session.SessionCode,
		&session.OrderType,
		&tableNumber,
		&session.Status,
		&session.MerchantID,
		&session.MaxParticipants,
		&session.ExpiresAt,
		&session.CreatedAt,
		&session.UpdatedAt,
	); err != nil {
		response.Error(w, http.StatusNotFound, "SESSION_NOT_FOUND", "Session not found, expired, or already submitted")
		return
	}
	if tableNumber.Valid {
		value := tableNumber.String
		session.TableNumber = &value
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, session.ID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch session details")
		return
	}
	session.Participants = participants

	var host *GroupOrderParticipant
	for i := range participants {
		if participants[i].IsHost && participants[i].DeviceID == body.DeviceID {
			host = &participants[i]
			break
		}
	}
	if host == nil {
		response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Only the host can submit the order")
		return
	}
	if len(participants) < 2 {
		response.Error(w, http.StatusBadRequest, "INSUFFICIENT_PARTICIPANTS", "Group order requires at least 2 participants")
		return
	}

	menuIDs := make([]int64, 0)
	addonIDs := make([]int64, 0)
	participantCart := make(map[int64][]groupOrderCartItem)
	for _, participant := range participants {
		items, err := decodeGroupOrderCartItems(participant.CartItems)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid cart data")
			return
		}
		participantCart[participant.ID] = items
		for _, item := range items {
			menuID, err := strconv.ParseInt(item.MenuID, 10, 64)
			if err != nil {
				continue
			}
			menuIDs = append(menuIDs, menuID)
			for _, addon := range item.Addons {
				addonID, err := strconv.ParseInt(addon.ID, 10, 64)
				if err == nil {
					addonIDs = append(addonIDs, addonID)
				}
			}
		}
	}
	if len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "NO_ITEMS", "At least one participant must have items in their cart")
		return
	}

	_, menuMap, err := h.fetchGroupOrderMenus(ctx, session.MerchantID, menuIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu data")
		return
	}
	addonMap, err := h.fetchGroupOrderAddons(ctx, addonIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch addon data")
		return
	}
	promoMap := h.fetchGroupOrderPromoPrices(ctx, menuIDs, session.MerchantID)

	orderItems := make([]groupOrderOrderItemData, 0)
	var subtotal float64

	for _, participant := range participants {
		items := participantCart[participant.ID]
		for _, item := range items {
			menuID, err := strconv.ParseInt(item.MenuID, 10, 64)
			if err != nil {
				continue
			}
			menu, ok := menuMap[menuID]
			if !ok || !menu.IsActive || menu.DeletedAt.Valid {
				continue
			}
			if menu.TrackStock && menu.StockQty.Valid && menu.StockQty.Int32 < item.Quantity {
				response.Error(w, http.StatusBadRequest, "INSUFFICIENT_STOCK", "Insufficient stock for \""+menu.Name+"\" (ordered by "+participant.Name+")")
				return
			}

			price := menu.Price
			if promo, ok := promoMap[menuID]; ok {
				price = promo
			}
			menuPrice := round2(price)
			itemTotal := round2(menuPrice * float64(item.Quantity))

			addons := make([]groupOrderAddonData, 0)
			if len(item.Addons) > 0 {
				for _, addon := range item.Addons {
					addonID, err := strconv.ParseInt(addon.ID, 10, 64)
					if err != nil {
						continue
					}
					addonData, ok := addonMap[addonID]
					if !ok || !addonData.IsActive || addonData.DeletedAt.Valid {
						continue
					}
					addonPrice := round2(addonData.Price)
					addonQty := int32(1)
					addonSubtotal := round2(addonPrice * float64(addonQty) * float64(item.Quantity))
					itemTotal = round2(itemTotal + addonSubtotal)
					addons = append(addons, groupOrderAddonData{
						AddonItemID: addonID,
						AddonName:   addonData.Name,
						AddonPrice:  addonData.Price,
						Quantity:    addonQty,
						Subtotal:    round2(addonPrice * float64(addonQty)),
					})
				}
			}

			subtotal = round2(subtotal + itemTotal)
			notes := strings.TrimSpace(item.Notes)
			var notesPtr *string
			if notes != "" {
				notesPtr = &notes
			}

			orderItems = append(orderItems, groupOrderOrderItemData{
				MenuID:          menuID,
				MenuName:        menu.Name,
				MenuPrice:       menuPrice,
				Quantity:        item.Quantity,
				Subtotal:        itemTotal,
				Notes:           notesPtr,
				Addons:          addons,
				ParticipantID:   participant.ID,
				ParticipantName: participant.Name,
			})
		}
	}
	if len(orderItems) == 0 {
		response.Error(w, http.StatusBadRequest, "NO_VALID_ITEMS", "No valid items found in carts. Some items may be unavailable.")
		return
	}

	merchant, deliveryConfig, err := h.fetchGroupOrderMerchant(ctx, session.MerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	taxAmount := 0.0
	if merchant.EnableTax && merchant.TaxPercentage.Valid {
		taxAmount = round2(subtotal * (utils.NumericToFloat64(merchant.TaxPercentage) / 100))
	}
	serviceChargeAmount := 0.0
	if merchant.EnableServiceCharge && merchant.ServiceChargePercent.Valid {
		serviceChargeAmount = round2(subtotal * (utils.NumericToFloat64(merchant.ServiceChargePercent) / 100))
	}
	packagingFeeAmount := 0.0
	if (session.OrderType == "TAKEAWAY" || session.OrderType == "DELIVERY") && merchant.EnablePackagingFee && merchant.PackagingFeeAmount.Valid {
		packagingFeeAmount = round2(utils.NumericToFloat64(merchant.PackagingFeeAmount))
	}

	deliveryFeeAmount := 0.0
	var deliveryDistance *float64
	var deliveryLat *float64
	var deliveryLng *float64
	if session.OrderType == "DELIVERY" {
		if body.DeliveryLatitude == nil || body.DeliveryLongitude == nil || body.DeliveryAddress == nil || strings.TrimSpace(*body.DeliveryAddress) == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Delivery address and coordinates are required for delivery orders")
			return
		}
		if !isFinite(*body.DeliveryLatitude) || !isFinite(*body.DeliveryLongitude) {
			response.Error(w, http.StatusBadRequest, "INVALID_DELIVERY_COORDS", "Valid delivery coordinates are required")
			return
		}

		fee, distance, errCode, errMessage := h.calculateGroupOrderDeliveryFee(ctx, deliveryConfig, *body.DeliveryLatitude, *body.DeliveryLongitude)
		if errCode != "" {
			response.Error(w, http.StatusBadRequest, errCode, errMessage)
			return
		}
		deliveryFeeAmount = fee
		deliveryDistance = &distance
		deliveryLat = body.DeliveryLatitude
		deliveryLng = body.DeliveryLongitude
	}

	totalAmount := round2(subtotal + taxAmount + serviceChargeAmount + packagingFeeAmount + deliveryFeeAmount)

	customerID, err := h.resolveGroupOrderCustomer(ctx, body.CustomerName, body.CustomerEmail, body.CustomerPhone)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to resolve customer")
		return
	}

	orderNumber, err := h.generateGroupOrderNumber(ctx, session.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate order number")
		return
	}

	createdOrderID, err := h.createGroupOrder(ctx, session, orderNumber, customerID, subtotal, taxAmount, serviceChargeAmount, packagingFeeAmount, deliveryFeeAmount, totalAmount, orderItems, body, deliveryLat, deliveryLng, deliveryDistance)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create order")
		return
	}

	for _, item := range orderItems {
		h.decrementPOSStock(ctx, item.MenuID, item.Quantity)
	}
	if customerID != nil {
		_, _ = h.DB.Exec(ctx, `
			update customers set total_orders = total_orders + 1, total_spent = total_spent + $1, last_order_at = now() where id = $2
		`, totalAmount, *customerID)
	}

	participantSubtotals := make(map[int64]float64)
	for _, item := range orderItems {
		participantSubtotals[item.ParticipantID] = round2(participantSubtotals[item.ParticipantID] + item.Subtotal)
	}

	splitBill := make([]map[string]any, 0)
	for _, participant := range participants {
		participantSubtotal := participantSubtotals[participant.ID]
		shareRatio := 0.0
		if subtotal > 0 {
			shareRatio = participantSubtotal / subtotal
		}
		splitBill = append(splitBill, map[string]any{
			"participantId":      strconv.FormatInt(participant.ID, 10),
			"participantName":    participant.Name,
			"isHost":             participant.IsHost,
			"subtotal":           participantSubtotal,
			"taxShare":           round2(taxAmount * shareRatio),
			"serviceChargeShare": round2(serviceChargeAmount * shareRatio),
			"packagingFeeShare":  round2(packagingFeeAmount * shareRatio),
			"total":              round2(participantSubtotal + (taxAmount * shareRatio) + (serviceChargeAmount * shareRatio) + (packagingFeeAmount * shareRatio)),
		})
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"order": map[string]any{
				"id":                  strconv.FormatInt(createdOrderID, 10),
				"orderNumber":         orderNumber,
				"status":              "PENDING",
				"subtotal":            subtotal,
				"taxAmount":           taxAmount,
				"serviceChargeAmount": serviceChargeAmount,
				"packagingFeeAmount":  packagingFeeAmount,
				"totalAmount":         totalAmount,
				"itemCount":           len(orderItems),
			},
			"sessionCode": sessionCode,
			"splitBill":   splitBill,
			"merchant": map[string]any{
				"code":     merchant.Code,
				"name":     merchant.Name,
				"currency": merchant.Currency,
			},
		},
		"message": "Group order submitted successfully",
	})

	h.notifyGroupOrderUpdate(ctx, sessionCode)
}

type groupOrderMenuRow struct {
	ID         int64
	Name       string
	Price      float64
	IsActive   bool
	DeletedAt  pgtype.Timestamptz
	TrackStock bool
	StockQty   pgtype.Int4
}

type groupOrderAddonRow struct {
	ID        int64
	Name      string
	Price     float64
	IsActive  bool
	DeletedAt pgtype.Timestamptz
}

type groupOrderMerchantRow struct {
	ID                   int64
	Code                 string
	Name                 string
	Currency             string
	EnableTax            bool
	TaxPercentage        pgtype.Numeric
	EnableServiceCharge  bool
	ServiceChargePercent pgtype.Numeric
	EnablePackagingFee   bool
	PackagingFeeAmount   pgtype.Numeric
}

type groupOrderDeliveryConfig struct {
	MerchantID           int64
	IsActive             bool
	IsDeliveryEnabled    bool
	EnforceDeliveryZones bool
	Latitude             pgtype.Numeric
	Longitude            pgtype.Numeric
	DeliveryMaxDistance  pgtype.Numeric
	DeliveryFeeBase      pgtype.Numeric
	DeliveryFeePerKm     pgtype.Numeric
	DeliveryFeeMin       pgtype.Numeric
	DeliveryFeeMax       pgtype.Numeric
}

func (h *Handler) fetchGroupOrderSessionByID(ctx context.Context, sessionID int64) (GroupOrderSession, []GroupOrderParticipant, error) {
	query := `
		select
		  s.id, s.session_code, s.order_type, s.table_number, s.status, s.merchant_id, s.order_id, s.max_participants,
		  s.expires_at, s.created_at, s.updated_at,
		  m.id, m.code, m.name, m.currency, m.enable_tax, m.tax_percentage, m.enable_service_charge, m.service_charge_percent,
		  m.enable_packaging_fee, m.packaging_fee_amount, m.is_dine_in_enabled, m.is_takeaway_enabled, m.is_delivery_enabled,
		  o.id, o.order_number, o.status, o.total_amount
		from group_order_sessions s
		join merchants m on m.id = s.merchant_id
		left join orders o on o.id = s.order_id
		where s.id = $1
		limit 1
	`

	var (
		session           GroupOrderSession
		merchantTax       pgtype.Numeric
		merchantService   pgtype.Numeric
		merchantPackaging pgtype.Numeric
		tableNumber       pgtype.Text
		orderID           pgtype.Int8
		orderNumber       pgtype.Text
		orderStatus       pgtype.Text
		orderTotal        pgtype.Numeric
	)

	if err := h.DB.QueryRow(ctx, query, sessionID).Scan(
		&session.ID,
		&session.SessionCode,
		&session.OrderType,
		&tableNumber,
		&session.Status,
		&session.MerchantID,
		&orderID,
		&session.MaxParticipants,
		&session.ExpiresAt,
		&session.CreatedAt,
		&session.UpdatedAt,
		&session.Merchant.ID,
		&session.Merchant.Code,
		&session.Merchant.Name,
		&session.Merchant.Currency,
		&session.Merchant.EnableTax,
		&merchantTax,
		&session.Merchant.EnableServiceCharge,
		&merchantService,
		&session.Merchant.EnablePackagingFee,
		&merchantPackaging,
		&session.Merchant.IsDineInEnabled,
		&session.Merchant.IsTakeawayEnabled,
		&session.Merchant.IsDeliveryEnabled,
		&orderID,
		&orderNumber,
		&orderStatus,
		&orderTotal,
	); err != nil {
		return GroupOrderSession{}, nil, err
	}
	if tableNumber.Valid {
		value := tableNumber.String
		session.TableNumber = &value
	}
	if merchantTax.Valid {
		value := utils.NumericToFloat64(merchantTax)
		session.Merchant.TaxPercentage = &value
	}
	if merchantService.Valid {
		value := utils.NumericToFloat64(merchantService)
		session.Merchant.ServiceChargePercent = &value
	}
	if merchantPackaging.Valid {
		value := utils.NumericToFloat64(merchantPackaging)
		session.Merchant.PackagingFeeAmount = &value
	}
	if orderID.Valid {
		val := utils.NumericToFloat64(orderTotal)
		session.OrderID = &orderID.Int64
		session.Order = &struct {
			ID          int64   `json:"id"`
			OrderNumber string  `json:"orderNumber"`
			Status      string  `json:"status"`
			TotalAmount float64 `json:"totalAmount"`
		}{
			ID:          orderID.Int64,
			OrderNumber: orderNumber.String,
			Status:      orderStatus.String,
			TotalAmount: val,
		}
	}

	participants, err := h.fetchGroupOrderParticipants(ctx, session.ID)
	if err != nil {
		return GroupOrderSession{}, nil, err
	}

	return session, participants, nil
}

func decodeGroupOrderCartItems(raw json.RawMessage) ([]groupOrderCartItem, error) {
	if len(raw) == 0 {
		return []groupOrderCartItem{}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var items []groupOrderCartItem
	if err := decoder.Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func parseOptionalInt64(value string) (*int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func generateGroupSessionCode() string {
	chars := "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	var code strings.Builder
	for i := 0; i < 4; i++ {
		code.WriteByte(chars[rand.Intn(len(chars))])
	}
	return code.String()
}

func generateGroupDeviceID() string {
	return "device_" + strconv.FormatInt(time.Now().UnixMilli(), 10) + "_" + strconv.FormatInt(rand.Int63(), 36)
}

func (h *Handler) fetchGroupOrderMenus(ctx context.Context, merchantID int64, menuIDs []int64) ([]groupOrderMenuRow, map[int64]groupOrderMenuRow, error) {
	rows, err := h.DB.Query(ctx, `
		select id, name, price, is_active, deleted_at, track_stock, stock_qty
		from menus
		where id = any($1) and merchant_id = $2
	`, menuIDs, merchantID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	menuMap := make(map[int64]groupOrderMenuRow)
	menus := make([]groupOrderMenuRow, 0)
	for rows.Next() {
		var (
			row   groupOrderMenuRow
			price pgtype.Numeric
		)
		if err := rows.Scan(&row.ID, &row.Name, &price, &row.IsActive, &row.DeletedAt, &row.TrackStock, &row.StockQty); err != nil {
			return nil, nil, err
		}
		row.Price = utils.NumericToFloat64(price)
		menus = append(menus, row)
		menuMap[row.ID] = row
	}
	return menus, menuMap, nil
}

func (h *Handler) fetchGroupOrderAddons(ctx context.Context, addonIDs []int64) (map[int64]groupOrderAddonRow, error) {
	if len(addonIDs) == 0 {
		return map[int64]groupOrderAddonRow{}, nil
	}
	rows, err := h.DB.Query(ctx, `
		select id, name, price, is_active, deleted_at
		from addon_items
		where id = any($1)
	`, addonIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addonMap := make(map[int64]groupOrderAddonRow)
	for rows.Next() {
		var (
			row   groupOrderAddonRow
			price pgtype.Numeric
		)
		if err := rows.Scan(&row.ID, &row.Name, &price, &row.IsActive, &row.DeletedAt); err != nil {
			return nil, err
		}
		row.Price = utils.NumericToFloat64(price)
		addonMap[row.ID] = row
	}
	return addonMap, nil
}

func (h *Handler) fetchGroupOrderPromoPrices(ctx context.Context, menuIDs []int64, merchantID int64) map[int64]float64 {
	promoMap := make(map[int64]float64)
	if len(menuIDs) == 0 {
		return promoMap
	}
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
	if err != nil {
		return promoMap
	}
	defer rows.Close()
	for rows.Next() {
		var menuID int64
		var promo pgtype.Numeric
		if err := rows.Scan(&menuID, &promo); err == nil {
			promoMap[menuID] = utils.NumericToFloat64(promo)
		}
	}
	return promoMap
}

func (h *Handler) fetchGroupOrderMerchant(ctx context.Context, merchantID int64) (groupOrderMerchantRow, groupOrderDeliveryConfig, error) {
	var merchant groupOrderMerchantRow
	var delivery groupOrderDeliveryConfig
	if err := h.DB.QueryRow(ctx, `
		select id, code, name, currency, enable_tax, tax_percentage, enable_service_charge, service_charge_percent,
		       enable_packaging_fee, packaging_fee_amount,
		       is_active, is_delivery_enabled, enforce_delivery_zones, latitude, longitude, delivery_max_distance_km,
		       delivery_fee_base, delivery_fee_per_km, delivery_fee_min, delivery_fee_max
		from merchants where id = $1
	`, merchantID).Scan(
		&merchant.ID,
		&merchant.Code,
		&merchant.Name,
		&merchant.Currency,
		&merchant.EnableTax,
		&merchant.TaxPercentage,
		&merchant.EnableServiceCharge,
		&merchant.ServiceChargePercent,
		&merchant.EnablePackagingFee,
		&merchant.PackagingFeeAmount,
		&delivery.IsActive,
		&delivery.IsDeliveryEnabled,
		&delivery.EnforceDeliveryZones,
		&delivery.Latitude,
		&delivery.Longitude,
		&delivery.DeliveryMaxDistance,
		&delivery.DeliveryFeeBase,
		&delivery.DeliveryFeePerKm,
		&delivery.DeliveryFeeMin,
		&delivery.DeliveryFeeMax,
	); err != nil {
		return groupOrderMerchantRow{}, groupOrderDeliveryConfig{}, err
	}
	merchant.ID = merchantID
	delivery.MerchantID = merchantID
	return merchant, delivery, nil
}

func (h *Handler) resolveGroupOrderCustomer(ctx context.Context, name, email string, phone *string) (*int64, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return nil, nil
	}
	name = strings.TrimSpace(name)
	phoneValue := ""
	if phone != nil {
		phoneValue = strings.TrimSpace(*phone)
	}
	var customerID int64
	if err := h.DB.QueryRow(ctx, `select id from customers where email = $1`, email).Scan(&customerID); err == nil {
		if name != "" || phoneValue != "" {
			_, _ = h.DB.Exec(ctx, `update customers set name = coalesce(nullif($1,''), name), phone = coalesce(nullif($2,''), phone), updated_at = now() where id = $3`, name, nullIfEmpty(phoneValue), customerID)
		}
		return &customerID, nil
	}
	if err := h.DB.QueryRow(ctx, `
		insert into customers (name, email, phone, is_active, updated_at) values ($1,$2,$3,true, now()) returning id
	`, defaultString(name, "Guest Customer"), email, nullIfEmpty(phoneValue)).Scan(&customerID); err != nil {
		return nil, err
	}
	return &customerID, nil
}

func (h *Handler) generateGroupOrderNumber(ctx context.Context, merchantID int64) (string, error) {
	for attempt := 0; attempt < 10; attempt++ {
		today := time.Now()
		dateStr := today.Format("20060102")
		start := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())
		end := start.Add(24*time.Hour - time.Millisecond)
		var count int64
		if err := h.DB.QueryRow(ctx, `select count(*) from orders where merchant_id = $1 and placed_at >= $2 and placed_at <= $3`, merchantID, start, end).Scan(&count); err != nil {
			return "", err
		}
		sequence := fmt.Sprintf("%04d", count+1)
		randomSuffix := strconv.Itoa(rand.Intn(1000))
		for len(randomSuffix) < 3 {
			randomSuffix = "0" + randomSuffix
		}
		orderNumber := "ORD-" + dateStr + "-" + sequence + randomSuffix
		var exists bool
		if err := h.DB.QueryRow(ctx, `select exists(select 1 from orders where merchant_id = $1 and order_number = $2)`, merchantID, orderNumber).Scan(&exists); err == nil && !exists {
			return orderNumber, nil
		}
	}
	return "", nil
}

func (h *Handler) createGroupOrder(ctx context.Context, session GroupOrderSession, orderNumber string, customerID *int64, subtotal, taxAmount, serviceChargeAmount, packagingFeeAmount, deliveryFeeAmount, totalAmount float64, items []groupOrderOrderItemData, body groupOrderSubmitRequest, deliveryLat, deliveryLng, deliveryDistance *float64) (int64, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `update group_order_sessions set status = 'LOCKED' where id = $1`, session.ID); err != nil {
		return 0, err
	}

	orderNotes := ""
	if body.Notes != nil && strings.TrimSpace(*body.Notes) != "" {
		orderNotes = strings.TrimSpace(*body.Notes)
	} else {
		participantNames := make([]string, 0, len(session.Participants))
		for _, p := range session.Participants {
			participantNames = append(participantNames, p.Name)
		}
		orderNotes = "Group Order: " + strings.Join(participantNames, ", ")
	}

	var orderID int64
	if err := tx.QueryRow(ctx, `
		insert into orders (
			merchant_id, customer_id, order_number, order_type, table_number, status,
			subtotal, tax_amount, service_charge_amount, packaging_fee, delivery_fee_amount, total_amount, notes,
			delivery_unit, delivery_address, delivery_latitude, delivery_longitude, delivery_distance_km,
			delivery_building_name, delivery_building_number, delivery_floor, delivery_instructions,
			delivery_street_line, delivery_suburb, delivery_city, delivery_state, delivery_postcode, delivery_country
		) values ($1,$2,$3,$4,$5,'PENDING',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
		returning id
	`,
		session.MerchantID,
		customerID,
		orderNumber,
		session.OrderType,
		nullIfEmptyPtr(session.TableNumber),
		subtotal,
		taxAmount,
		serviceChargeAmount,
		packagingFeeAmount,
		deliveryFeeAmount,
		totalAmount,
		nullIfEmpty(orderNotes),
		nullIfEmptyPtr(body.DeliveryUnit),
		nullIfEmptyPtr(body.DeliveryAddress),
		deliveryLat,
		deliveryLng,
		deliveryDistance,
		nullIfEmptyPtr(body.DeliveryBuildingName),
		nullIfEmptyPtr(body.DeliveryBuildingNumber),
		nullIfEmptyPtr(body.DeliveryFloor),
		nullIfEmptyPtr(body.DeliveryInstructions),
		nullIfEmptyPtr(body.DeliveryStreetLine),
		nullIfEmptyPtr(body.DeliverySuburb),
		nullIfEmptyPtr(body.DeliveryCity),
		nullIfEmptyPtr(body.DeliveryState),
		nullIfEmptyPtr(body.DeliveryPostcode),
		nullIfEmptyPtr(body.DeliveryCountry),
	).Scan(&orderID); err != nil {
		return 0, err
	}

	for _, item := range items {
		var orderItemID int64
		if err := tx.QueryRow(ctx, `
			insert into order_items (order_id, menu_id, menu_name, menu_price, quantity, subtotal, notes)
			values ($1,$2,$3,$4,$5,$6,$7)
			returning id
		`, orderID, item.MenuID, item.MenuName, item.MenuPrice, item.Quantity, item.Subtotal, nullIfEmptyPtr(item.Notes)).Scan(&orderItemID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `
			insert into group_order_details (session_id, participant_id, order_item_id, participant_name, item_subtotal)
			values ($1,$2,$3,$4,$5)
		`, session.ID, item.ParticipantID, orderItemID, item.ParticipantName, item.Subtotal); err != nil {
			return 0, err
		}
		if len(item.Addons) > 0 {
			for _, addon := range item.Addons {
				if _, err := tx.Exec(ctx, `
					insert into order_item_addons (order_item_id, addon_item_id, addon_name, addon_price, quantity, subtotal)
					values ($1,$2,$3,$4,$5,$6)
				`, orderItemID, addon.AddonItemID, addon.AddonName, addon.AddonPrice, addon.Quantity, addon.Subtotal); err != nil {
					return 0, err
				}
			}
		}
	}

	if _, err := tx.Exec(ctx, `update group_order_participants set cart_items = '[]', subtotal = 0 where session_id = $1`, session.ID); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `update group_order_sessions set order_id = $1, status = 'SUBMITTED' where id = $2`, orderID, session.ID); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return orderID, nil
}

func (h *Handler) calculateGroupOrderDeliveryFee(ctx context.Context, config groupOrderDeliveryConfig, lat, lng float64) (float64, float64, string, string) {
	if !config.IsActive {
		return 0, 0, "MERCHANT_NOT_FOUND", "Merchant not found or inactive"
	}
	if !config.IsDeliveryEnabled {
		return 0, 0, "DELIVERY_NOT_ENABLED", "Delivery is not available for this merchant"
	}
	if !config.Latitude.Valid || !config.Longitude.Valid {
		return 0, 0, "MERCHANT_LOCATION_NOT_SET", "Merchant location is not configured for delivery"
	}

	merchantLat := utils.NumericToFloat64(config.Latitude)
	merchantLng := utils.NumericToFloat64(config.Longitude)

	distanceKm := deliveryRound3(haversineDistanceKm(merchantLat, merchantLng, lat, lng))
	if config.DeliveryMaxDistance.Valid {
		maxDistance := utils.NumericToFloat64(config.DeliveryMaxDistance)
		if distanceKm > maxDistance {
			return 0, 0, "OUT_OF_RANGE", "Delivery is only available within " + formatFloat(maxDistance) + " km"
		}
	}
	if config.EnforceDeliveryZones {
		ok, err := h.validateDeliveryZones(ctx, config.MerchantID, merchantLat, merchantLng, lat, lng)
		if err == errNoZonesConfigured {
			return 0, 0, "NO_ZONES_CONFIGURED", "Delivery zones are not properly configured"
		}
		if err != nil {
			return 0, 0, "VALIDATION_ERROR", "Delivery validation failed"
		}
		if !ok {
			return 0, 0, "OUT_OF_ZONE", "Delivery is not available for this location"
		}
	}

	feeBase := 0.0
	if config.DeliveryFeeBase.Valid {
		feeBase = utils.NumericToFloat64(config.DeliveryFeeBase)
	}
	feePerKm := 0.0
	if config.DeliveryFeePerKm.Valid {
		feePerKm = utils.NumericToFloat64(config.DeliveryFeePerKm)
	}
	fee := deliveryRound2(feeBase + feePerKm*distanceKm)
	if config.DeliveryFeeMin.Valid {
		minFee := utils.NumericToFloat64(config.DeliveryFeeMin)
		if fee < minFee {
			fee = minFee
		}
	}
	if config.DeliveryFeeMax.Valid {
		maxFee := utils.NumericToFloat64(config.DeliveryFeeMax)
		if fee > maxFee {
			fee = maxFee
		}
	}

	return deliveryRound2(fee), distanceKm, "", ""
}

func (h *Handler) fetchGroupOrderParticipants(ctx context.Context, sessionID int64) ([]GroupOrderParticipant, error) {
	query := `
		select p.id, p.customer_id, p.name, p.device_id, p.is_host, p.cart_items, p.subtotal, p.joined_at, p.updated_at,
		       c.name, c.phone
		from group_order_participants p
		left join customers c on c.id = p.customer_id
		where p.session_id = $1
		order by p.joined_at asc
	`

	rows, err := h.DB.Query(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	participants := make([]GroupOrderParticipant, 0)
	for rows.Next() {
		var (
			p             GroupOrderParticipant
			customerID    pgtype.Int8
			customerName  pgtype.Text
			customerPhone pgtype.Text
			subtotal      pgtype.Numeric
			cartItems     []byte
		)

		if err := rows.Scan(
			&p.ID,
			&customerID,
			&p.Name,
			&p.DeviceID,
			&p.IsHost,
			&cartItems,
			&subtotal,
			&p.JoinedAt,
			&p.UpdatedAt,
			&customerName,
			&customerPhone,
		); err != nil {
			return nil, err
		}

		p.CartItems = cartItems
		p.Subtotal = utils.NumericToFloat64(subtotal)

		if customerID.Valid {
			p.CustomerID = &customerID.Int64
			p.Customer = &CustomerSummary{
				ID:   customerID.Int64,
				Name: customerName.String,
			}
			if customerPhone.Valid {
				p.Customer.Phone = &customerPhone.String
			}
		}

		participants = append(participants, p)
	}

	return participants, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
