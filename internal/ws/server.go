package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"genfity-order-services/internal/auth"
	"genfity-order-services/internal/config"
	"genfity-order-services/internal/http/handlers"
	"genfity-order-services/internal/utils"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	DB     *pgxpool.Pool
	Logger *zap.Logger
	Config config.Config

	groupOrderRealtime      *groupOrderRealtime
	merchantOrdersRealtime  *merchantOrdersRealtime
	customerDisplayRealtime *customerDisplayRealtime
	publicOrderRealtime     *publicOrderRealtime
}

func New(db *pgxpool.Pool, logger *zap.Logger, cfg config.Config) *Server {
	srv := &Server{DB: db, Logger: logger, Config: cfg}
	srv.groupOrderRealtime = newGroupOrderRealtime(db, logger)
	srv.merchantOrdersRealtime = newMerchantOrdersRealtime(db, logger)
	srv.customerDisplayRealtime = newCustomerDisplayRealtime(db, logger)
	srv.publicOrderRealtime = newPublicOrderRealtime(db, logger)
	return srv
}

type wsRealtimeClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *wsRealtimeClient) writeJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(value)
}

type merchantOrdersRealtime struct {
	db     *pgxpool.Pool
	logger *zap.Logger

	started sync.Once
	mu      sync.RWMutex
	subs    map[string]map[*wsRealtimeClient]struct{}
}

func newMerchantOrdersRealtime(db *pgxpool.Pool, logger *zap.Logger) *merchantOrdersRealtime {
	return &merchantOrdersRealtime{
		db:     db,
		logger: logger,
		subs:   make(map[string]map[*wsRealtimeClient]struct{}),
	}
}

func (mr *merchantOrdersRealtime) ensureStarted() {
	mr.started.Do(func() {
		go mr.listenLoop(context.Background())
	})
}

func (mr *merchantOrdersRealtime) subscribe(merchantID string, client *wsRealtimeClient) (unsubscribe func()) {
	key := strings.TrimSpace(merchantID)
	if key == "" {
		return func() {}
	}

	mr.mu.Lock()
	if mr.subs[key] == nil {
		mr.subs[key] = make(map[*wsRealtimeClient]struct{})
	}
	mr.subs[key][client] = struct{}{}
	mr.mu.Unlock()

	return func() {
		mr.mu.Lock()
		clients := mr.subs[key]
		delete(clients, client)
		if len(clients) == 0 {
			delete(mr.subs, key)
		}
		mr.mu.Unlock()
	}
}

func (mr *merchantOrdersRealtime) broadcast(merchantID string, message any) {
	key := strings.TrimSpace(merchantID)
	if key == "" {
		return
	}

	mr.mu.RLock()
	clientsMap := mr.subs[key]
	clients := make([]*wsRealtimeClient, 0, len(clientsMap))
	for c := range clientsMap {
		clients = append(clients, c)
	}
	mr.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	for _, c := range clients {
		if err := c.writeJSON(message); err != nil {
			_ = c.conn.Close()
			mr.mu.Lock()
			if current := mr.subs[key]; current != nil {
				delete(current, c)
				if len(current) == 0 {
					delete(mr.subs, key)
				}
			}
			mr.mu.Unlock()
		}
	}
}

func (mr *merchantOrdersRealtime) fetchActiveOrdersUpdatedAt(ctx context.Context, merchantID int64) time.Time {
	query := `
		select coalesce(max(updated_at), now())
		from orders
		where merchant_id = $1 and status in ('PENDING', 'ACCEPTED', 'IN_PROGRESS', 'READY')
	`
	var updated time.Time
	if err := mr.db.QueryRow(ctx, query, merchantID).Scan(&updated); err != nil {
		return time.Time{}
	}
	return updated
}

func (mr *merchantOrdersRealtime) fetchMerchantActiveOrders(ctx context.Context, merchantID int64) ([]handlers.OrderListItem, bool, error) {
	query := `
		select
		  o.id, o.merchant_id, o.customer_id, o.order_number, o.order_type, o.table_number,
		  o.status, o.is_scheduled, o.scheduled_date, o.scheduled_time,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount,
		  o.delivery_distance_km, o.delivery_delivered_at,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.notes, o.admin_note, o.kitchen_notes, o.placed_at, o.updated_at, o.edited_at, o.edited_by_user_id,
		  p.id, p.status, p.payment_method, p.paid_at,
		  r.id, r.party_size, r.reservation_date, r.reservation_time, r.table_number, r.status,
		  c.id, c.name, c.phone, c.email
		from orders o
		left join payments p on p.order_id = o.id
		left join reservations r on r.order_id = o.id
		left join customers c on c.id = o.customer_id
		where o.merchant_id = $1 and o.status in ('PENDING', 'ACCEPTED', 'IN_PROGRESS', 'READY')
		order by o.placed_at desc
	`

	rows, err := mr.db.Query(ctx, query, merchantID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	orders := make([]handlers.OrderListItem, 0)
	for rows.Next() {
		var order handlers.OrderListItem
		var (
			customerID        pgtype.Int8
			customerName      pgtype.Text
			customerPhone     pgtype.Text
			customerEmail     pgtype.Text
			paymentID         pgtype.Int8
			paymentStatus     pgtype.Text
			paymentMethod     pgtype.Text
			paidAt            pgtype.Timestamptz
			reservationID     pgtype.Int8
			reservationParty  pgtype.Int4
			reservationDate   pgtype.Text
			reservationTime   pgtype.Text
			reservationTable  pgtype.Text
			reservationStatus pgtype.Text
			scheduledDate     pgtype.Text
			scheduledTime     pgtype.Text
			deliveryStatus    pgtype.Text
			deliveryUnit      pgtype.Text
			deliveryAddress   pgtype.Text
			notes             pgtype.Text
			adminNote         pgtype.Text
			kitchenNotes      pgtype.Text
			editedAt          pgtype.Timestamptz
			editedBy          pgtype.Int8
			subtotal          pgtype.Numeric
			taxAmount         pgtype.Numeric
			serviceCharge     pgtype.Numeric
			packagingFee      pgtype.Numeric
			discountAmount    pgtype.Numeric
			totalAmount       pgtype.Numeric
			deliveryFee       pgtype.Numeric
			deliveryDistance  pgtype.Numeric
			deliveryDelivered pgtype.Timestamptz
		)

		if err := rows.Scan(
			&order.ID,
			&order.MerchantID,
			&customerID,
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
			&deliveryDelivered,
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
		); err != nil {
			return nil, false, err
		}

		order.Subtotal = utils.NumericToFloat64(subtotal)
		order.TaxAmount = utils.NumericToFloat64(taxAmount)
		order.ServiceChargeAmount = utils.NumericToFloat64(serviceCharge)
		order.PackagingFeeAmount = utils.NumericToFloat64(packagingFee)
		order.DiscountAmount = utils.NumericToFloat64(discountAmount)
		order.TotalAmount = utils.NumericToFloat64(totalAmount)

		if customerID.Valid {
			order.CustomerID = &customerID.Int64
		}
		if customerName.Valid || customerPhone.Valid || customerEmail.Valid {
			custSummary := &handlers.CustomerSummary{}
			if customerID.Valid {
				custSummary.ID = customerID.Int64
			}
			if customerName.Valid {
				custSummary.Name = customerName.String
			}
			if customerPhone.Valid {
				custSummary.Phone = &customerPhone.String
			}
			if customerEmail.Valid {
				custSummary.Email = &customerEmail.String
			}
			order.Customer = custSummary
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
		if deliveryFee.Valid {
			order.DeliveryFeeAmount = utils.NumericToFloat64(deliveryFee)
		}
		if deliveryDistance.Valid {
			dist := utils.NumericToFloat64(deliveryDistance)
			order.DeliveryDistanceKm = &dist
		}
		if deliveryDelivered.Valid {
			order.DeliveryDeliveredAt = &deliveryDelivered.Time
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

		if paymentID.Valid && paymentStatus.Valid {
			pmtSummary := &handlers.PaymentSummary{
				ID:     paymentID.Int64,
				Status: paymentStatus.String,
			}
			if paymentMethod.Valid {
				pmtSummary.PaymentMethod = paymentMethod.String
			}
			if paidAt.Valid {
				pmtSummary.PaidAt = &paidAt.Time
			}
			order.Payment = pmtSummary
		}

		if reservationID.Valid && reservationStatus.Valid {
			resSummary := &handlers.ReservationSummary{
				ID:     reservationID.Int64,
				Status: reservationStatus.String,
			}
			if reservationParty.Valid {
				resSummary.PartySize = reservationParty.Int32
			}
			if reservationDate.Valid {
				resSummary.ReservationDate = reservationDate.String
			}
			if reservationTime.Valid {
				resSummary.ReservationTime = reservationTime.String
			}
			if reservationTable.Valid {
				resSummary.TableNumber = &reservationTable.String
			}
			order.Reservation = resSummary
		}

		orders = append(orders, order)
	}

	return orders, true, nil
}

func (mr *merchantOrdersRealtime) listenLoop(ctx context.Context) {
	backoff := time.Second
	for {
		conn, err := mr.db.Acquire(ctx)
		if err != nil {
			if mr.logger != nil {
				mr.logger.Warn("orders LISTEN acquire failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		_, err = conn.Exec(ctx, `listen orders_updates`)
		if err != nil {
			conn.Release()
			if mr.logger != nil {
				mr.logger.Warn("orders LISTEN failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		backoff = time.Second
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			merchantIDText := strings.TrimSpace(n.Payload)
			if merchantIDText == "" {
				continue
			}

			merchantID, parseErr := parseInt64(merchantIDText)
			if parseErr != nil {
				updatedAt := time.Now()
				mr.broadcast(merchantIDText, map[string]any{"type": "orders.refresh", "updatedAt": updatedAt})
				continue
			}

			orders, found, fetchErr := mr.fetchMerchantActiveOrders(ctx, merchantID)
			if fetchErr != nil || !found {
				updatedAt := mr.fetchActiveOrdersUpdatedAt(ctx, merchantID)
				mr.broadcast(merchantIDText, map[string]any{"type": "orders.refresh", "updatedAt": updatedAt})
				continue
			}

			mr.broadcast(merchantIDText, map[string]any{"type": "orders.state", "data": orders})
			// Backward-compat for older clients
			updatedAt := time.Now()
			if len(orders) > 0 {
				updatedAt = orders[0].UpdatedAt
			}
			mr.broadcast(merchantIDText, map[string]any{"type": "orders.refresh", "updatedAt": updatedAt})
		}

		conn.Release()
		time.Sleep(backoff)
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

type customerDisplayRealtime struct {
	db     *pgxpool.Pool
	logger *zap.Logger

	started sync.Once
	mu      sync.RWMutex
	subs    map[string]map[*wsRealtimeClient]struct{}
}

func newCustomerDisplayRealtime(db *pgxpool.Pool, logger *zap.Logger) *customerDisplayRealtime {
	return &customerDisplayRealtime{
		db:     db,
		logger: logger,
		subs:   make(map[string]map[*wsRealtimeClient]struct{}),
	}
}

func (cr *customerDisplayRealtime) ensureStarted() {
	cr.started.Do(func() {
		go cr.listenLoop(context.Background())
	})
}

func (cr *customerDisplayRealtime) subscribe(merchantID string, client *wsRealtimeClient) (unsubscribe func()) {
	key := strings.TrimSpace(merchantID)
	if key == "" {
		return func() {}
	}

	cr.mu.Lock()
	if cr.subs[key] == nil {
		cr.subs[key] = make(map[*wsRealtimeClient]struct{})
	}
	cr.subs[key][client] = struct{}{}
	cr.mu.Unlock()

	return func() {
		cr.mu.Lock()
		clients := cr.subs[key]
		delete(clients, client)
		if len(clients) == 0 {
			delete(cr.subs, key)
		}
		cr.mu.Unlock()
	}
}

func (cr *customerDisplayRealtime) broadcast(merchantID string, message any) {
	key := strings.TrimSpace(merchantID)
	if key == "" {
		return
	}

	cr.mu.RLock()
	clientsMap := cr.subs[key]
	clients := make([]*wsRealtimeClient, 0, len(clientsMap))
	for c := range clientsMap {
		clients = append(clients, c)
	}
	cr.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	for _, c := range clients {
		if err := c.writeJSON(message); err != nil {
			_ = c.conn.Close()
			cr.mu.Lock()
			if current := cr.subs[key]; current != nil {
				delete(current, c)
				if len(current) == 0 {
					delete(cr.subs, key)
				}
			}
			cr.mu.Unlock()
		}
	}
}

func (cr *customerDisplayRealtime) fetchCustomerDisplayState(ctx context.Context, merchantID int64) (map[string]any, bool, error) {
	query := `
		select mode::text, is_locked, payload, updated_at
		from customer_display_state
		where merchant_id = $1
	`

	var mode string
	var isLocked bool
	var payloadBytes []byte
	var updatedAt time.Time
	if err := cr.db.QueryRow(ctx, query, merchantID).Scan(&mode, &isLocked, &payloadBytes, &updatedAt); err != nil {
		return nil, false, err
	}

	var payload any
	if len(payloadBytes) > 0 {
		_ = json.Unmarshal(payloadBytes, &payload)
	}

	return map[string]any{
		"mode":      mode,
		"payload":   payload,
		"isLocked":  isLocked,
		"updatedAt": updatedAt,
	}, true, nil
}

func (cr *customerDisplayRealtime) listenLoop(ctx context.Context) {
	backoff := time.Second
	for {
		conn, err := cr.db.Acquire(ctx)
		if err != nil {
			if cr.logger != nil {
				cr.logger.Warn("customer-display LISTEN acquire failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		_, err = conn.Exec(ctx, `listen customer_display_updates`)
		if err != nil {
			conn.Release()
			if cr.logger != nil {
				cr.logger.Warn("customer-display LISTEN failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		backoff = time.Second
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			merchantIDText := strings.TrimSpace(n.Payload)
			if merchantIDText == "" {
				continue
			}

			merchantID, parseErr := parseInt64(merchantIDText)
			if parseErr != nil {
				cr.broadcast(merchantIDText, map[string]any{"type": "customer-display.refresh"})
				continue
			}

			state, ok, fetchErr := cr.fetchCustomerDisplayState(ctx, merchantID)
			if fetchErr != nil || !ok {
				cr.broadcast(merchantIDText, map[string]any{"type": "customer-display.refresh"})
				continue
			}

			cr.broadcast(merchantIDText, map[string]any{"type": "customer-display.state", "data": state})
		}

		conn.Release()
		time.Sleep(backoff)
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

type publicOrderRealtime struct {
	db     *pgxpool.Pool
	logger *zap.Logger

	started sync.Once
	mu      sync.RWMutex
	subs    map[string]map[*wsRealtimeClient]struct{}
}

func newPublicOrderRealtime(db *pgxpool.Pool, logger *zap.Logger) *publicOrderRealtime {
	return &publicOrderRealtime{
		db:     db,
		logger: logger,
		subs:   make(map[string]map[*wsRealtimeClient]struct{}),
	}
}

func (pr *publicOrderRealtime) ensureStarted() {
	pr.started.Do(func() {
		go pr.listenLoop(context.Background())
	})
}

func (pr *publicOrderRealtime) subscribe(orderNumber string, client *wsRealtimeClient) (unsubscribe func()) {
	key := strings.TrimSpace(orderNumber)
	if key == "" {
		return func() {}
	}

	pr.mu.Lock()
	if pr.subs[key] == nil {
		pr.subs[key] = make(map[*wsRealtimeClient]struct{})
	}
	pr.subs[key][client] = struct{}{}
	pr.mu.Unlock()

	return func() {
		pr.mu.Lock()
		clients := pr.subs[key]
		delete(clients, client)
		if len(clients) == 0 {
			delete(pr.subs, key)
		}
		pr.mu.Unlock()
	}
}

func (pr *publicOrderRealtime) broadcast(orderNumber string, message any) {
	key := strings.TrimSpace(orderNumber)
	if key == "" {
		return
	}

	pr.mu.RLock()
	clientsMap := pr.subs[key]
	clients := make([]*wsRealtimeClient, 0, len(clientsMap))
	for c := range clientsMap {
		clients = append(clients, c)
	}
	pr.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	for _, c := range clients {
		if err := c.writeJSON(message); err != nil {
			_ = c.conn.Close()
			pr.mu.Lock()
			if current := pr.subs[key]; current != nil {
				delete(current, c)
				if len(current) == 0 {
					delete(pr.subs, key)
				}
			}
			pr.mu.Unlock()
		}
	}
}

func (pr *publicOrderRealtime) fetchOrderStatus(ctx context.Context, orderNumber string) (string, time.Time) {
	query := `select status, updated_at from orders where order_number = $1`
	var status string
	var updated time.Time
	if err := pr.db.QueryRow(ctx, query, orderNumber).Scan(&status, &updated); err != nil {
		return "", time.Time{}
	}
	return status, updated
}

func (pr *publicOrderRealtime) fetchOrderDetail(ctx context.Context, orderNumber string) (handlers.OrderDetail, bool, error) {
	query := `
		select
		  o.id, o.order_number, o.status, o.order_type, o.table_number,
		  o.subtotal, o.tax_amount, o.service_charge_amount, o.packaging_fee, o.discount_amount, o.total_amount,
		  o.created_at, o.updated_at, o.placed_at, o.completed_at,
		  o.delivery_status, o.delivery_unit, o.delivery_address, o.delivery_fee_amount, o.delivery_distance_km, o.delivery_delivered_at,
		  o.edited_at,
		  m.name, m.currency, m.code,
		  c.name,
		  p.status, p.payment_method, p.amount, p.paid_at, p.customer_paid_at, p.customer_proof_url, p.customer_proof_uploaded_at, p.customer_payment_note, p.customer_proof_meta,
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
		detail           handlers.OrderDetail
		merchantName     string
		merchantCurrency string
		merchantCode     string
		customerName     pgtype.Text
		pStatus          pgtype.Text
		pMethod          pgtype.Text
		pAmount          pgtype.Numeric
		pPaidAt          pgtype.Timestamptz
		pCustomerPaidAt  pgtype.Timestamptz
		pProofUrl        pgtype.Text
		pProofUploadedAt pgtype.Timestamptz
		pPaymentNote     pgtype.Text
		pProofMeta       []byte
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

	if err := pr.db.QueryRow(ctx, query, orderNumber).Scan(
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
		&pCustomerPaidAt,
		&pProofUrl,
		&pProofUploadedAt,
		&pPaymentNote,
		&pProofMeta,
		&rStatus,
		&rParty,
		&rDate,
		&rTime,
		&rTable,
	); err != nil {
		return handlers.OrderDetail{}, false, err
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
		var customerPaidAtPtr *time.Time
		if pCustomerPaidAt.Valid {
			value := pCustomerPaidAt.Time
			customerPaidAtPtr = &value
		}
		var proofUploadedAtPtr *time.Time
		if pProofUploadedAt.Valid {
			value := pProofUploadedAt.Time
			proofUploadedAtPtr = &value
		}
		var proofUrlPtr *string
		if pProofUrl.Valid {
			value := pProofUrl.String
			proofUrlPtr = &value
		}
		var paymentNotePtr *string
		if pPaymentNote.Valid {
			value := pPaymentNote.String
			paymentNotePtr = &value
		}
		var proofMeta map[string]any
		if len(pProofMeta) > 0 {
			var parsed map[string]any
			if err := json.Unmarshal(pProofMeta, &parsed); err == nil {
				proofMeta = parsed
			}
		}
		detail.Payment = &struct {
			Status                  *string        `json:"status"`
			PaymentMethod           *string        `json:"paymentMethod"`
			Amount                  *float64       `json:"amount"`
			PaidAt                  *time.Time     `json:"paidAt"`
			CustomerPaidAt          *time.Time     `json:"customerPaidAt"`
			CustomerProofUrl        *string        `json:"customerProofUrl"`
			CustomerProofUploadedAt *time.Time     `json:"customerProofUploadedAt"`
			CustomerPaymentNote     *string        `json:"customerPaymentNote"`
			CustomerProofMeta       map[string]any `json:"customerProofMeta"`
		}{
			Status:                  &status,
			PaymentMethod:           &method,
			Amount:                  &amount,
			PaidAt:                  paidAtPtr,
			CustomerPaidAt:          customerPaidAtPtr,
			CustomerProofUrl:        proofUrlPtr,
			CustomerProofUploadedAt: proofUploadedAtPtr,
			CustomerPaymentNote:     paymentNotePtr,
			CustomerProofMeta:       proofMeta,
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

	items, err := pr.fetchOrderItems(ctx, detail.ID)
	if err != nil {
		return handlers.OrderDetail{}, false, err
	}
	detail.OrderItems = items

	return detail, true, nil
}

func (pr *publicOrderRealtime) fetchOrderItems(ctx context.Context, orderID int64) ([]handlers.OrderItem, error) {
	query := `
		select id, menu_name, menu_price, quantity, subtotal, notes
		from order_items
		where order_id = $1
		order by id asc
	`

	rows, err := pr.db.Query(ctx, query, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]handlers.OrderItem, 0)
	itemIDs := make([]int64, 0)
	for rows.Next() {
		var (
			item      handlers.OrderItem
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

	addonsByItem, err := pr.fetchOrderItemAddons(ctx, itemIDs)
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

func (pr *publicOrderRealtime) fetchOrderItemAddons(ctx context.Context, itemIDs []int64) (map[int64][]handlers.OrderItemAddon, error) {
	query := `
		select order_item_id, id, addon_name, addon_price, quantity
		from order_item_addons
		where order_item_id = any($1)
	`

	rows, err := pr.db.Query(ctx, query, itemIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	addons := make(map[int64][]handlers.OrderItemAddon)
	for rows.Next() {
		var (
			itemID int64
			addon  handlers.OrderItemAddon
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

func (pr *publicOrderRealtime) listenLoop(ctx context.Context) {
	backoff := time.Second
	for {
		conn, err := pr.db.Acquire(ctx)
		if err != nil {
			if pr.logger != nil {
				pr.logger.Warn("public-order LISTEN acquire failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		_, err = conn.Exec(ctx, `listen public_order_updates`)
		if err != nil {
			conn.Release()
			if pr.logger != nil {
				pr.logger.Warn("public-order LISTEN failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		backoff = time.Second
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			orderNumber := strings.TrimSpace(n.Payload)
			if orderNumber == "" {
				continue
			}

			detail, found, detailErr := pr.fetchOrderDetail(ctx, orderNumber)
			if detailErr == nil && found {
				pr.broadcast(orderNumber, map[string]any{
					"type": "order.state",
					"data": detail,
				})
				// Backward-compat for older clients that still refetch on refresh events.
				pr.broadcast(orderNumber, map[string]any{
					"type":      "order.refresh",
					"status":    detail.Status,
					"updatedAt": detail.UpdatedAt,
				})
				continue
			}

			status, updatedAt := pr.fetchOrderStatus(ctx, orderNumber)
			pr.broadcast(orderNumber, map[string]any{
				"type":      "order.refresh",
				"status":    status,
				"updatedAt": updatedAt,
			})
		}

		conn.Release()
		time.Sleep(backoff)
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

type wsGroupOrderClient struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *wsGroupOrderClient) writeJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(value)
}

type groupOrderRealtime struct {
	db     *pgxpool.Pool
	logger *zap.Logger

	started sync.Once
	mu      sync.RWMutex
	subs    map[string]map[*wsGroupOrderClient]struct{}
}

func newGroupOrderRealtime(db *pgxpool.Pool, logger *zap.Logger) *groupOrderRealtime {
	return &groupOrderRealtime{
		db:     db,
		logger: logger,
		subs:   make(map[string]map[*wsGroupOrderClient]struct{}),
	}
}

func (gr *groupOrderRealtime) ensureStarted() {
	gr.started.Do(func() {
		go gr.listenLoop(context.Background())
	})
}

func (gr *groupOrderRealtime) subscribe(code string, client *wsGroupOrderClient) (unsubscribe func()) {
	sessionCode := strings.ToUpper(strings.TrimSpace(code))
	if sessionCode == "" {
		return func() {}
	}

	gr.mu.Lock()
	if gr.subs[sessionCode] == nil {
		gr.subs[sessionCode] = make(map[*wsGroupOrderClient]struct{})
	}
	gr.subs[sessionCode][client] = struct{}{}
	gr.mu.Unlock()

	return func() {
		gr.mu.Lock()
		clients := gr.subs[sessionCode]
		delete(clients, client)
		if len(clients) == 0 {
			delete(gr.subs, sessionCode)
		}
		gr.mu.Unlock()
	}
}

func (gr *groupOrderRealtime) broadcast(code string, message any) {
	sessionCode := strings.ToUpper(strings.TrimSpace(code))
	if sessionCode == "" {
		return
	}

	gr.mu.RLock()
	clientsMap := gr.subs[sessionCode]
	clients := make([]*wsGroupOrderClient, 0, len(clientsMap))
	for c := range clientsMap {
		clients = append(clients, c)
	}
	gr.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	for _, c := range clients {
		if err := c.writeJSON(message); err != nil {
			_ = c.conn.Close()
			gr.mu.Lock()
			if current := gr.subs[sessionCode]; current != nil {
				delete(current, c)
				if len(current) == 0 {
					delete(gr.subs, sessionCode)
				}
			}
			gr.mu.Unlock()
		}
	}
}

func (gr *groupOrderRealtime) listenLoop(ctx context.Context) {
	backoff := time.Second
	for {
		conn, err := gr.db.Acquire(ctx)
		if err != nil {
			if gr.logger != nil {
				gr.logger.Warn("group-order LISTEN acquire failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		_, err = conn.Exec(ctx, `listen group_order_updates`)
		if err != nil {
			conn.Release()
			if gr.logger != nil {
				gr.logger.Warn("group-order LISTEN failed", zap.Error(err))
			}
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		backoff = time.Second
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			code := strings.TrimSpace(n.Payload)
			if code == "" {
				continue
			}

			payload, status, found, fetchErr := handlers.FetchGroupOrderSessionPayloadByCode(ctx, gr.db, code)
			if fetchErr != nil {
				gr.broadcast(code, map[string]any{"type": "error", "message": "failed to load session"})
				continue
			}
			if !found {
				gr.broadcast(code, map[string]any{"type": "group-order.closed", "status": status})
				continue
			}
			if status != "OPEN" && status != "LOCKED" {
				gr.broadcast(code, map[string]any{"type": "group-order.closed", "status": status, "data": payload})
				continue
			}

			gr.broadcast(code, map[string]any{"type": "group-order.session", "data": payload, "status": status})
		}

		conn.Release()
		time.Sleep(backoff)
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (s *Server) MerchantOrdersWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	token := auth.ParseBearerToken(r.URL.Query().Get("token"))
	claims, err := auth.VerifyAccessToken(token, s.Config.JWTSecret)
	if err != nil || claims.MerchantID == nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "unauthorized"})
		return
	}

	merchantID, err := parseInt64(*claims.MerchantID)
	if err != nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "unauthorized"})
		return
	}

	s.merchantOrdersRealtime.ensureStarted()
	ctx := r.Context()
	client := &wsRealtimeClient{conn: conn}
	unsubscribe := s.merchantOrdersRealtime.subscribe(fmt.Sprint(merchantID), client)
	defer unsubscribe()

	// Send initial orders snapshot immediately
	if orders, found, fetchErr := s.merchantOrdersRealtime.fetchMerchantActiveOrders(ctx, merchantID); fetchErr == nil && found {
		_ = client.writeJSON(map[string]any{"type": "orders.state", "data": orders})
		updatedAt := time.Now()
		if len(orders) > 0 {
			updatedAt = orders[0].UpdatedAt
		}
		_ = client.writeJSON(map[string]any{"type": "orders.refresh", "updatedAt": updatedAt})
	} else {
		updatedAt := s.merchantOrdersRealtime.fetchActiveOrdersUpdatedAt(ctx, merchantID)
		_ = client.writeJSON(map[string]any{"type": "orders.refresh", "updatedAt": updatedAt})
	}

	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}()

	select {
	case <-clientClosed:
		return
	case <-ctx.Done():
		return
	}
}

func (s *Server) MerchantCustomerDisplayWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	token := auth.ParseBearerToken(r.URL.Query().Get("token"))
	claims, err := auth.VerifyAccessToken(token, s.Config.JWTSecret)
	if err != nil || claims.MerchantID == nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "unauthorized"})
		return
	}

	merchantID, err := parseInt64(*claims.MerchantID)
	if err != nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "unauthorized"})
		return
	}

	s.customerDisplayRealtime.ensureStarted()
	ctx := r.Context()
	client := &wsRealtimeClient{conn: conn}
	unsubscribe := s.customerDisplayRealtime.subscribe(fmt.Sprint(merchantID), client)
	defer unsubscribe()

	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}()

	select {
	case <-clientClosed:
		return
	case <-ctx.Done():
		return
	}
}

func (s *Server) PublicOrderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	orderNumber := r.URL.Query().Get("orderNumber")
	token := r.URL.Query().Get("token")
	if orderNumber == "" || token == "" {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "invalid request"})
		return
	}

	merchantCode, ok := s.getMerchantCodeForOrder(r.Context(), orderNumber)
	if !ok || !utils.VerifyOrderTrackingToken(s.Config.OrderTrackingTokenSecret, token, merchantCode, orderNumber) {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "order not found"})
		return
	}

	s.publicOrderRealtime.ensureStarted()
	ctx := r.Context()
	client := &wsRealtimeClient{conn: conn}
	unsubscribe := s.publicOrderRealtime.subscribe(orderNumber, client)
	defer unsubscribe()

	// Send initial full order snapshot immediately.
	if detail, found, detailErr := s.publicOrderRealtime.fetchOrderDetail(ctx, orderNumber); detailErr == nil && found {
		_ = client.writeJSON(map[string]any{"type": "order.state", "data": detail})
		_ = client.writeJSON(map[string]any{"type": "order.refresh", "status": detail.Status, "updatedAt": detail.UpdatedAt})
	} else {
		status, updatedAt := s.publicOrderRealtime.fetchOrderStatus(ctx, orderNumber)
		_ = client.writeJSON(map[string]any{"type": "order.refresh", "status": status, "updatedAt": updatedAt})
	}

	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}()

	select {
	case <-clientClosed:
		return
	case <-ctx.Done():
		return
	}
}

func (s *Server) PublicGroupOrderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	code := r.URL.Query().Get("code")
	if code == "" {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "invalid request"})
		return
	}

	s.groupOrderRealtime.ensureStarted()
	ctx := r.Context()
	client := &wsGroupOrderClient{conn: conn}
	unsubscribe := s.groupOrderRealtime.subscribe(code, client)
	defer unsubscribe()

	// Send initial full session snapshot immediately.
	payload, status, found, fetchErr := handlers.FetchGroupOrderSessionPayloadByCode(ctx, s.DB, code)
	if fetchErr != nil {
		_ = client.writeJSON(map[string]any{"type": "error", "message": "failed to load session"})
		return
	}
	if !found {
		_ = client.writeJSON(map[string]any{"type": "group-order.closed", "status": status})
		return
	}
	if status != "OPEN" && status != "LOCKED" {
		_ = client.writeJSON(map[string]any{"type": "group-order.closed", "status": status, "data": payload})
		return
	}
	_ = client.writeJSON(map[string]any{"type": "group-order.session", "data": payload, "status": status})

	var expiresTimer *time.Timer
	if expiresAtValue, ok := payload["expiresAt"].(time.Time); ok {
		until := time.Until(expiresAtValue)
		if until > 0 {
			expiresTimer = time.NewTimer(until + time.Second)
			defer expiresTimer.Stop()
		}
	}

	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}()

	select {
	case <-clientClosed:
		return
	case <-ctx.Done():
		return
	case <-func() <-chan time.Time {
		if expiresTimer == nil {
			return nil
		}
		return expiresTimer.C
	}():
		_, _ = s.DB.Exec(ctx, `
			update group_order_sessions
			set status = 'EXPIRED'
			where session_code = $1 and status = 'OPEN' and expires_at <= now()
		`, strings.ToUpper(code))
		_, _ = s.DB.Exec(ctx, `select pg_notify('group_order_updates', $1)`, strings.ToUpper(code))
		_ = client.writeJSON(map[string]any{"type": "group-order.closed", "status": "EXPIRED"})
		return
	}
}

func (s *Server) fetchActiveOrdersUpdatedAt(ctx context.Context, merchantID int64) time.Time {
	query := `
		select coalesce(max(updated_at), now())
		from orders
		where merchant_id = $1 and status in ('PENDING', 'ACCEPTED', 'IN_PROGRESS', 'READY')
	`
	var updated time.Time
	if err := s.DB.QueryRow(ctx, query, merchantID).Scan(&updated); err != nil {
		return time.Time{}
	}
	return updated
}

func (s *Server) getMerchantCodeForOrder(ctx context.Context, orderNumber string) (string, bool) {
	var code string
	query := `select m.code from orders o join merchants m on m.id = o.merchant_id where o.order_number = $1`
	if err := s.DB.QueryRow(ctx, query, orderNumber).Scan(&code); err != nil {
		return "", false
	}
	return code, true
}

func (s *Server) fetchOrderStatus(ctx context.Context, orderNumber string) (string, time.Time) {
	query := `select status, updated_at from orders where order_number = $1`
	var status string
	var updated time.Time
	if err := s.DB.QueryRow(ctx, query, orderNumber).Scan(&status, &updated); err != nil {
		return "", time.Time{}
	}
	return status, updated
}

func (s *Server) fetchGroupOrderUpdatedAt(ctx context.Context, code string) time.Time {
	query := `
		select greatest(
			coalesce(max(p.updated_at), '1970-01-01'::timestamptz),
			coalesce(s.updated_at, '1970-01-01'::timestamptz)
		)
		from group_order_sessions s
		left join group_order_participants p on p.session_id = s.id
		where s.session_code = $1 and s.status in ('OPEN', 'LOCKED', 'SUBMITTED')
		group by s.updated_at
	`
	var updated time.Time
	if err := s.DB.QueryRow(ctx, query, strings.ToUpper(code)).Scan(&updated); err != nil {
		return time.Time{}
	}
	return updated
}

func (s *Server) fetchGroupOrderStatusAndUpdatedAt(ctx context.Context, code string) (string, time.Time, bool) {
	query := `
		select
			s.status,
			greatest(
				coalesce(max(p.updated_at), '1970-01-01'::timestamptz),
				coalesce(s.updated_at, '1970-01-01'::timestamptz)
			)
		from group_order_sessions s
		left join group_order_participants p on p.session_id = s.id
		where s.session_code = $1
		group by s.status, s.updated_at
	`
	var status string
	var updated time.Time
	if err := s.DB.QueryRow(ctx, query, strings.ToUpper(code)).Scan(&status, &updated); err != nil {
		return "", time.Time{}, false
	}
	return status, updated, true
}

func (s *Server) fetchCustomerDisplayUpdatedAt(ctx context.Context, merchantID int64) time.Time {
	query := `select updated_at from customer_display_state where merchant_id = $1`
	var updated time.Time
	if err := s.DB.QueryRow(ctx, query, merchantID).Scan(&updated); err != nil {
		return time.Time{}
	}
	return updated
}

func parseInt64(value string) (int64, error) {
	var out int64
	_, err := fmt.Sscan(value, &out)
	return out, err
}
