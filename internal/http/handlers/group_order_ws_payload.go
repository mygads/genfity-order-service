package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"genfity-order-services/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FetchGroupOrderSessionPayloadByCode loads the full group order session payload (same shape as the HTTP API)
// for use by WebSocket broadcasters.
//
// Returns: payload, status, found, error
func FetchGroupOrderSessionPayloadByCode(
	ctx context.Context,
	db *pgxpool.Pool,
	code string,
) (map[string]any, string, bool, error) {
	sessionCode := strings.ToUpper(strings.TrimSpace(code))
	if sessionCode == "" {
		return nil, "", false, nil
	}

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
		limit 1
	`

	var (
		session           GroupOrderSession
		merchantTax       pgtype.Numeric
		merchantService   pgtype.Numeric
		merchantPackaging pgtype.Numeric
		tableNumber       pgtype.Text
		sessionOrderID    pgtype.Int8
		orderRowID        pgtype.Int8
		orderNumber       pgtype.Text
		orderStatus       pgtype.Text
		orderTotal        pgtype.Numeric
	)

	if err := db.QueryRow(ctx, query, sessionCode).Scan(
		&session.ID,
		&session.SessionCode,
		&session.OrderType,
		&tableNumber,
		&session.Status,
		&session.MerchantID,
		&sessionOrderID,
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
		&orderRowID,
		&orderNumber,
		&orderStatus,
		&orderTotal,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}

	if tableNumber.Valid {
		value := tableNumber.String
		session.TableNumber = &value
	}

	// Expiry enforcement (mirrors HTTP behavior)
	if session.ExpiresAt.Before(time.Now()) && session.Status == "OPEN" {
		_, _ = db.Exec(ctx, "update group_order_sessions set status = 'EXPIRED' where id = $1", session.ID)
		session.Status = "EXPIRED"
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

	if sessionOrderID.Valid {
		val := 0.0
		if orderTotal.Valid {
			val = utils.NumericToFloat64(orderTotal)
		}
		session.OrderID = &sessionOrderID.Int64
		session.Order = &struct {
			ID          int64   `json:"id"`
			OrderNumber string  `json:"orderNumber"`
			Status      string  `json:"status"`
			TotalAmount float64 `json:"totalAmount"`
		}{
			ID:          sessionOrderID.Int64,
			OrderNumber: orderNumber.String,
			Status:      orderStatus.String,
			TotalAmount: val,
		}
	}

	participants, err := fetchGroupOrderParticipantsBySessionID(ctx, db, session.ID)
	if err != nil {
		return nil, session.Status, false, err
	}

	payload := buildGroupOrderSessionPayload(session, participants)
	return payload, session.Status, true, nil
}

func fetchGroupOrderParticipantsBySessionID(ctx context.Context, db *pgxpool.Pool, sessionID int64) ([]GroupOrderParticipant, error) {
	query := `
		select p.id, p.customer_id, p.name, p.device_id, p.is_host, p.cart_items, p.subtotal, p.joined_at, p.updated_at,
		       c.name, c.phone
		from group_order_participants p
		left join customers c on c.id = p.customer_id
		where p.session_id = $1
		order by p.joined_at asc
	`

	rows, err := db.Query(ctx, query, sessionID)
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

		p.CartItems = json.RawMessage(cartItems)
		p.Subtotal = utils.NumericToFloat64(subtotal)

		if customerID.Valid {
			p.CustomerID = &customerID.Int64
			p.Customer = &CustomerSummary{ID: customerID.Int64, Name: customerName.String}
			if customerPhone.Valid {
				p.Customer.Phone = &customerPhone.String
			}
		}

		participants = append(participants, p)
	}

	return participants, nil
}
