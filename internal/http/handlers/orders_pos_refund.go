package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"golang.org/x/crypto/bcrypt"
)

type posRefundRequest struct {
	OrderID   any    `json:"orderId"`
	DeletePin string `json:"deletePin"`
	Reason    string `json:"reason"`
}

func (h *Handler) MerchantPOSRefund(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body posRefundRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	orderID, ok := parseNumericID(body.OrderID)
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	pin := strings.TrimSpace(body.DeletePin)
	if pin == "" {
		response.Error(w, http.StatusBadRequest, "PIN_REQUIRED", "Delete PIN is required to refund/void an order")
		return
	}

	var merchantPin pgtype.Text
	if err := h.DB.QueryRow(ctx, `select delete_pin from merchants where id = $1`, *authCtx.MerchantID).Scan(&merchantPin); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if !merchantPin.Valid || merchantPin.String == "" {
		response.Error(w, http.StatusBadRequest, "PIN_NOT_SET", "Delete PIN is not configured for this merchant")
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(merchantPin.String), []byte(pin)) != nil {
		response.Error(w, http.StatusUnauthorized, "INVALID_PIN", "Invalid PIN")
		return
	}

	var (
		orderStatus     pgtype.Text
		paymentStatus   pgtype.Text
		paymentID       pgtype.Int8
		paymentMetadata []byte
	)
	rows, err := h.DB.Query(ctx, `
		select o.status, p.id, p.status, p.metadata
		from orders o
		left join payments p on p.order_id = o.id
		where o.id = $1 and o.merchant_id = $2
	`, orderID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found or does not belong to this merchant")
		return
	}
	defer rows.Close()
	if !rows.Next() {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found or does not belong to this merchant")
		return
	}
	if err := rows.Scan(&orderStatus, &paymentID, &paymentStatus, &paymentMetadata); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to refund/void order")
		return
	}

	alreadyCancelled := orderStatus.Valid && orderStatus.String == "CANCELLED"
	alreadyRefunded := paymentStatus.Valid && paymentStatus.String == "REFUNDED"
	finalPaymentStatus := paymentStatus.String

	refundReason := strings.TrimSpace(body.Reason)
	refundedAt := time.Now().Format(time.RFC3339)
	refundedBy := int64(0)
	if authCtx.UserID != 0 {
		refundedBy = authCtx.UserID
	}

	if err := withRefundTx(ctx, h.DB, func(ctx context.Context, tx refundTx) error {
		if !alreadyCancelled {
			if _, err := tx.Exec(ctx, `update orders set status = 'CANCELLED' where id = $1`, orderID); err != nil {
				return err
			}
		}

		if paymentID.Valid && !alreadyRefunded {
			nextStatus := paymentStatus.String
			if paymentStatus.Valid {
				switch paymentStatus.String {
				case "COMPLETED":
					nextStatus = "REFUNDED"
				case "PENDING", "FAILED":
					nextStatus = "CANCELLED"
				}
			}
			finalPaymentStatus = nextStatus

			metadata := map[string]any{}
			if len(paymentMetadata) > 0 {
				_ = json.Unmarshal(paymentMetadata, &metadata)
			}
			metadata["refundedByUserId"] = refundedBy
			metadata["refundedAt"] = refundedAt
			if refundReason != "" {
				metadata["refundReason"] = refundReason
			}
			if _, ok := metadata["source"]; !ok {
				metadata["source"] = "POS"
			}

			if _, err := tx.Exec(ctx, `update payments set status = $1, metadata = $2 where id = $3`, nextStatus, metadata, paymentID.Int64); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to refund/void order")
		return
	}

	// Restore stock (best-effort)
	stockRows, _ := h.DB.Query(ctx, `
		select oi.menu_id, oi.quantity
		from order_items oi
		where oi.order_id = $1
	`, orderID)
	if stockRows != nil {
		defer stockRows.Close()
		for stockRows.Next() {
			var menuID int64
			var qty int32
			if err := stockRows.Scan(&menuID, &qty); err != nil {
				continue
			}
			var trackStock bool
			var stockQty pgtype.Int4
			if err := h.DB.QueryRow(ctx, `select track_stock, stock_qty from menus where id = $1`, menuID).Scan(&trackStock, &stockQty); err != nil {
				continue
			}
			if trackStock && stockQty.Valid {
				_, _ = h.DB.Exec(ctx, `update menus set stock_qty = $1, is_active = true where id = $2`, stockQty.Int32+qty, menuID)
			}
		}
	}

	message := "Order refunded/voided successfully"
	if alreadyCancelled && (alreadyRefunded || !paymentID.Valid) {
		message = "Order already voided"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"order": map[string]any{
				"id":     orderID,
				"status": "CANCELLED",
			},
			"payment": map[string]any{
				"id":     paymentID.Int64,
				"status": finalPaymentStatus,
			},
		},
		"message":    message,
		"statusCode": 200,
	})
}

type refundTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
}

type dbBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func withRefundTx(ctx context.Context, db dbBeginner, fn func(ctx context.Context, tx refundTx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
