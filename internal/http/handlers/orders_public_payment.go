package handlers

import (
	"encoding/json"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicOrderConfirmPayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	var body struct {
		Note string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	note := strings.TrimSpace(body.Note)

	var (
		merchantID    pgtype.Int8
		merchantCode  pgtype.Text
		paymentID     pgtype.Int8
		paymentMethod pgtype.Text
		existingNote  pgtype.Text
		proofURL      pgtype.Text
	)

	if err := h.DB.QueryRow(ctx, `
		select m.id, m.code, p.id, p.payment_method, p.customer_payment_note, p.customer_proof_url
		from orders o
		join merchants m on m.id = o.merchant_id
		left join payments p on p.order_id = o.id
		where o.order_number = $1
		limit 1
	`, orderNumber).Scan(
		&merchantID,
		&merchantCode,
		&paymentID,
		&paymentMethod,
		&existingNote,
		&proofURL,
	); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !merchantCode.Valid || !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode.String, orderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !paymentID.Valid || !paymentMethod.Valid {
		response.Error(w, http.StatusNotFound, "PAYMENT_NOT_FOUND", "Payment record not found")
		return
	}

	method := strings.ToUpper(paymentMethod.String)
	if method != "MANUAL_TRANSFER" && method != "QRIS" && method != "ONLINE" {
		response.Error(w, http.StatusBadRequest, "PAYMENT_METHOD_NOT_SUPPORTED", "This payment method does not require confirmation")
		return
	}

	if merchantID.Valid {
		settings, _, err := h.fetchPublicPaymentConfig(ctx, merchantID.Int64)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to confirm payment")
			return
		}
		if settings.RequirePaymentProof && (!proofURL.Valid || strings.TrimSpace(proofURL.String) == "") {
			response.Error(w, http.StatusBadRequest, "PAYMENT_PROOF_REQUIRED", "Payment proof is required")
			return
		}
	}

	finalNote := ""
	if existingNote.Valid {
		finalNote = strings.TrimSpace(existingNote.String)
	}
	if note != "" {
		if finalNote != "" {
			finalNote = finalNote + " | " + note
		} else {
			finalNote = note
		}
	}

	var updatedPaidAt time.Time
	var updatedNote pgtype.Text
	if err := h.DB.QueryRow(ctx, `
		update payments
		set customer_paid_at = $1, customer_payment_note = $2
		where id = $3
		returning customer_paid_at, customer_payment_note
	`, time.Now(), nullIfEmpty(finalNote), paymentID.Int64).Scan(&updatedPaidAt, &updatedNote); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to confirm payment")
		return
	}

	var notePtr *string
	if updatedNote.Valid {
		value := updatedNote.String
		notePtr = &value
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"customerPaidAt":      updatedPaidAt,
			"customerPaymentNote": notePtr,
		},
		"message": "Payment confirmation saved",
	})
}
