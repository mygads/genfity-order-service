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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type posPaymentRequest struct {
	OrderID           any      `json:"orderId"`
	PaymentMethod     string   `json:"paymentMethod"`
	AmountPaid        *float64 `json:"amountPaid"`
	ChangeAmount      *float64 `json:"changeAmount"`
	Notes             string   `json:"notes"`
	CashAmount        *float64 `json:"cashAmount"`
	CardAmount        *float64 `json:"cardAmount"`
	DiscountType      string   `json:"discountType"`
	DiscountValue     *float64 `json:"discountValue"`
	DiscountAmount    *float64 `json:"discountAmount"`
	FinalTotal        *float64 `json:"finalTotal"`
	VoucherCode       string   `json:"voucherCode"`
	VoucherTemplateID any      `json:"voucherTemplateId"`
}

func (h *Handler) MerchantPOSPayment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var body posPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	orderID, ok := parseNumericID(body.OrderID)
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	paymentMethod := strings.TrimSpace(body.PaymentMethod)
	if paymentMethod == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Payment method is required")
		return
	}
	validMethods := map[string]struct{}{
		"CASH_ON_COUNTER": {},
		"CARD_ON_COUNTER": {},
		"SPLIT":           {},
	}
	if _, ok := validMethods[paymentMethod]; !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payment method")
		return
	}

	order, items, existingPayment, err := h.loadPOSPaymentOrder(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found or does not belong to this merchant")
		return
	}

	merchantCurrency, merchantTimezone, err := h.getMerchantCurrencyTimezone(ctx, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("pos payment merchant lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load merchant settings")
		return
	}
	orderTotalBeforeDiscount := order.TotalAmount

	var (
		discountAmountToApply    float64
		discountLabel            string
		discountSource           string
		discountTypeToStore      string
		discountValueToStore     *float64
		voucherTemplateIDToApply *int64
		voucherCodeIDToApply     *int64
	)

	requestedDiscountAmount := body.DiscountAmount
	requestedDiscountType := strings.TrimSpace(body.DiscountType)
	requestedDiscountValue := body.DiscountValue

	voucherCode := strings.TrimSpace(body.VoucherCode)
	var voucherTemplateID *int64
	if body.VoucherTemplateID != nil {
		if id, ok := parseNumericID(body.VoucherTemplateID); ok {
			voucherTemplateID = &id
		}
	}

	if voucherCode != "" || voucherTemplateID != nil {
		params := voucher.ComputeParams{
			MerchantID:         *authCtx.MerchantID,
			MerchantCurrency:   merchantCurrency,
			MerchantTimezone:   merchantTimezone,
			Audience:           voucher.AudiencePOS,
			OrderType:          order.OrderType,
			Subtotal:           order.Subtotal,
			Items:              items,
			VoucherCode:        voucherCode,
			VoucherTemplateID:  voucherTemplateID,
			OrderIDForStacking: &orderID,
		}

		computed, verr := voucher.ComputeVoucherDiscount(ctx, h.DB, params)
		if verr != nil {
			writeVoucherError(w, verr)
			return
		}

		discountAmountToApply = computed.DiscountAmount
		discountLabel = computed.Label
		discountSource = "POS_VOUCHER"
		discountTypeToStore = string(computed.DiscountType)
		value := computed.DiscountValue
		discountValueToStore = &value
		voucherTemplateIDToApply = &computed.TemplateID
		voucherCodeIDToApply = computed.CodeID

		if requestedDiscountType != "" && requestedDiscountValue != nil && *requestedDiscountValue > 0 {
			eligible := computed.EligibleSubtotal
			if strings.EqualFold(requestedDiscountType, "PERCENTAGE") {
				pct := math.Max(0, math.Min(*requestedDiscountValue, 100))
				amount := eligible * (pct / 100)
				maxCap, err := h.loadVoucherMaxDiscount(ctx, computed.TemplateID)
				if err == nil && maxCap != nil {
					if amount > *maxCap {
						amount = *maxCap
					}
				}
				discountAmountToApply = math.Min(amount, eligible)
				discountTypeToStore = "PERCENTAGE"
				discountValueToStore = &pct
			} else {
				amount := math.Min(*requestedDiscountValue, computed.EligibleSubtotal)
				discountAmountToApply = amount
				discountTypeToStore = "FIXED_AMOUNT"
				discountValueToStore = requestedDiscountValue
			}
		}
	} else if requestedDiscountAmount != nil && *requestedDiscountAmount > 0 {
		discountAmountToApply = *requestedDiscountAmount
		discountLabel = "Manual discount"
		discountSource = "MANUAL"
		if strings.EqualFold(requestedDiscountType, "PERCENTAGE") {
			discountTypeToStore = "PERCENTAGE"
		} else {
			discountTypeToStore = "FIXED_AMOUNT"
		}
		discountValueToStore = requestedDiscountValue
	}

	finalTotal := math.Max(0, orderTotalBeforeDiscount-discountAmountToApply)
	paidAmount := finalTotal
	if body.AmountPaid != nil {
		paidAmount = *body.AmountPaid
	}
	changeAmount := math.Max(0, paidAmount-finalTotal)
	if body.ChangeAmount != nil {
		changeAmount = *body.ChangeAmount
	}

	prismaMethod := paymentMethod
	if paymentMethod == "SPLIT" {
		cash := body.CashAmount
		card := body.CardAmount
		switch {
		case cash != nil && *cash > 0 && (card == nil || *card == 0):
			prismaMethod = "CASH_ON_COUNTER"
		case card != nil && *card > 0 && (cash == nil || *cash == 0):
			prismaMethod = "CARD_ON_COUNTER"
		default:
			prismaMethod = "CASH_ON_COUNTER"
		}
	}

	metadata := map[string]any{
		"source":                 "POS",
		"paidAmount":             paidAmount,
		"changeAmount":           changeAmount,
		"notes":                  strings.TrimSpace(body.Notes),
		"requestedPaymentMethod": paymentMethod,
	}
	if paymentMethod == "SPLIT" {
		metadata["split"] = map[string]any{
			"cashAmount": defaultFloat(body.CashAmount),
			"cardAmount": defaultFloat(body.CardAmount),
		}
	}

	result, err := h.savePOSPayment(ctx, *authCtx.MerchantID, order, existingPayment, prismaMethod, finalTotal, metadata, authCtx.UserID, discountAmountToApply, discountSource, discountLabel, discountTypeToStore, discountValueToStore, voucherTemplateIDToApply, voucherCodeIDToApply, merchantCurrency)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to record payment")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"paymentId":     result.PaymentID,
			"orderId":       order.ID,
			"orderNumber":   order.OrderNumber,
			"amount":        finalTotal,
			"paymentMethod": paymentMethod,
			"paidAmount":    paidAmount,
			"changeAmount":  changeAmount,
			"status":        order.Status,
		},
	})
}

type posPaymentOrder struct {
	ID          int64
	OrderNumber string
	TotalAmount float64
	Subtotal    float64
	OrderType   string
	Status      string
}

type existingPaymentInfo struct {
	ID     int64
	Status string
}

type posPaymentResult struct {
	PaymentID int64
}

func (h *Handler) loadPOSPaymentOrder(ctx context.Context, merchantID int64, orderID int64) (posPaymentOrder, []voucher.OrderItemInput, *existingPaymentInfo, error) {
	var (
		order      posPaymentOrder
		orderTotal pgtype.Numeric
		subtotal   pgtype.Numeric
	)
	if err := h.DB.QueryRow(ctx, `
		select id, order_number, total_amount, subtotal, order_type, status
		from orders
		where id = $1 and merchant_id = $2
	`, orderID, merchantID).Scan(&order.ID, &order.OrderNumber, &orderTotal, &subtotal, &order.OrderType, &order.Status); err != nil {
		return posPaymentOrder{}, nil, nil, err
	}

	order.TotalAmount = utils.NumericToFloat64(orderTotal)
	order.Subtotal = utils.NumericToFloat64(subtotal)

	items := make([]voucher.OrderItemInput, 0)
	itemRows, err := h.DB.Query(ctx, `select menu_id, subtotal from order_items where order_id = $1`, orderID)
	if err == nil {
		defer itemRows.Close()
		for itemRows.Next() {
			var menuID int64
			var itemSubtotal pgtype.Numeric
			if err := itemRows.Scan(&menuID, &itemSubtotal); err != nil {
				continue
			}
			items = append(items, voucher.OrderItemInput{MenuID: menuID, Subtotal: utils.NumericToFloat64(itemSubtotal)})
		}
	}

	var payment existingPaymentInfo
	if err := h.DB.QueryRow(ctx, `select id, status from payments where order_id = $1`, orderID).Scan(&payment.ID, &payment.Status); err == nil {
		return order, items, &payment, nil
	}

	return order, items, nil, nil
}

func (h *Handler) loadVoucherMaxDiscount(ctx context.Context, templateID int64) (*float64, error) {
	var max pgtype.Numeric
	if err := h.DB.QueryRow(ctx, `select max_discount_amount from order_voucher_templates where id = $1`, templateID).Scan(&max); err != nil {
		return nil, err
	}
	if !max.Valid {
		return nil, nil
	}
	value := utils.NumericToFloat64(max)
	return &value, nil
}

func (h *Handler) savePOSPayment(ctx context.Context, merchantID int64, order posPaymentOrder, existing *existingPaymentInfo, prismaMethod string, totalAmount float64, metadata map[string]any, userID int64, discountAmount float64, discountSource string, discountLabel string, discountType string, discountValue *float64, voucherTemplateID *int64, voucherCodeID *int64, currency string) (*posPaymentResult, error) {
	return h.withPOSPaymentTx(ctx, func(ctx context.Context, tx posPaymentTx) (*posPaymentResult, error) {
		if discountAmount > 0 {
			if _, err := tx.Exec(ctx, `update orders set total_amount = $1 where id = $2`, totalAmount, order.ID); err != nil {
				return nil, err
			}

			if discountSource != "" && discountLabel != "" && discountType != "" {
				err := voucher.ApplyOrderDiscountTx(ctx, tx, voucher.ApplyParams{
					MerchantID:        merchantID,
					OrderID:           order.ID,
					Source:            discountSource,
					Currency:          currency,
					Label:             discountLabel,
					DiscountType:      discountType,
					DiscountValue:     discountValue,
					DiscountAmount:    discountAmount,
					VoucherTemplateID: voucherTemplateID,
					VoucherCodeID:     voucherCodeID,
					AppliedByUserID:   &userID,
					ReplaceSources:    []string{"POS_VOUCHER", "MANUAL"},
				})
				if err != nil {
					return nil, err
				}
			}
		}

		if existing != nil {
			if existing.Status == "COMPLETED" {
				return &posPaymentResult{PaymentID: existing.ID}, nil
			}

			if _, err := tx.Exec(ctx, `
				update payments
				set amount = $1, payment_method = $2, status = 'COMPLETED', paid_by_user_id = $3, paid_at = $4, notes = $5, metadata = $6
				where id = $7
			`, totalAmount, prismaMethod, userID, time.Now(), nilIfEmpty(metadata["notes"]), metadata, existing.ID); err != nil {
				return nil, err
			}
			return &posPaymentResult{PaymentID: existing.ID}, nil
		}

		var paymentID int64
		if err := tx.QueryRow(ctx, `
			insert into payments (order_id, amount, payment_method, status, paid_by_user_id, paid_at, notes, metadata)
			values ($1,$2,$3,'COMPLETED',$4,$5,$6,$7)
			returning id
		`, order.ID, totalAmount, prismaMethod, userID, time.Now(), nilIfEmpty(metadata["notes"]), metadata).Scan(&paymentID); err != nil {
			return nil, err
		}

		return &posPaymentResult{PaymentID: paymentID}, nil
	})
}

type posPaymentTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (h *Handler) withPOSPaymentTx(ctx context.Context, fn func(ctx context.Context, tx posPaymentTx) (*posPaymentResult, error)) (*posPaymentResult, error) {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := fn(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func nilIfEmpty(value any) any {
	if s, ok := value.(string); ok {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			return nil
		}
		return trimmed
	}
	return value
}

func defaultFloat(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
