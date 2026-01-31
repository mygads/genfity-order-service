package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type voucherRedeemPayload struct {
	Code string
}

func (h *Handler) MerchantVouchersRedeem(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_REQUIRED", "Merchant context required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}
	payload, err := decodeVoucherRedeemPayload(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	if payload.Code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Voucher code is required")
		return
	}

	merchantID := *authCtx.MerchantID

	var merchantCurrency string
	if err := h.DB.QueryRow(ctx, `select currency from merchants where id = $1`, merchantID).Scan(&merchantCurrency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	codeNormalized := strings.ToUpper(strings.TrimSpace(payload.Code))
	if _, err = tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext($1))`, codeNormalized); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
		return
	}

	var (
		voucherID       int64
		voucherCode     string
		voucherType     string
		voucherValue    pgtype.Numeric
		voucherCurrency pgtype.Text
		maxUsage        pgtype.Int4
		currentUsage    int32
		validFrom       pgtype.Timestamptz
		validUntil      pgtype.Timestamptz
		isActive        bool
	)

	if err = tx.QueryRow(ctx, `
		select id, code, type, value, currency, max_usage, current_usage, valid_from, valid_until, is_active
		from vouchers where code = $1
		for update
	`, codeNormalized).Scan(
		&voucherID, &voucherCode, &voucherType, &voucherValue, &voucherCurrency, &maxUsage, &currentUsage,
		&validFrom, &validUntil, &isActive,
	); err != nil {
		response.Error(w, http.StatusNotFound, "VOUCHER_NOT_FOUND", "Voucher not found")
		return
	}

	if !isActive {
		response.Error(w, http.StatusBadRequest, "VOUCHER_INACTIVE", "This voucher is no longer active")
		return
	}

	if voucherCurrency.Valid && !strings.EqualFold(voucherCurrency.String, merchantCurrency) {
		response.Error(w, http.StatusBadRequest, "CURRENCY_MISMATCH", fmt.Sprintf("This voucher is only valid for %s merchants", voucherCurrency.String))
		return
	}

	now := time.Now().UTC()
	if validFrom.Valid && now.Before(validFrom.Time) {
		response.Error(w, http.StatusBadRequest, "VOUCHER_NOT_STARTED", "This voucher is not yet valid")
		return
	}
	if validUntil.Valid && now.After(validUntil.Time) {
		response.Error(w, http.StatusBadRequest, "VOUCHER_EXPIRED", "This voucher has expired")
		return
	}

	if maxUsage.Valid && currentUsage >= maxUsage.Int32 {
		response.Error(w, http.StatusBadRequest, "VOUCHER_LIMIT_REACHED", "This voucher has reached its usage limit")
		return
	}

	var hasRedeemed bool
	_ = tx.QueryRow(ctx, `select exists(select 1 from voucher_redemptions where voucher_id = $1 and merchant_id = $2)`, voucherID, merchantID).Scan(&hasRedeemed)
	if hasRedeemed {
		response.Error(w, http.StatusBadRequest, "ALREADY_REDEEMED", "You have already used this voucher")
		return
	}

	valueAmount := utils.NumericToFloat64(voucherValue)
	var (
		balanceBefore         *float64
		balanceAfter          *float64
		subscriptionEndBefore *time.Time
		subscriptionEndAfter  *time.Time
		previousSubType       string
		newSubType            string
		autoSwitchTriggered   bool
		redemptionID          int64
	)

	if voucherType == "BALANCE" {
		var balanceID int64
		var balance pgtype.Numeric
		err = tx.QueryRow(ctx, `select id, balance from merchant_balances where merchant_id = $1 for update`, merchantID).Scan(&balanceID, &balance)
		if err != nil && err != pgx.ErrNoRows {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
			return
		}

		beforeValue := float64(0)
		if err == nil {
			beforeValue = utils.NumericToFloat64(balance)
		}
		afterValue := beforeValue + valueAmount
		balanceBefore = &beforeValue
		balanceAfter = &afterValue

		if err == pgx.ErrNoRows {
			if err = tx.QueryRow(ctx, `
					insert into merchant_balances (merchant_id, balance, last_topup_at, created_at, updated_at)
					values ($1,$2,now(),now(),now()) returning id
				`, merchantID, afterValue).Scan(&balanceID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
				return
			}
		} else {
			if _, err = tx.Exec(ctx, `
					update merchant_balances set balance = $1, last_topup_at = now(), updated_at = now() where id = $2
				`, afterValue, balanceID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
				return
			}
		}

		_, err = tx.Exec(ctx, `
				insert into balance_transactions (balance_id, type, amount, balance_before, balance_after, description, created_by_user_id, created_at)
				values ($1,'DEPOSIT',$2,$3,$4,$5,$6,now())
			`, balanceID, valueAmount, beforeValue, afterValue, fmt.Sprintf("Voucher redemption: %s", voucherCode), authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
			return
		}
	} else if voucherType == "SUBSCRIPTION_DAYS" {
		var subID int64
		var subType string
		var currentPeriodStart pgtype.Timestamptz
		var currentPeriodEnd pgtype.Timestamptz

		err = tx.QueryRow(ctx, `
				select id, type, current_period_start, current_period_end
				from merchant_subscriptions where merchant_id = $1
				for update
			`, merchantID).Scan(&subID, &subType, &currentPeriodStart, &currentPeriodEnd)
		if err != nil && err != pgx.ErrNoRows {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
			return
		}

		daysToAdd := int(math.Floor(valueAmount))
		if daysToAdd <= 0 {
			response.Error(w, http.StatusBadRequest, "VOUCHER_INVALID", "Voucher value must be greater than 0")
			return
		}

		var periodStart time.Time
		if currentPeriodStart.Valid {
			periodStart = currentPeriodStart.Time
		} else {
			periodStart = now
		}

		var baseDate time.Time
		if currentPeriodEnd.Valid && currentPeriodEnd.Time.After(now) {
			baseDate = currentPeriodEnd.Time
		} else {
			baseDate = now
		}
		newPeriodEnd := baseDate.AddDate(0, 0, daysToAdd)

		if subType == "TRIAL" {
			previousSubType = subType
			newSubType = "MONTHLY"
			autoSwitchTriggered = true
		} else {
			previousSubType = subType
			newSubType = subType
		}

		if err == pgx.ErrNoRows {
			if err = tx.QueryRow(ctx, `
					insert into merchant_subscriptions (merchant_id, type, status, current_period_start, current_period_end, created_at, updated_at)
					values ($1,'MONTHLY','ACTIVE',$2,$3,now(),now()) returning id
				`, merchantID, now, newPeriodEnd).Scan(&subID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
				return
			}
			previousSubType = "NONE"
			newSubType = "MONTHLY"
			autoSwitchTriggered = true
		} else {
			if _, err = tx.Exec(ctx, `
					update merchant_subscriptions
					set type = $1, status = 'ACTIVE', current_period_start = $2, current_period_end = $3,
						suspended_at = null, suspend_reason = null, updated_at = now()
					where id = $4
				`, newSubType, periodStart, newPeriodEnd, subID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
				return
			}
		}

		if currentPeriodEnd.Valid {
			value := currentPeriodEnd.Time
			subscriptionEndBefore = &value
		}
		value := newPeriodEnd
		subscriptionEndAfter = &value
	} else {
		response.Error(w, http.StatusBadRequest, "VOUCHER_INVALID", "Unsupported voucher type")
		return
	}

	if err = tx.QueryRow(ctx, `
			insert into voucher_redemptions (
				voucher_id, merchant_id, redeemed_by_user_id, voucher_code, voucher_type, value_applied, currency,
				balance_before, balance_after, subscription_end_before, subscription_end_after,
				triggered_auto_switch, previous_sub_type, new_sub_type, redeemed_at
			) values (
				$1,$2,$3,$4,$5,$6,$7,
				$8,$9,$10,$11,$12,$13,$14,now()
			) returning id
		`,
		voucherID,
		merchantID,
		authCtx.UserID,
		voucherCode,
		voucherType,
		valueAmount,
		merchantCurrency,
		balanceBefore,
		balanceAfter,
		subscriptionEndBefore,
		subscriptionEndAfter,
		autoSwitchTriggered,
		nullIfEmptyString(previousSubType),
		nullIfEmptyString(newSubType),
	).Scan(&redemptionID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
		return
	}

	if _, err = tx.Exec(ctx, `
			update vouchers set current_usage = current_usage + 1, updated_at = now() where id = $1
		`, voucherID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
		return
	}

	if err = tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to redeem voucher")
		return
	}

	message := "Voucher redeemed successfully"
	if voucherType == "BALANCE" {
		message = fmt.Sprintf("Successfully added %s to your balance", formatCurrencyLabel(valueAmount, merchantCurrency))
	}
	if voucherType == "SUBSCRIPTION_DAYS" && subscriptionEndAfter != nil {
		message = fmt.Sprintf(
			"Successfully added %d days to your subscription (valid until %s)",
			int(math.Floor(valueAmount)),
			subscriptionEndAfter.Format("1/2/2006"),
		)
	}

	if voucherType == "BALANCE" {
		autoSwitchResult, autoErr := h.checkAndAutoSwitchSubscription(ctx, merchantID)
		if autoErr == nil && autoSwitchResult.Action == "AUTO_SWITCHED" {
			autoSwitchTriggered = true
			previousSubType = autoSwitchResult.PreviousType
			newSubType = autoSwitchResult.NewType
			_, _ = h.DB.Exec(ctx, `
				update voucher_redemptions
				set triggered_auto_switch = $1, previous_sub_type = $2, new_sub_type = $3
				where id = $4
			`, autoSwitchTriggered, nullIfEmptyString(previousSubType), nullIfEmptyString(newSubType), redemptionID)
		}
	}

	subscription, _ := h.fetchMerchantSubscription(ctx, merchantID)
	balanceValue := h.fetchMerchantBalanceValue(ctx, merchantID)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": message,
		"data": map[string]any{
			"voucherType":         voucherType,
			"valueApplied":        valueAmount,
			"autoSwitchTriggered": autoSwitchTriggered,
			"previousSubType": func() any {
				if autoSwitchTriggered {
					return previousSubType
				}
				return nil
			}(),
			"newSubType": func() any {
				if autoSwitchTriggered {
					return newSubType
				}
				return nil
			}(),
			"subscription": subscription,
			"balance":      balanceValue,
		},
	})
}

func decodeVoucherRedeemPayload(r *http.Request) (voucherRedeemPayload, error) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return voucherRedeemPayload{}, err
	}

	payload := voucherRedeemPayload{}
	payload.Code = strings.TrimSpace(readStringField(body["code"]))

	return payload, nil
}

func (h *Handler) fetchMerchantSubscription(ctx context.Context, merchantID int64) (any, error) {
	var (
		id              int64
		merchantIDValue int64
		subType         string
		status          string
		trialStartedAt  pgtype.Timestamptz
		trialEndsAt     pgtype.Timestamptz
		currentStart    pgtype.Timestamptz
		currentEnd      pgtype.Timestamptz
		suspendedAt     pgtype.Timestamptz
		suspendReason   pgtype.Text
		createdAt       time.Time
		updatedAt       time.Time
	)

	err := h.DB.QueryRow(ctx, `
		select id, merchant_id, type, status, trial_started_at, trial_ends_at, current_period_start, current_period_end,
		       suspended_at, suspend_reason, created_at, updated_at
		from merchant_subscriptions where merchant_id = $1
	`, merchantID).Scan(
		&id, &merchantIDValue, &subType, &status, &trialStartedAt, &trialEndsAt, &currentStart, &currentEnd,
		&suspendedAt, &suspendReason, &createdAt, &updatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return map[string]any{
		"id":                 int64ToString(id),
		"merchantId":         int64ToString(merchantIDValue),
		"type":               subType,
		"status":             status,
		"trialStartedAt":     timeOrNil(trialStartedAt),
		"trialEndsAt":        timeOrNil(trialEndsAt),
		"currentPeriodStart": timeOrNil(currentStart),
		"currentPeriodEnd":   timeOrNil(currentEnd),
		"suspendedAt":        timeOrNil(suspendedAt),
		"suspendReason":      textOrDefault(suspendReason, ""),
		"createdAt":          createdAt,
		"updatedAt":          updatedAt,
	}, nil
}

func timeOrNil(value pgtype.Timestamptz) any {
	if value.Valid {
		return value.Time
	}
	return nil
}

func nullIfEmptyString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
