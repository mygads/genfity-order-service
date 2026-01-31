package handlers

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"genfity-order-services/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type subscriptionAutoSwitchResult struct {
	Action         string
	Reason         string
	PreviousType   string
	PreviousStatus string
	NewType        string
	NewStatus      string
	StoreOpened    bool
}

type subscriptionRow struct {
	Type             string
	Status           string
	TrialEndsAt      pgtype.Timestamptz
	CurrentPeriodEnd pgtype.Timestamptz
}

type merchantAutoSwitchInfo struct {
	ID       int64
	Code     string
	Name     string
	IsOpen   bool
	IsActive bool
}

func (h *Handler) checkAndAutoSwitchSubscription(ctx context.Context, merchantID int64) (subscriptionAutoSwitchResult, error) {
	merchant, err := h.fetchMerchantAutoSwitchInfo(ctx, merchantID)
	if err != nil {
		return subscriptionAutoSwitchResult{}, err
	}

	subscription, err := h.fetchSubscriptionRow(ctx, merchantID)
	if err != nil {
		if err == pgx.ErrNoRows {
			created, createErr := h.createTrialSubscription(ctx, merchantID)
			if createErr != nil {
				return subscriptionAutoSwitchResult{}, createErr
			}
			if created {
				return subscriptionAutoSwitchResult{
					Action:         "AUTO_SWITCHED",
					Reason:         "No subscription found, created trial",
					PreviousType:   "NONE",
					PreviousStatus: "NONE",
					NewType:        "TRIAL",
					NewStatus:      "ACTIVE",
					StoreOpened:    false,
				}, nil
			}
			return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Subscription already exists"}, nil
		}
		return subscriptionAutoSwitchResult{}, err
	}

	balanceValue := h.fetchMerchantBalanceValue(ctx, merchantID)
	now := time.Now().UTC()

	if subscription.Status == "SUSPENDED" {
		result, reactivated, reactivateErr := h.tryReactivateSubscription(ctx, merchant, subscription, balanceValue, now)
		if reactivateErr != nil {
			return subscriptionAutoSwitchResult{}, reactivateErr
		}
		if reactivated {
			return result, nil
		}
	}

	switch subscription.Type {
	case "TRIAL":
		return h.handleTrialAutoSwitch(ctx, merchant, subscription, balanceValue, now)
	case "DEPOSIT":
		return h.handleDepositAutoSwitch(ctx, merchant, subscription, balanceValue, now)
	case "MONTHLY":
		return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Monthly subscription still valid"}, nil
	default:
		return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Unknown subscription type"}, nil
	}
}

func (h *Handler) fetchMerchantAutoSwitchInfo(ctx context.Context, merchantID int64) (merchantAutoSwitchInfo, error) {
	var info merchantAutoSwitchInfo
	if err := h.DB.QueryRow(ctx, `
        select id, code, name, is_open, is_active
        from merchants
        where id = $1
    `, merchantID).Scan(&info.ID, &info.Code, &info.Name, &info.IsOpen, &info.IsActive); err != nil {
		return merchantAutoSwitchInfo{}, err
	}
	return info, nil
}

func (h *Handler) fetchSubscriptionRow(ctx context.Context, merchantID int64) (subscriptionRow, error) {
	var row subscriptionRow
	if err := h.DB.QueryRow(ctx, `
        select type, status, trial_ends_at, current_period_end
        from merchant_subscriptions
        where merchant_id = $1
    `, merchantID).Scan(&row.Type, &row.Status, &row.TrialEndsAt, &row.CurrentPeriodEnd); err != nil {
		return subscriptionRow{}, err
	}
	return row, nil
}

func (h *Handler) createTrialSubscription(ctx context.Context, merchantID int64) (bool, error) {
	pricing, err := h.fetchPlanPricing(ctx, "IDR")
	if err != nil {
		return false, err
	}
	trialDays := pricing.TrialDays
	if trialDays <= 0 {
		trialDays = 30
	}

	trialStart := time.Now().UTC()
	trialEnds := trialStart.AddDate(0, 0, trialDays)

	result, err := h.DB.Exec(ctx, `
        insert into merchant_subscriptions (merchant_id, type, status, trial_started_at, trial_ends_at)
        values ($1, 'TRIAL', 'ACTIVE', $2, $3)
        on conflict (merchant_id) do nothing
    `, merchantID, trialStart, trialEnds)
	if err != nil {
		return false, err
	}

	_ = h.ensureMerchantBalance(ctx, merchantID)
	return result.RowsAffected() > 0, nil
}

func (h *Handler) ensureMerchantBalance(ctx context.Context, merchantID int64) error {
	_, err := h.DB.Exec(ctx, `
        insert into merchant_balances (merchant_id, balance)
        values ($1, 0)
        on conflict (merchant_id) do nothing
    `, merchantID)
	return err
}

func (h *Handler) fetchMerchantBalanceValue(ctx context.Context, merchantID int64) float64 {
	var balance pgtype.Numeric
	if err := h.DB.QueryRow(ctx, "select balance from merchant_balances where merchant_id = $1", merchantID).Scan(&balance); err != nil {
		return 0
	}
	return utils.NumericToFloat64(balance)
}

func (h *Handler) handleTrialAutoSwitch(ctx context.Context, merchant merchantAutoSwitchInfo, subscription subscriptionRow, balance float64, now time.Time) (subscriptionAutoSwitchResult, error) {
	if !subscription.TrialEndsAt.Valid {
		return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Trial has no end date"}, nil
	}

	pricing, err := h.fetchPlanPricing(ctx, "IDR")
	if err != nil {
		return subscriptionAutoSwitchResult{}, err
	}
	graceDays := pricing.GracePeriodDays
	if graceDays <= 0 {
		graceDays = 3
	}

	graceEnd := subscription.TrialEndsAt.Time.AddDate(0, 0, graceDays)
	if !now.After(graceEnd) {
		return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Trial still valid"}, nil
	}

	if subscription.CurrentPeriodEnd.Valid && subscription.CurrentPeriodEnd.Time.After(now) {
		if err := h.updateSubscriptionType(ctx, merchant.ID, "MONTHLY", "ACTIVE", true); err != nil {
			return subscriptionAutoSwitchResult{}, err
		}
		storeOpened, _ := h.reopenMerchantStore(ctx, merchant)
		_ = h.recordSubscriptionHistory(ctx, merchant.ID, "AUTO_SWITCHED", subscription.Type, subscription.Status, balance, subscription.CurrentPeriodEnd, "MONTHLY", "ACTIVE", balance, subscription.CurrentPeriodEnd, "Trial expired, switched to Monthly", buildPeriodMetadata(subscription.CurrentPeriodEnd, subscription.CurrentPeriodEnd))
		return subscriptionAutoSwitchResult{
			Action:         "AUTO_SWITCHED",
			Reason:         "Trial expired, switched to Monthly (has active period)",
			PreviousType:   subscription.Type,
			PreviousStatus: subscription.Status,
			NewType:        "MONTHLY",
			NewStatus:      "ACTIVE",
			StoreOpened:    storeOpened,
		}, nil
	}

	if balance > 0 {
		if err := h.updateSubscriptionType(ctx, merchant.ID, "DEPOSIT", "ACTIVE", true); err != nil {
			return subscriptionAutoSwitchResult{}, err
		}
		storeOpened, _ := h.reopenMerchantStore(ctx, merchant)
		_ = h.recordSubscriptionHistory(ctx, merchant.ID, "AUTO_SWITCHED", subscription.Type, subscription.Status, balance, subscription.CurrentPeriodEnd, "DEPOSIT", "ACTIVE", balance, subscription.CurrentPeriodEnd, "Trial expired, switched to Deposit", buildPeriodMetadata(subscription.CurrentPeriodEnd, subscription.CurrentPeriodEnd))
		return subscriptionAutoSwitchResult{
			Action:         "AUTO_SWITCHED",
			Reason:         "Trial expired, switched to Deposit (has balance)",
			PreviousType:   subscription.Type,
			PreviousStatus: subscription.Status,
			NewType:        "DEPOSIT",
			NewStatus:      "ACTIVE",
			StoreOpened:    storeOpened,
		}, nil
	}

	if err := h.suspendSubscription(ctx, merchant.ID, "Trial expired - no payment method available"); err != nil {
		return subscriptionAutoSwitchResult{}, err
	}
	_, _ = h.closeMerchantStore(ctx, merchant)
	_ = h.recordSubscriptionHistory(ctx, merchant.ID, "SUSPENDED", subscription.Type, subscription.Status, balance, subscription.CurrentPeriodEnd, subscription.Type, "SUSPENDED", balance, subscription.CurrentPeriodEnd, "Trial expired - no payment method available", buildPeriodMetadata(subscription.CurrentPeriodEnd, subscription.CurrentPeriodEnd))
	return subscriptionAutoSwitchResult{
		Action:         "SUSPENDED",
		Reason:         "Trial expired - no payment method available",
		PreviousType:   subscription.Type,
		PreviousStatus: subscription.Status,
		NewType:        subscription.Type,
		NewStatus:      "SUSPENDED",
		StoreOpened:    false,
	}, nil
}

func (h *Handler) handleDepositAutoSwitch(ctx context.Context, merchant merchantAutoSwitchInfo, subscription subscriptionRow, balance float64, now time.Time) (subscriptionAutoSwitchResult, error) {
	if balance > 0 {
		return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Deposit balance available"}, nil
	}

	if subscription.CurrentPeriodEnd.Valid && subscription.CurrentPeriodEnd.Time.After(now) {
		if err := h.updateSubscriptionType(ctx, merchant.ID, "MONTHLY", "ACTIVE", false); err != nil {
			return subscriptionAutoSwitchResult{}, err
		}
		storeOpened, _ := h.reopenMerchantStore(ctx, merchant)
		_ = h.recordSubscriptionHistory(ctx, merchant.ID, "AUTO_SWITCHED", subscription.Type, subscription.Status, balance, subscription.CurrentPeriodEnd, "MONTHLY", "ACTIVE", balance, subscription.CurrentPeriodEnd, "Deposit balance exhausted, switched to Monthly", buildPeriodMetadata(subscription.CurrentPeriodEnd, subscription.CurrentPeriodEnd))
		return subscriptionAutoSwitchResult{
			Action:         "AUTO_SWITCHED",
			Reason:         "Deposit balance exhausted, switched to Monthly (has active period)",
			PreviousType:   subscription.Type,
			PreviousStatus: subscription.Status,
			NewType:        "MONTHLY",
			NewStatus:      "ACTIVE",
			StoreOpened:    storeOpened,
		}, nil
	}

	return subscriptionAutoSwitchResult{Action: "NO_CHANGE", Reason: "Deposit balance exhausted; awaiting nightly suspension"}, nil
}

func (h *Handler) tryReactivateSubscription(ctx context.Context, merchant merchantAutoSwitchInfo, subscription subscriptionRow, balance float64, now time.Time) (subscriptionAutoSwitchResult, bool, error) {
	shouldReactivate := false
	reason := ""

	switch subscription.Type {
	case "MONTHLY":
		shouldReactivate = subscription.CurrentPeriodEnd.Valid && subscription.CurrentPeriodEnd.Time.After(now)
		reason = "Reactivated as Monthly (days added)"
	case "DEPOSIT":
		shouldReactivate = balance > 0
		reason = "Reactivated as Deposit (balance available)"
	case "TRIAL":
		shouldReactivate = subscription.TrialEndsAt.Valid && subscription.TrialEndsAt.Time.After(now)
		reason = "Reactivated as Trial (trial still valid)"
	}

	if !shouldReactivate {
		return subscriptionAutoSwitchResult{}, false, nil
	}

	if err := h.updateSubscriptionStatus(ctx, merchant.ID, "ACTIVE"); err != nil {
		return subscriptionAutoSwitchResult{}, false, err
	}
	storeOpened, _ := h.reopenMerchantStore(ctx, merchant)
	_ = h.recordSubscriptionHistory(ctx, merchant.ID, "REACTIVATED", subscription.Type, "SUSPENDED", balance, subscription.CurrentPeriodEnd, subscription.Type, "ACTIVE", balance, subscription.CurrentPeriodEnd, reason, buildPeriodMetadata(subscription.CurrentPeriodEnd, subscription.CurrentPeriodEnd))
	return subscriptionAutoSwitchResult{
		Action:         "REACTIVATED",
		Reason:         reason,
		PreviousType:   subscription.Type,
		PreviousStatus: "SUSPENDED",
		NewType:        subscription.Type,
		NewStatus:      "ACTIVE",
		StoreOpened:    storeOpened,
	}, true, nil
}

func (h *Handler) updateSubscriptionType(ctx context.Context, merchantID int64, newType string, status string, clearTrial bool) error {
	if clearTrial {
		_, err := h.DB.Exec(ctx, `
            update merchant_subscriptions
            set type = $1, status = $2, trial_ends_at = null, suspended_at = null, suspend_reason = null, updated_at = now()
            where merchant_id = $3
        `, newType, status, merchantID)
		return err
	}
	_, err := h.DB.Exec(ctx, `
        update merchant_subscriptions
        set type = $1, status = $2, suspended_at = null, suspend_reason = null, updated_at = now()
        where merchant_id = $3
    `, newType, status, merchantID)
	return err
}

func (h *Handler) updateSubscriptionStatus(ctx context.Context, merchantID int64, status string) error {
	_, err := h.DB.Exec(ctx, `
        update merchant_subscriptions
        set status = $1, suspended_at = null, suspend_reason = null, updated_at = now()
        where merchant_id = $2
    `, status, merchantID)
	return err
}

func (h *Handler) suspendSubscription(ctx context.Context, merchantID int64, reason string) error {
	_, err := h.DB.Exec(ctx, `
        update merchant_subscriptions
        set status = 'SUSPENDED', suspended_at = now(), suspend_reason = $1, updated_at = now()
        where merchant_id = $2
    `, reason, merchantID)
	return err
}

func (h *Handler) reopenMerchantStore(ctx context.Context, merchant merchantAutoSwitchInfo) (bool, error) {
	if merchant.IsOpen {
		return false, nil
	}
	if _, err := h.DB.Exec(ctx, `
        update merchants
        set is_open = true, is_manual_override = false, updated_at = now()
        where id = $1
    `, merchant.ID); err != nil {
		return false, err
	}
	return true, nil
}

func (h *Handler) closeMerchantStore(ctx context.Context, merchant merchantAutoSwitchInfo) (bool, error) {
	if !merchant.IsOpen {
		return false, nil
	}
	if _, err := h.DB.Exec(ctx, `
        update merchants
        set is_open = false, is_manual_override = true, updated_at = now()
        where id = $1
    `, merchant.ID); err != nil {
		return false, err
	}
	return true, nil
}

func (h *Handler) recordSubscriptionHistory(
	ctx context.Context,
	merchantID int64,
	eventType string,
	previousType string,
	previousStatus string,
	previousBalance float64,
	previousPeriodEnd pgtype.Timestamptz,
	newType string,
	newStatus string,
	newBalance float64,
	newPeriodEnd pgtype.Timestamptz,
	reason string,
	metadata map[string]any,
) error {
	metadataValue := marshalMetadata(metadata)
	_, err := h.DB.Exec(ctx, `
        insert into subscription_history (
            merchant_id, event_type, previous_type, previous_status, previous_balance, previous_period_end,
            new_type, new_status, new_balance, new_period_end, reason, metadata, triggered_by
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'SYSTEM')
    `,
		merchantID,
		eventType,
		previousType,
		previousStatus,
		previousBalance,
		nullableTimeValue(previousPeriodEnd),
		newType,
		newStatus,
		newBalance,
		nullableTimeValue(newPeriodEnd),
		reason,
		metadataValue,
	)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Warn("subscription history insert failed", zapError(err), zap.Int64("merchantId", merchantID), zap.String("eventType", eventType))
		}
	}
	return err
}

func nullableTimeValue(value pgtype.Timestamptz) any {
	if value.Valid {
		return value.Time
	}
	return nil
}

func buildPeriodMetadata(previousPeriodEnd pgtype.Timestamptz, newPeriodEnd pgtype.Timestamptz) map[string]any {
	if !previousPeriodEnd.Valid && !newPeriodEnd.Valid {
		return nil
	}

	metadata := map[string]any{}
	if previousPeriodEnd.Valid {
		metadata["periodFrom"] = previousPeriodEnd.Time.Format(time.RFC3339)
	}
	if newPeriodEnd.Valid {
		metadata["periodTo"] = newPeriodEnd.Time.Format(time.RFC3339)
	}
	if previousPeriodEnd.Valid && newPeriodEnd.Valid {
		daysDelta := int(math.Round(newPeriodEnd.Time.Sub(previousPeriodEnd.Time).Hours() / 24))
		metadata["daysDelta"] = daysDelta
	}

	return metadata
}

func marshalMetadata(metadata map[string]any) any {
	if len(metadata) == 0 {
		return nil
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return nil
	}
	return raw
}

func (h *Handler) logAutoSwitchResult(result subscriptionAutoSwitchResult, merchantID int64) {
	if result.Action == "NO_CHANGE" {
		return
	}
	h.logBillingEvent(
		"subscription_auto_switch",
		zap.Int64("merchantId", merchantID),
		zap.String("action", result.Action),
		zap.String("reason", result.Reason),
		zap.String("previousType", result.PreviousType),
		zap.String("previousStatus", result.PreviousStatus),
		zap.String("newType", result.NewType),
		zap.String("newStatus", result.NewStatus),
		zap.Bool("storeOpened", result.StoreOpened),
	)
}

func (h *Handler) subscriptionAutoSwitchError(err error, merchantID int64) {
	if h.Logger == nil {
		return
	}
	h.Logger.Warn("subscription auto-switch failed", zap.Error(err), zap.Int64("merchantId", merchantID))
}
