package services

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"genfity-order-services/internal/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SubscriptionState struct {
	Type          string
	Status        string
	IsValid       bool
	DaysRemaining *int
	SuspendReason *string
}

const subscriptionGracePeriodDays = 3

func GetSubscriptionState(ctx context.Context, db *pgxpool.Pool, merchantID int64) (SubscriptionState, error) {
	defaultDays := 0
	defaultReason := "No active subscription"
	state := SubscriptionState{
		Type:          "NONE",
		Status:        "SUSPENDED",
		IsValid:       false,
		DaysRemaining: &defaultDays,
		SuspendReason: &defaultReason,
	}

	var rawType string
	var rawStatus string
	var trialEndsAt pgtype.Timestamptz
	var currentPeriodEnd pgtype.Timestamptz
	var suspendReason pgtype.Text

	err := db.QueryRow(ctx, `
		select type, status, trial_ends_at, current_period_end, suspend_reason
		from merchant_subscriptions
		where merchant_id = $1
	`, merchantID).Scan(&rawType, &rawStatus, &trialEndsAt, &currentPeriodEnd, &suspendReason)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return state, nil
		}
		return state, err
	}

	state.Type = rawType
	state.Status = rawStatus
	state.SuspendReason = nil
	if suspendReason.Valid && strings.TrimSpace(suspendReason.String) != "" {
		reason := suspendReason.String
		state.SuspendReason = &reason
	}

	now := time.Now().UTC()
	var daysRemainingValue *int
	var inGracePeriod bool
	var graceDaysRemaining int

	if state.Type == "TRIAL" && trialEndsAt.Valid {
		days := int(math.Ceil(trialEndsAt.Time.Sub(now).Hours() / 24))
		if days <= 0 && state.Status == "ACTIVE" {
			inGracePeriod = true
			graceEndsAt := trialEndsAt.Time.AddDate(0, 0, subscriptionGracePeriodDays)
			graceDaysRemaining = int(math.Ceil(graceEndsAt.Sub(now).Hours() / 24))
			if graceDaysRemaining < 0 {
				graceDaysRemaining = 0
			}
			days = 0
		} else if days < 0 {
			days = 0
		}
		daysRemainingValue = &days
	}

	if state.Type == "MONTHLY" && currentPeriodEnd.Valid {
		days := int(math.Ceil(currentPeriodEnd.Time.Sub(now).Hours() / 24))
		if days <= 0 && state.Status == "ACTIVE" {
			inGracePeriod = true
			graceEndsAt := currentPeriodEnd.Time.AddDate(0, 0, subscriptionGracePeriodDays)
			graceDaysRemaining = int(math.Ceil(graceEndsAt.Sub(now).Hours() / 24))
			if graceDaysRemaining < 0 {
				graceDaysRemaining = 0
			}
			days = 0
		} else if days < 0 {
			days = 0
		}
		daysRemainingValue = &days
	}

	if daysRemainingValue != nil {
		state.DaysRemaining = daysRemainingValue
	} else if state.Type == "DEPOSIT" {
		state.DaysRemaining = nil
	}

	var balanceValue float64
	if state.Type == "DEPOSIT" {
		var balance pgtype.Numeric
		if err := db.QueryRow(ctx, `
			select balance
			from merchant_balances
			where merchant_id = $1
		`, merchantID).Scan(&balance); err == nil {
			balanceValue = utils.NumericToFloat64(balance)
		}
	}

	if state.Status == "ACTIVE" {
		if inGracePeriod && graceDaysRemaining > 0 {
			state.IsValid = true
		} else {
			switch state.Type {
			case "TRIAL":
				state.IsValid = trialEndsAt.Valid && trialEndsAt.Time.After(now)
			case "MONTHLY":
				state.IsValid = currentPeriodEnd.Valid && currentPeriodEnd.Time.After(now)
			case "DEPOSIT":
				state.IsValid = balanceValue > 0
			default:
				state.IsValid = false
			}
		}
	}

	return state, nil
}
