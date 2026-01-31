package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

type planPricing struct {
	Currency        string
	DepositMinimum  float64
	OrderFee        float64
	MonthlyPrice    float64
	GracePeriodDays int
	MonthlyDays     int
	TrialDays       int
	BankName        *string
	BankAccount     *string
	BankAccountName *string
}

func (h *Handler) MerchantSubscriptionGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var currency string
	if err := h.DB.QueryRow(ctx, "select currency from merchants where id = $1", *authCtx.MerchantID).Scan(&currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if strings.TrimSpace(currency) == "" {
		currency = "IDR"
	}

	if result, err := h.checkAndAutoSwitchSubscription(ctx, *authCtx.MerchantID); err != nil {
		h.subscriptionAutoSwitchError(err, *authCtx.MerchantID)
	} else {
		h.logAutoSwitchResult(result, *authCtx.MerchantID)
	}

	pricing, err := h.fetchPlanPricing(ctx, currency)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get subscription")
		return
	}

	var (
		subType          string
		subStatus        string
		trialEndsAt      pgtype.Timestamptz
		currentPeriodEnd pgtype.Timestamptz
		suspendReason    pgtype.Text
	)
	err = h.DB.QueryRow(ctx, `
        select type, status, trial_ends_at, current_period_end, suspend_reason
        from merchant_subscriptions
        where merchant_id = $1
    `, *authCtx.MerchantID).Scan(&subType, &subStatus, &trialEndsAt, &currentPeriodEnd, &suspendReason)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			response.JSON(w, http.StatusOK, map[string]any{
				"success": true,
				"data": map[string]any{
					"subscription": map[string]any{
						"type":                    "NONE",
						"status":                  "SUSPENDED",
						"isValid":                 false,
						"daysRemaining":           0,
						"trialEndsAt":             nil,
						"currentPeriodEnd":        nil,
						"suspendReason":           "No active subscription",
						"pendingSuspension":       false,
						"pendingSuspensionReason": nil,
					},
					"balance": nil,
					"pricing": map[string]any{
						"currency":       pricing.Currency,
						"depositMinimum": pricing.DepositMinimum,
						"orderFee":       pricing.OrderFee,
						"monthlyPrice":   pricing.MonthlyPrice,
					},
				},
			})
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get subscription")
		return
	}

	now := time.Now().UTC()
	isValid := false
	inGracePeriod := false
	daysRemaining := 0

	if subType == "TRIAL" && trialEndsAt.Valid {
		daysRemaining = int(math.Ceil(trialEndsAt.Time.Sub(now).Hours() / 24))
		if daysRemaining <= 0 && strings.EqualFold(subStatus, "ACTIVE") {
			graceEndsAt := trialEndsAt.Time.AddDate(0, 0, pricing.GracePeriodDays)
			graceRemaining := int(math.Ceil(graceEndsAt.Sub(now).Hours() / 24))
			if graceRemaining > 0 {
				inGracePeriod = true
				isValid = true
			}
			if daysRemaining < 0 {
				daysRemaining = 0
			}
		} else if daysRemaining > 0 {
			isValid = true
		} else if daysRemaining < 0 {
			daysRemaining = 0
		}
	}

	if subType == "MONTHLY" && currentPeriodEnd.Valid {
		daysRemaining = int(math.Ceil(currentPeriodEnd.Time.Sub(now).Hours() / 24))
		if daysRemaining <= 0 && strings.EqualFold(subStatus, "ACTIVE") {
			graceEndsAt := currentPeriodEnd.Time.AddDate(0, 0, pricing.GracePeriodDays)
			graceRemaining := int(math.Ceil(graceEndsAt.Sub(now).Hours() / 24))
			if graceRemaining > 0 {
				inGracePeriod = true
				isValid = true
			}
			if daysRemaining < 0 {
				daysRemaining = 0
			}
		} else if daysRemaining > 0 {
			isValid = true
		} else if daysRemaining < 0 {
			daysRemaining = 0
		}
	}

	var balanceAmount float64
	if subType == "DEPOSIT" {
		var balance pgtype.Numeric
		_ = h.DB.QueryRow(ctx, "select balance from merchant_balances where merchant_id = $1", *authCtx.MerchantID).Scan(&balance)
		balanceAmount = utils.NumericToFloat64(balance)
		if strings.EqualFold(subStatus, "ACTIVE") {
			isValid = balanceAmount > 0
		}
	}

	pendingSuspension := strings.EqualFold(subStatus, "ACTIVE") && !isValid && !inGracePeriod && (subType == "DEPOSIT" || subType == "MONTHLY" || subType == "TRIAL")
	pendingReason := any(nil)
	if pendingSuspension {
		switch subType {
		case "DEPOSIT":
			pendingReason = "DEPOSIT_DEPLETED"
		case "MONTHLY":
			pendingReason = "MONTHLY_EXPIRED"
		case "TRIAL":
			pendingReason = "TRIAL_EXPIRED"
		}
	}

	suspendReasonValue := any(nil)
	if suspendReason.Valid {
		suspendReasonValue = suspendReason.String
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"subscription": map[string]any{
				"type":                    subType,
				"status":                  subStatus,
				"isValid":                 isValid,
				"daysRemaining":           daysRemaining,
				"trialEndsAt":             nullableTime(trialEndsAt),
				"currentPeriodEnd":        nullableTime(currentPeriodEnd),
				"suspendReason":           suspendReasonValue,
				"pendingSuspension":       pendingSuspension,
				"pendingSuspensionReason": pendingReason,
			},
			"balance": func() any {
				if subType != "DEPOSIT" {
					return nil
				}
				return map[string]any{
					"amount":   balanceAmount,
					"currency": pricing.Currency,
				}
			}(),
			"pricing": map[string]any{
				"currency":       pricing.Currency,
				"depositMinimum": pricing.DepositMinimum,
				"orderFee":       pricing.OrderFee,
				"monthlyPrice":   pricing.MonthlyPrice,
			},
		},
	})
}

func (h *Handler) MerchantSubscriptionCanSwitch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant access required")
		return
	}

	var (
		subType          pgtype.Text
		currentPeriodEnd pgtype.Timestamptz
	)
	_ = h.DB.QueryRow(ctx, `
        select type, current_period_end
        from merchant_subscriptions
        where merchant_id = $1
    `, *authCtx.MerchantID).Scan(&subType, &currentPeriodEnd)

	var balance pgtype.Numeric
	_ = h.DB.QueryRow(ctx, "select balance from merchant_balances where merchant_id = $1", *authCtx.MerchantID).Scan(&balance)
	balanceValue := utils.NumericToFloat64(balance)

	now := time.Now()
	hasActiveMonthly := currentPeriodEnd.Valid && currentPeriodEnd.Time.After(now)
	hasPositiveBalance := balanceValue > 0

	currentType := "NONE"
	if subType.Valid && strings.TrimSpace(subType.String) != "" {
		currentType = subType.String
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"canSwitchToDeposit": hasPositiveBalance && currentType != "DEPOSIT",
			"canSwitchToMonthly": hasActiveMonthly && currentType != "MONTHLY",
			"currentType":        currentType,
			"hasActiveMonthly":   hasActiveMonthly,
			"hasPositiveBalance": hasPositiveBalance,
			"balance":            balanceValue,
			"monthlyEndsAt":      nullableTime(currentPeriodEnd),
		},
	})
}

func (h *Handler) MerchantSubscriptionSwitchType(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant access required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	var body struct {
		NewType string `json:"newType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	newType := strings.TrimSpace(body.NewType)
	if newType != "MONTHLY" && newType != "DEPOSIT" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid subscription type")
		return
	}

	canSwitch, err := h.getManualSwitchState(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to switch subscription type")
		return
	}
	if newType == canSwitch.CurrentType {
		response.Error(w, http.StatusBadRequest, "SWITCH_NOT_ALLOWED", fmt.Sprintf("Already on %s mode", newType))
		return
	}

	allowed := false
	if newType == "MONTHLY" {
		allowed = canSwitch.CanSwitchToMonthly
	} else {
		allowed = canSwitch.CanSwitchToDeposit
	}
	if !allowed {
		message := ""
		if newType == "MONTHLY" {
			message = "Monthly subscription is not active. Please renew first."
		} else {
			message = "Deposit balance is empty. Please top up first."
		}
		response.Error(w, http.StatusBadRequest, "SWITCH_NOT_ALLOWED", message)
		return
	}

	if _, err := h.DB.Exec(ctx, `
        update merchant_subscriptions
        set type = $1, status = 'ACTIVE', suspended_at = null, suspend_reason = null, updated_at = now()
        where merchant_id = $2
    `, newType, *authCtx.MerchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to switch subscription type")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully switched to %s mode", newType),
		"data": map[string]any{
			"merchantId": fmt.Sprint(*authCtx.MerchantID),
			"newType":    newType,
		},
	})

	h.logBillingEvent(
		"subscription_manual_switch",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.String("newType", newType),
	)
}

func (h *Handler) MerchantSubscriptionHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	limit := parseIntQuery(r, "limit", 20)
	if limit > 100 {
		limit = 100
	}
	offset := parseIntQuery(r, "offset", 0)
	eventType := strings.TrimSpace(r.URL.Query().Get("eventType"))

	args := []any{*authCtx.MerchantID}
	where := "where merchant_id = $1"
	if eventType != "" {
		args = append(args, eventType)
		where += fmt.Sprintf(" and event_type = $%d", len(args))
	}

	var currency string
	_ = h.DB.QueryRow(ctx, "select currency from merchants where id = $1", *authCtx.MerchantID).Scan(&currency)
	if strings.TrimSpace(currency) == "" {
		currency = "IDR"
	}

	var total int
	if err := h.DB.QueryRow(ctx, "select count(*) from subscription_history "+where, args...).Scan(&total); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch subscription history")
		return
	}

	args = append(args, limit, offset)
	query := fmt.Sprintf(`
        select id, merchant_id, event_type, previous_type, previous_status, previous_balance, previous_period_end,
               new_type, new_status, new_balance, new_period_end, reason, metadata, triggered_by, triggered_by_user_id, created_at
        from subscription_history
        %s
        order by created_at desc
        limit $%d offset $%d
    `, where, len(args)-1, len(args))

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch subscription history")
		return
	}
	defer rows.Close()

	history := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id              int64
			merchantID      int64
			eventTypeValue  string
			previousType    pgtype.Text
			previousStatus  pgtype.Text
			previousBalance pgtype.Numeric
			previousPeriod  pgtype.Timestamptz
			newType         pgtype.Text
			newStatus       pgtype.Text
			newBalance      pgtype.Numeric
			newPeriod       pgtype.Timestamptz
			reason          pgtype.Text
			metadataBytes   []byte
			triggeredBy     pgtype.Text
			triggeredByUser pgtype.Int8
			createdAt       time.Time
		)
		if err := rows.Scan(
			&id,
			&merchantID,
			&eventTypeValue,
			&previousType,
			&previousStatus,
			&previousBalance,
			&previousPeriod,
			&newType,
			&newStatus,
			&newBalance,
			&newPeriod,
			&reason,
			&metadataBytes,
			&triggeredBy,
			&triggeredByUser,
			&createdAt,
		); err != nil {
			continue
		}

		metadata := map[string]any{}
		if len(metadataBytes) > 0 {
			_ = json.Unmarshal(metadataBytes, &metadata)
		}

		normalized := normalizeSubscriptionMetadata(metadata, eventTypeValue, id, previousPeriod, newPeriod)

		item := map[string]any{
			"id":                fmt.Sprint(id),
			"merchantId":        fmt.Sprint(merchantID),
			"eventType":         eventTypeValue,
			"previousType":      nullIfEmptyText(previousType),
			"previousStatus":    nullIfEmptyText(previousStatus),
			"previousBalance":   nullableNumeric(previousBalance),
			"previousPeriodEnd": nullableTime(previousPeriod),
			"newType":           nullIfEmptyText(newType),
			"newStatus":         nullIfEmptyText(newStatus),
			"newBalance":        nullableNumeric(newBalance),
			"newPeriodEnd":      nullableTime(newPeriod),
			"reason":            nullIfEmptyText(reason),
			"metadata":          normalized,
			"triggeredBy":       nullIfEmptyText(triggeredBy),
			"triggeredByUserId": func() any {
				if triggeredByUser.Valid {
					return fmt.Sprint(triggeredByUser.Int64)
				}
				return nil
			}(),
			"createdAt": createdAt,
		}
		history = append(history, item)
	}

	filtered := make([]map[string]any, 0, len(history))
	for _, item := range history {
		if item["eventType"] == "ORDER_FEE_DEDUCTED" {
			continue
		}
		filtered = append(filtered, item)
	}

	filteredTotal := total - (len(history) - len(filtered))
	if filteredTotal < 0 {
		filteredTotal = 0
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"history": filtered,
			"pagination": map[string]any{
				"total":   filteredTotal,
				"limit":   limit,
				"offset":  offset,
				"hasMore": offset+len(filtered) < filteredTotal,
			},
			"currency": currency,
		},
	})
}

func (h *Handler) MerchantBalanceGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	var currency string
	if err := h.DB.QueryRow(ctx, "select currency from merchants where id = $1", *authCtx.MerchantID).Scan(&currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if strings.TrimSpace(currency) == "" {
		currency = "IDR"
	}

	pricing, err := h.fetchPlanPricing(ctx, currency)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get balance")
		return
	}

	var balanceID int64
	var balance pgtype.Numeric
	var lastTopup pgtype.Timestamptz
	err = h.DB.QueryRow(ctx, `
        select id, balance, last_topup_at
        from merchant_balances
        where merchant_id = $1
    `, *authCtx.MerchantID).Scan(&balanceID, &balance, &lastTopup)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = h.DB.QueryRow(ctx, `
                insert into merchant_balances (merchant_id, balance)
                values ($1, 0)
                returning id, balance, last_topup_at
            `, *authCtx.MerchantID).Scan(&balanceID, &balance, &lastTopup)
		}
	}
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get balance")
		return
	}

	balanceValue := utils.NumericToFloat64(balance)
	orderFee := pricing.OrderFee
	estimatedOrders := 0
	if orderFee > 0 {
		estimatedOrders = int(math.Floor(balanceValue / orderFee))
	}
	isLow := estimatedOrders < 10

	billingSummary, _ := h.fetchBillingSummary(ctx, *authCtx.MerchantID)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"balance":         balanceValue,
			"currency":        currency,
			"lastTopupAt":     nullableTime(lastTopup),
			"isLow":           isLow,
			"orderFee":        orderFee,
			"estimatedOrders": estimatedOrders,
			"billingSummary":  billingSummary,
		},
	})
}

func (h *Handler) MerchantBalanceUsageSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	summary, err := h.fetchUsageSummary(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get usage summary")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    summary,
	})
}

func (h *Handler) MerchantBalanceTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	limit := parseIntQuery(r, "limit", 20)
	offset := parseIntQuery(r, "offset", 0)
	if limit <= 0 {
		limit = 20
	}

	transactionType := strings.TrimSpace(r.URL.Query().Get("type"))
	startDate := strings.TrimSpace(r.URL.Query().Get("startDate"))
	endDate := strings.TrimSpace(r.URL.Query().Get("endDate"))
	search := strings.TrimSpace(r.URL.Query().Get("search"))
	includePending := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("includePending")), "true")

	var balanceID int64
	if err := h.DB.QueryRow(ctx, "select id from merchant_balances where merchant_id = $1", *authCtx.MerchantID).Scan(&balanceID); err != nil {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"transactions": []any{},
				"pagination": map[string]any{
					"total":   0,
					"limit":   limit,
					"offset":  offset,
					"hasMore": false,
				},
				"pendingCount": 0,
			},
		})
		return
	}

	where := []string{"balance_id = $1"}
	args := []any{balanceID}

	if transactionType != "" {
		args = append(args, transactionType)
		where = append(where, fmt.Sprintf("type = $%d", len(args)))
	}

	if startDate != "" {
		if parsed, err := time.Parse(time.RFC3339, startDate); err == nil {
			args = append(args, parsed)
			where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
		}
	}
	if endDate != "" {
		if parsed, err := time.Parse(time.RFC3339, endDate); err == nil {
			parsed = parsed.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
			args = append(args, parsed)
			where = append(where, fmt.Sprintf("created_at <= $%d", len(args)))
		}
	}

	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, fmt.Sprintf("description ilike $%d", len(args)))
	}

	where = append(where, "not (type = 'SUBSCRIPTION' and amount = 0 and description ilike '%days subscription%')")

	countQuery := "select count(*) from balance_transactions where " + strings.Join(where, " and ")
	var transactionTotal int
	if err := h.DB.QueryRow(ctx, countQuery, args...).Scan(&transactionTotal); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get transactions")
		return
	}

	argsWithPaging := append([]any{}, args...)
	argsWithPaging = append(argsWithPaging, limit, offset)
	query := fmt.Sprintf(`
        select id, type, amount, balance_before, balance_after, description, created_at, payment_request_id
        from balance_transactions
        where %s
        order by created_at desc
        limit $%d offset $%d
    `, strings.Join(where, " and "), len(argsWithPaging)-1, len(argsWithPaging))

	rows, err := h.DB.Query(ctx, query, argsWithPaging...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get transactions")
		return
	}
	defer rows.Close()

	transactions := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id               int64
			rowType          string
			amount           pgtype.Numeric
			balanceBefore    pgtype.Numeric
			balanceAfter     pgtype.Numeric
			description      pgtype.Text
			createdAt        time.Time
			paymentRequestID pgtype.Int8
		)
		if err := rows.Scan(&id, &rowType, &amount, &balanceBefore, &balanceAfter, &description, &createdAt, &paymentRequestID); err == nil {
			transactions = append(transactions, map[string]any{
				"id":               fmt.Sprint(id),
				"type":             rowType,
				"amount":           utils.NumericToFloat64(amount),
				"balanceBefore":    utils.NumericToFloat64(balanceBefore),
				"balanceAfter":     utils.NumericToFloat64(balanceAfter),
				"description":      nullIfEmptyText(description),
				"createdAt":        createdAt,
				"isPaymentRequest": false,
				"paymentRequestId": func() any {
					if paymentRequestID.Valid {
						return fmt.Sprint(paymentRequestID.Int64)
					}
					return nil
				}(),
			})
		}
	}

	pendingRequests := make([]map[string]any, 0)
	pendingTotal := 0
	if includePending && offset == 0 {
		pendingWhere := "merchant_id = $1 and status in ('PENDING','CONFIRMED','REJECTED')"
		pendingArgs := []any{*authCtx.MerchantID}

		if startDate != "" {
			if parsed, err := time.Parse(time.RFC3339, startDate); err == nil {
				pendingArgs = append(pendingArgs, parsed)
				pendingWhere += fmt.Sprintf(" and created_at >= $%d", len(pendingArgs))
			}
		}
		if endDate != "" {
			if parsed, err := time.Parse(time.RFC3339, endDate); err == nil {
				parsed = parsed.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
				pendingArgs = append(pendingArgs, parsed)
				pendingWhere += fmt.Sprintf(" and created_at <= $%d", len(pendingArgs))
			}
		}

		_ = h.DB.QueryRow(ctx, "select count(*) from payment_requests where "+pendingWhere, pendingArgs...).Scan(&pendingTotal)

		pendingQuery := "select id, type, status, currency, amount, months_requested, created_at from payment_requests where " + pendingWhere + " order by created_at desc limit 10"
		pendingRows, err := h.DB.Query(ctx, pendingQuery, pendingArgs...)
		if err == nil {
			defer pendingRows.Close()
			for pendingRows.Next() {
				var (
					id        int64
					reqType   string
					status    string
					currency  string
					amount    pgtype.Numeric
					months    pgtype.Int4
					createdAt time.Time
				)
				if err := pendingRows.Scan(&id, &reqType, &status, &currency, &amount, &months, &createdAt); err == nil {
					pendingRequests = append(pendingRequests, map[string]any{
						"id":               fmt.Sprintf("pr_%d", id),
						"type":             mapPendingType(reqType),
						"amount":           utils.NumericToFloat64(amount),
						"balanceBefore":    0,
						"balanceAfter":     0,
						"description":      buildPaymentRequestDescription(reqType, status, utils.NumericToFloat64(amount), currency),
						"createdAt":        createdAt,
						"status":           status,
						"paymentRequestId": fmt.Sprint(id),
						"paymentType":      reqType,
						"isPaymentRequest": true,
					})
				}
			}
		}
	}

	merged := append(pendingRequests, transactions...)
	sortTransactionsByDate(merged)

	total := transactionTotal
	if includePending {
		total += pendingTotal
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"transactions": sliceTransactions(merged, limit),
			"pagination": map[string]any{
				"total":   total,
				"limit":   limit,
				"offset":  offset,
				"hasMore": offset+len(merged) < total,
			},
			"pendingCount": pendingTotal,
		},
	})
}

func (h *Handler) MerchantBalanceTransactionsExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	var (
		merchantCode string
		currency     string
	)
	if err := h.DB.QueryRow(ctx, "select code, currency from merchants where id = $1", *authCtx.MerchantID).Scan(&merchantCode, &currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if strings.TrimSpace(currency) == "" {
		currency = "IDR"
	}

	var balanceID int64
	if err := h.DB.QueryRow(ctx, "select id from merchant_balances where merchant_id = $1", *authCtx.MerchantID).Scan(&balanceID); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "No transactions found")
		return
	}

	transactionType := strings.TrimSpace(r.URL.Query().Get("type"))
	startDate := strings.TrimSpace(r.URL.Query().Get("startDate"))
	endDate := strings.TrimSpace(r.URL.Query().Get("endDate"))

	where := []string{"balance_id = $1"}
	args := []any{balanceID}
	if transactionType != "" {
		args = append(args, transactionType)
		where = append(where, fmt.Sprintf("type = $%d", len(args)))
	}
	if startDate != "" {
		if parsed, err := time.Parse(time.RFC3339, startDate); err == nil {
			args = append(args, parsed)
			where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
		}
	}
	if endDate != "" {
		if parsed, err := time.Parse(time.RFC3339, endDate); err == nil {
			parsed = parsed.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
			args = append(args, parsed)
			where = append(where, fmt.Sprintf("created_at <= $%d", len(args)))
		}
	}
	where = append(where, "not (type = 'SUBSCRIPTION' and amount = 0 and description ilike '%days subscription%')")

	query := fmt.Sprintf(`
        select created_at, type, amount, balance_before, balance_after, description
        from balance_transactions
        where %s
        order by created_at desc
    `, strings.Join(where, " and "))

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to export transactions")
		return
	}
	defer rows.Close()

	buffer := &bytes.Buffer{}
	currencySymbol := "IDR"
	if strings.EqualFold(currency, "AUD") {
		currencySymbol = "AUD"
	}
	_, _ = buffer.WriteString(fmt.Sprintf("Date,Time,Type,Amount (%s),Balance Before (%s),Balance After (%s),Description\n", currencySymbol, currencySymbol, currencySymbol))

	for rows.Next() {
		var (
			createdAt     time.Time
			rowType       string
			amount        pgtype.Numeric
			balanceBefore pgtype.Numeric
			balanceAfter  pgtype.Numeric
			description   pgtype.Text
		)
		if err := rows.Scan(&createdAt, &rowType, &amount, &balanceBefore, &balanceAfter, &description); err == nil {
			line := fmt.Sprintf("%s,%s,%s,%s,%s,%s,\"%s\"\n",
				createdAt.Format("02/01/2006"),
				createdAt.Format("15:04"),
				rowType,
				formatCurrencyExport(utils.NumericToFloat64(amount), currency),
				formatCurrencyExport(utils.NumericToFloat64(balanceBefore), currency),
				formatCurrencyExport(utils.NumericToFloat64(balanceAfter), currency),
				strings.ReplaceAll(fmt.Sprint(nullIfEmptyText(description)), "\"", "\"\""),
			)
			_, _ = buffer.WriteString(line)
		}
	}

	dateStr := time.Now().Format("2006-01-02")
	filename := fmt.Sprintf("transactions_%s_%s.csv", merchantCode, dateStr)

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buffer.Bytes())
}

func (h *Handler) MerchantBalanceTransfer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	var payload struct {
		FromMerchantID any    `json:"fromMerchantId"`
		ToMerchantID   any    `json:"toMerchantId"`
		Amount         any    `json:"amount"`
		Note           string `json:"note"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	fromMerchantID, err := parseAnyInt64(payload.FromMerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Source and destination are required")
		return
	}
	toMerchantID, err := parseAnyInt64(payload.ToMerchantID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Source and destination are required")
		return
	}
	amount := parseAnyFloat(payload.Amount)
	if !(amount > 0) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Amount must be greater than zero")
		return
	}
	if fromMerchantID == toMerchantID {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Source and destination must be different")
		return
	}

	merchants, err := h.fetchMerchantGroupInfo(ctx, []int64{fromMerchantID, toMerchantID})
	if err != nil || len(merchants) < 2 {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	fromMerchant, okFrom := merchants[fromMerchantID]
	toMerchant, okTo := merchants[toMerchantID]
	if !okFrom || !okTo {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	fromMain := fromMerchant.ParentMerchantID
	if fromMain == 0 {
		fromMain = fromMerchant.ID
	}
	toMain := toMerchant.ParentMerchantID
	if toMain == 0 {
		toMain = toMerchant.ID
	}
	if fromMain != toMain {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Branches must be in the same group")
		return
	}

	if strings.TrimSpace(fromMerchant.Currency) != strings.TrimSpace(toMerchant.Currency) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Branch currencies must match")
		return
	}

	var ownerLink bool
	if err := h.DB.QueryRow(ctx, `
        select exists(
            select 1 from merchant_users
            where user_id = $1 and role = 'OWNER' and is_active = true and merchant_id = any($2)
        )
    `, authCtx.UserID, []int64{fromMain, fromMerchantID, toMerchantID}).Scan(&ownerLink); err != nil || !ownerLink {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "You do not have access to this merchant group")
		return
	}

	noteSuffix := ""
	if strings.TrimSpace(payload.Note) != "" {
		noteSuffix = fmt.Sprintf(" (%s)", strings.TrimSpace(payload.Note))
	}
	descriptionFrom := fmt.Sprintf("Transfer to %s%s", toMerchant.Name, noteSuffix)
	descriptionTo := fmt.Sprintf("Transfer from %s%s", fromMerchant.Name, noteSuffix)

	if err := h.executeBalanceTransfer(ctx, fromMerchantID, toMerchantID, amount, descriptionFrom, descriptionTo, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to transfer balance")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"fromMerchantId": fmt.Sprint(fromMerchantID),
			"toMerchantId":   fmt.Sprint(toMerchantID),
			"amount":         amount,
		},
		"message": "Balance transferred successfully",
	})

	h.logBillingEvent(
		"balance_transfer",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.Int64("fromMerchantId", fromMerchantID),
		zap.Int64("toMerchantId", toMerchantID),
		zap.Float64("amount", amount),
	)
}

func (h *Handler) MerchantBalanceGroup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	merchantIDs, err := h.fetchOwnerMerchantIDs(ctx, authCtx.UserID)
	if err != nil || len(merchantIDs) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       map[string]any{"groups": []any{}},
			"message":    "No branch data found",
			"statusCode": http.StatusOK,
		})
		return
	}

	groups, err := h.fetchMerchantGroups(ctx, merchantIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get group balances")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"groups": groups,
		},
		"message":    "Group balances retrieved successfully",
		"statusCode": http.StatusOK,
	})

	h.logBillingEvent(
		"balance_group_listed",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.Int("groupCount", len(groups)),
	)
}

func (h *Handler) MerchantPaymentRequestList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	limit := parseIntQuery(r, "limit", 20)
	offset := parseIntQuery(r, "offset", 0)

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from merchants where id = $1)", *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	var total int
	if err := h.DB.QueryRow(ctx, "select count(*) from payment_requests where merchant_id = $1", *authCtx.MerchantID).Scan(&total); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get payment requests")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select id, type, status, currency, amount, months_requested, bank_name, bank_account_number, bank_account_name,
               confirmed_at, verified_at, rejected_at, rejection_reason, expires_at, created_at
        from payment_requests
        where merchant_id = $1
        order by created_at desc
        limit $2 offset $3
    `, *authCtx.MerchantID, limit, offset)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get payment requests")
		return
	}
	defer rows.Close()

	requests := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id              int64
			reqType         string
			status          string
			currency        string
			amount          pgtype.Numeric
			months          pgtype.Int4
			bankName        pgtype.Text
			bankNumber      pgtype.Text
			bankAccountName pgtype.Text
			confirmedAt     pgtype.Timestamptz
			verifiedAt      pgtype.Timestamptz
			rejectedAt      pgtype.Timestamptz
			rejectionReason pgtype.Text
			expiresAt       pgtype.Timestamptz
			createdAt       time.Time
		)
		if err := rows.Scan(
			&id,
			&reqType,
			&status,
			&currency,
			&amount,
			&months,
			&bankName,
			&bankNumber,
			&bankAccountName,
			&confirmedAt,
			&verifiedAt,
			&rejectedAt,
			&rejectionReason,
			&expiresAt,
			&createdAt,
		); err == nil {
			requests = append(requests, map[string]any{
				"id":                fmt.Sprint(id),
				"type":              reqType,
				"status":            status,
				"currency":          currency,
				"amount":            utils.NumericToFloat64(amount),
				"monthsRequested":   nullIfEmptyInt32(months),
				"bankName":          nullIfEmptyText(bankName),
				"bankAccountNumber": nullIfEmptyText(bankNumber),
				"bankAccountName":   nullIfEmptyText(bankAccountName),
				"confirmedAt":       nullableTime(confirmedAt),
				"verifiedAt":        nullableTime(verifiedAt),
				"rejectedAt":        nullableTime(rejectedAt),
				"rejectionReason":   nullIfEmptyText(rejectionReason),
				"expiresAt":         nullableTime(expiresAt),
				"createdAt":         createdAt,
			})
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"requests": requests,
			"pagination": map[string]any{
				"total":   total,
				"limit":   limit,
				"offset":  offset,
				"hasMore": offset+len(requests) < total,
			},
		},
	})
}

func (h *Handler) MerchantPaymentRequestCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	var currency string
	if err := h.DB.QueryRow(ctx, "select currency from merchants where id = $1", *authCtx.MerchantID).Scan(&currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if strings.TrimSpace(currency) == "" {
		currency = "IDR"
	}

	var payload struct {
		Type            string  `json:"type"`
		Amount          float64 `json:"amount"`
		MonthsRequested int     `json:"monthsRequested"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	payload.Type = strings.TrimSpace(payload.Type)
	if payload.Type != "DEPOSIT_TOPUP" && payload.Type != "MONTHLY_SUBSCRIPTION" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payment request type")
		return
	}

	var hasActive bool
	_ = h.DB.QueryRow(ctx, "select exists(select 1 from payment_requests where merchant_id = $1 and status in ('PENDING','CONFIRMED'))", *authCtx.MerchantID).Scan(&hasActive)
	if hasActive {
		response.Error(w, http.StatusConflict, "CONFLICT", "You already have a pending payment request. Please complete or cancel it first.")
		return
	}

	pricing, err := h.fetchPlanPricing(ctx, currency)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create payment request")
		return
	}

	amount := payload.Amount
	months := payload.MonthsRequested

	if payload.Type == "DEPOSIT_TOPUP" {
		if amount < pricing.DepositMinimum {
			message := fmt.Sprintf("Minimum deposit is %s", formatCurrencyLabel(pricing.DepositMinimum, currency))
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", message)
			return
		}
	}
	if payload.Type == "MONTHLY_SUBSCRIPTION" {
		if months <= 0 {
			months = 1
		}
		if months < 1 || months > 12 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Months must be between 1 and 12")
			return
		}
		amount = pricing.MonthlyPrice * float64(months)
	}

	expiresAt := time.Now().Add(24 * time.Hour)

	var (
		id              int64
		status          string
		bankName        pgtype.Text
		bankNumber      pgtype.Text
		bankAccountName pgtype.Text
		createdAt       time.Time
	)
	err = h.DB.QueryRow(ctx, `
        insert into payment_requests (
            merchant_id, type, status, currency, amount, months_requested,
            bank_name, bank_account_number, bank_account_name, expires_at
        )
        values ($1, $2, 'PENDING', $3, $4, $5, $6, $7, $8, $9)
        returning id, status, bank_name, bank_account_number, bank_account_name, created_at
    `, *authCtx.MerchantID, payload.Type, currency, amount, nullableInt32(months), pricing.BankName, pricing.BankAccount, pricing.BankAccountName, expiresAt).Scan(
		&id,
		&status,
		&bankName,
		&bankNumber,
		&bankAccountName,
		&createdAt,
	)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create payment request")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":                fmt.Sprint(id),
			"type":              payload.Type,
			"status":            status,
			"currency":          currency,
			"amount":            amount,
			"monthsRequested":   nullableInt32(months),
			"bankName":          nullIfEmptyText(bankName),
			"bankAccountNumber": nullIfEmptyText(bankNumber),
			"bankAccountName":   nullIfEmptyText(bankAccountName),
			"expiresAt":         expiresAt,
			"createdAt":         createdAt,
		},
	})

	h.logBillingEvent(
		"payment_request_created",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.String("requestId", fmt.Sprint(id)),
		zap.String("type", payload.Type),
		zap.Float64("amount", amount),
		zap.String("currency", currency),
	)
}

func (h *Handler) MerchantPaymentRequestActive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	row := h.DB.QueryRow(ctx, `
        select id, type, status, currency, amount, months_requested, bank_name, bank_account_number, bank_account_name, expires_at, created_at
        from payment_requests
        where merchant_id = $1 and status in ('PENDING','CONFIRMED')
        order by created_at desc
        limit 1
    `, *authCtx.MerchantID)

	var (
		id              int64
		reqType         string
		status          string
		currency        string
		amount          pgtype.Numeric
		months          pgtype.Int4
		bankName        pgtype.Text
		bankNumber      pgtype.Text
		bankAccountName pgtype.Text
		expiresAt       pgtype.Timestamptz
		createdAt       time.Time
	)
	if err := row.Scan(&id, &reqType, &status, &currency, &amount, &months, &bankName, &bankNumber, &bankAccountName, &expiresAt, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			response.JSON(w, http.StatusOK, map[string]any{
				"success": true,
				"data":    nil,
			})
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get active payment request")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":                fmt.Sprint(id),
			"type":              reqType,
			"status":            status,
			"currency":          currency,
			"amount":            utils.NumericToFloat64(amount),
			"monthsRequested":   nullIfEmptyInt32(months),
			"bankName":          nullIfEmptyText(bankName),
			"bankAccountNumber": nullIfEmptyText(bankNumber),
			"bankAccountName":   nullIfEmptyText(bankAccountName),
			"expiresAt":         nullableTime(expiresAt),
			"createdAt":         createdAt,
		},
	})
}

func (h *Handler) MerchantPaymentRequestConfirm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	requestID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request id")
		return
	}

	var body struct {
		TransferNotes    string `json:"transferNotes"`
		TransferProofURL string `json:"transferProofUrl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	var status string
	var expiresAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
        select status, expires_at
        from payment_requests
        where id = $1 and merchant_id = $2
    `, requestID, *authCtx.MerchantID).Scan(&status, &expiresAt); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Payment request not found")
		return
	}

	if status != "PENDING" {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("Cannot confirm a %s request", strings.ToLower(status)))
		return
	}
	if expiresAt.Valid && expiresAt.Time.Before(time.Now()) {
		_, _ = h.DB.Exec(ctx, `
            update payment_requests
            set status = 'REJECTED', rejected_at = now(), rejection_reason = 'Request expired', updated_at = now()
            where id = $1 and merchant_id = $2
        `, requestID, *authCtx.MerchantID)
		response.Error(w, http.StatusGone, "EXPIRED", "This payment request has expired. Please create a new one.")
		return
	}

	var confirmedAt time.Time
	if err := h.DB.QueryRow(ctx, `
        update payment_requests
        set status = 'CONFIRMED', confirmed_at = now(), transfer_notes = $1, transfer_proof_url = $2, updated_at = now()
        where id = $3 and merchant_id = $4
        returning confirmed_at
    `, nullableString(body.TransferNotes), nullableString(body.TransferProofURL), requestID, *authCtx.MerchantID).Scan(&confirmedAt); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to confirm payment")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Payment confirmed. Waiting for admin verification.",
		"data": map[string]any{
			"id":          fmt.Sprint(requestID),
			"status":      "CONFIRMED",
			"confirmedAt": confirmedAt,
		},
	})

	h.logBillingEvent(
		"payment_request_confirmed",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.String("requestId", fmt.Sprint(requestID)),
	)
}

func (h *Handler) MerchantPaymentRequestCancel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Merchant owner access required")
		return
	}

	requestID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request id")
		return
	}

	var status string
	if err := h.DB.QueryRow(ctx, `
        select status from payment_requests where id = $1 and merchant_id = $2
    `, requestID, *authCtx.MerchantID).Scan(&status); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Payment request not found")
		return
	}

	if status != "PENDING" && status != "CONFIRMED" {
		response.Error(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("Cannot cancel a %s request", strings.ToLower(status)))
		return
	}

	var updatedAt time.Time
	if err := h.DB.QueryRow(ctx, `
        update payment_requests
        set status = 'CANCELLED', updated_at = now()
        where id = $1 and merchant_id = $2
        returning updated_at
    `, requestID, *authCtx.MerchantID).Scan(&updatedAt); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to cancel payment request")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Payment request cancelled.",
		"data": map[string]any{
			"id":        fmt.Sprint(requestID),
			"status":    "CANCELLED",
			"updatedAt": updatedAt,
		},
	})

	h.logBillingEvent(
		"payment_request_cancelled",
		zap.Int64("merchantId", *authCtx.MerchantID),
		zap.String("requestId", fmt.Sprint(requestID)),
	)
}

type manualSwitchState struct {
	CurrentType        string
	CanSwitchToDeposit bool
	CanSwitchToMonthly bool
}

func (h *Handler) getManualSwitchState(ctx context.Context, merchantID int64) (manualSwitchState, error) {
	var (
		subType          pgtype.Text
		currentPeriodEnd pgtype.Timestamptz
	)
	_ = h.DB.QueryRow(ctx, "select type, current_period_end from merchant_subscriptions where merchant_id = $1", merchantID).Scan(&subType, &currentPeriodEnd)

	var balance pgtype.Numeric
	_ = h.DB.QueryRow(ctx, "select balance from merchant_balances where merchant_id = $1", merchantID).Scan(&balance)

	balanceValue := utils.NumericToFloat64(balance)
	hasPositiveBalance := balanceValue > 0
	hasActiveMonthly := currentPeriodEnd.Valid && currentPeriodEnd.Time.After(time.Now())

	currentType := "NONE"
	if subType.Valid && strings.TrimSpace(subType.String) != "" {
		currentType = subType.String
	}

	return manualSwitchState{
		CurrentType:        currentType,
		CanSwitchToDeposit: hasPositiveBalance && currentType != "DEPOSIT",
		CanSwitchToMonthly: hasActiveMonthly && currentType != "MONTHLY",
	}, nil
}

func (h *Handler) fetchPlanPricing(ctx context.Context, currency string) (planPricing, error) {
	var (
		depositMin      pgtype.Numeric
		orderFee        pgtype.Numeric
		monthlyPrice    pgtype.Numeric
		graceDays       pgtype.Int4
		monthlyDays     pgtype.Int4
		trialDays       pgtype.Int4
		bankName        pgtype.Text
		bankAccount     pgtype.Text
		bankAccountName pgtype.Text
	)

	query := `
		select deposit_minimum_idr, order_fee_idr, monthly_price_idr, grace_period_days, monthly_days, trial_days, bank_name_idr, bank_account_idr, bank_account_name_idr
        from subscription_plans
        where is_active = true
        order by id asc limit 1
    `
	if strings.EqualFold(currency, "AUD") {
		query = `
			select deposit_minimum_aud, order_fee_aud, monthly_price_aud, grace_period_days, monthly_days, trial_days, bank_name_aud, bank_account_aud, bank_account_name_aud
            from subscription_plans
            where is_active = true
            order by id asc limit 1
        `
	}

	if err := h.DB.QueryRow(ctx, query).Scan(&depositMin, &orderFee, &monthlyPrice, &graceDays, &monthlyDays, &trialDays, &bankName, &bankAccount, &bankAccountName); err != nil {
		return planPricing{}, err
	}

	pricing := planPricing{
		Currency:        strings.ToUpper(currency),
		DepositMinimum:  utils.NumericToFloat64(depositMin),
		OrderFee:        utils.NumericToFloat64(orderFee),
		MonthlyPrice:    utils.NumericToFloat64(monthlyPrice),
		GracePeriodDays: int(graceDays.Int32),
		MonthlyDays:     int(monthlyDays.Int32),
		TrialDays:       int(trialDays.Int32),
	}
	if bankName.Valid {
		pricing.BankName = &bankName.String
	}
	if bankAccount.Valid {
		pricing.BankAccount = &bankAccount.String
	}
	if bankAccountName.Valid {
		pricing.BankAccountName = &bankAccountName.String
	}
	return pricing, nil
}

func nullableTime(value pgtype.Timestamptz) any {
	if value.Valid {
		return value.Time
	}
	return nil
}

func nullableInt32(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func parseIntQuery(r *http.Request, key string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseAnyFloat(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err == nil {
			return parsed
		}
	}
	return 0
}

func (h *Handler) fetchBillingSummary(ctx context.Context, merchantID int64) (map[string]any, error) {
	now := time.Now()
	startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startOfYesterday := startOfToday.AddDate(0, 0, -1)
	startOfWeek := startOfToday.AddDate(0, 0, -7)
	startOfMonth := startOfToday.AddDate(0, 0, -30)

	rows, err := h.DB.Query(ctx, `
        select type, amount, created_at
        from balance_transactions
        where balance_id = (select id from merchant_balances where merchant_id = $1)
          and created_at >= $2 and created_at <= $3
    `, merchantID, startOfMonth, now)
	if err != nil {
		return map[string]any{"yesterday": 0, "lastWeek": 0, "lastMonth": 0}, err
	}
	defer rows.Close()

	yesterday := 0.0
	lastWeek := 0.0
	lastMonth := 0.0

	for rows.Next() {
		var rowType string
		var amount pgtype.Numeric
		var createdAt time.Time
		if err := rows.Scan(&rowType, &amount, &createdAt); err == nil {
			if rowType != "ORDER_FEE" {
				continue
			}
			value := utils.NumericToFloat64(amount)
			if value >= 0 {
				continue
			}
			fee := math.Abs(value)
			if createdAt.After(startOfYesterday) && createdAt.Before(startOfToday) {
				yesterday += fee
			}
			if createdAt.After(startOfWeek) {
				lastWeek += fee
			}
			lastMonth += fee
		}
	}

	return map[string]any{
		"yesterday": yesterday,
		"lastWeek":  lastWeek,
		"lastMonth": lastMonth,
	}, nil
}

func (h *Handler) fetchUsageSummary(ctx context.Context, merchantID int64) (map[string]any, error) {
	now := time.Now()
	startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startOfLast30 := startOfToday.AddDate(0, 0, -30)

	rows, err := h.DB.Query(ctx, `
        select type, amount, created_at
        from balance_transactions
        where balance_id = (select id from merchant_balances where merchant_id = $1)
          and created_at >= $2 and created_at <= $3
    `, merchantID, startOfLast30, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summary := map[string]any{
		"today": map[string]any{
			"orderFee":                    0.0,
			"orderFeeCount":               0,
			"completedOrderEmailFee":      0.0,
			"completedOrderEmailFeeCount": 0,
			"total":                       0.0,
		},
		"last30Days": map[string]any{
			"orderFee":                    0.0,
			"orderFeeCount":               0,
			"completedOrderEmailFee":      0.0,
			"completedOrderEmailFeeCount": 0,
			"total":                       0.0,
		},
	}

	today := summary["today"].(map[string]any)
	last30 := summary["last30Days"].(map[string]any)

	for rows.Next() {
		var rowType string
		var amount pgtype.Numeric
		var createdAt time.Time
		if err := rows.Scan(&rowType, &amount, &createdAt); err == nil {
			value := utils.NumericToFloat64(amount)
			if !(value < 0) {
				continue
			}
			fee := math.Abs(value)
			inToday := createdAt.After(startOfToday)

			if rowType == "ORDER_FEE" {
				last30["orderFee"] = last30["orderFee"].(float64) + fee
				last30["orderFeeCount"] = last30["orderFeeCount"].(int) + 1
				if inToday {
					today["orderFee"] = today["orderFee"].(float64) + fee
					today["orderFeeCount"] = today["orderFeeCount"].(int) + 1
				}
			}

			if rowType == "COMPLETED_ORDER_EMAIL_FEE" {
				last30["completedOrderEmailFee"] = last30["completedOrderEmailFee"].(float64) + fee
				last30["completedOrderEmailFeeCount"] = last30["completedOrderEmailFeeCount"].(int) + 1
				if inToday {
					today["completedOrderEmailFee"] = today["completedOrderEmailFee"].(float64) + fee
					today["completedOrderEmailFeeCount"] = today["completedOrderEmailFeeCount"].(int) + 1
				}
			}
		}
	}

	today["total"] = today["orderFee"].(float64) + today["completedOrderEmailFee"].(float64)
	last30["total"] = last30["orderFee"].(float64) + last30["completedOrderEmailFee"].(float64)

	return summary, nil
}

func buildPaymentRequestDescription(reqType string, status string, amount float64, currency string) string {
	typeLabel := "Monthly Subscription"
	if reqType == "DEPOSIT_TOPUP" {
		typeLabel = "Top Up"
	}

	amountStr := formatCurrencyLabel(amount, currency)
	switch status {
	case "PENDING":
		return fmt.Sprintf("%s %s - Waiting for payment", typeLabel, amountStr)
	case "CONFIRMED":
		return fmt.Sprintf("%s %s - Waiting for admin verification", typeLabel, amountStr)
	case "REJECTED":
		return fmt.Sprintf("%s %s - Payment rejected", typeLabel, amountStr)
	case "VERIFIED":
		return fmt.Sprintf("%s %s - Payment verified", typeLabel, amountStr)
	case "EXPIRED":
		return fmt.Sprintf("%s %s - Request expired", typeLabel, amountStr)
	default:
		return fmt.Sprintf("%s %s", typeLabel, amountStr)
	}
}

func formatCurrencyLabel(amount float64, currency string) string {
	if strings.EqualFold(currency, "AUD") {
		return fmt.Sprintf("A$%s", formatNumberAUD(amount))
	}
	return fmt.Sprintf("Rp %s", formatNumberIDR(amount))
}

func formatCurrencyExport(amount float64, currency string) string {
	if strings.EqualFold(currency, "AUD") {
		return formatNumberAUD(amount)
	}
	return formatNumberIDR(amount)
}

func formatNumberIDR(value float64) string {
	intValue := int64(math.Round(value))
	s := strconv.FormatInt(intValue, 10)
	if len(s) <= 3 {
		return s
	}
	var out strings.Builder
	start := len(s) % 3
	if start == 0 {
		start = 3
	}
	out.WriteString(s[:start])
	for i := start; i < len(s); i += 3 {
		out.WriteString(".")
		out.WriteString(s[i : i+3])
	}
	return out.String()
}

func formatNumberAUD(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func mapPendingType(reqType string) string {
	if reqType == "DEPOSIT_TOPUP" {
		return "PENDING_DEPOSIT"
	}
	return "PENDING_SUBSCRIPTION"
}

func sortTransactionsByDate(items []map[string]any) {
	if len(items) <= 1 {
		return
	}
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			aPending, _ := items[i]["isPaymentRequest"].(bool)
			bPending, _ := items[j]["isPaymentRequest"].(bool)
			if aPending != bPending {
				if bPending {
					items[i], items[j] = items[j], items[i]
				}
				continue
			}
			aTime, _ := items[i]["createdAt"].(time.Time)
			bTime, _ := items[j]["createdAt"].(time.Time)
			if bTime.After(aTime) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}

func sliceTransactions(items []map[string]any, limit int) []map[string]any {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func normalizeSubscriptionMetadata(metadata map[string]any, eventType string, historyID int64, previousPeriod pgtype.Timestamptz, newPeriod pgtype.Timestamptz) map[string]any {
	normalized := map[string]any{}
	for k, v := range metadata {
		normalized[k] = v
	}

	requestID, _ := normalized["requestId"].(string)
	voucherCode, _ := normalized["voucherCode"].(string)
	orderID, _ := normalized["orderId"].(string)

	if _, ok := normalized["flowType"].(string); !ok {
		switch eventType {
		case "PAYMENT_SUBMITTED", "PAYMENT_CANCELLED", "PAYMENT_RECEIVED", "PAYMENT_REJECTED":
			normalized["flowType"] = "PAYMENT_VERIFICATION"
		case "ORDER_FEE_DEDUCTED":
			normalized["flowType"] = "ORDER_FEE"
		case "BALANCE_TOPUP":
			normalized["flowType"] = "BALANCE_ADJUSTMENT"
		case "PERIOD_EXTENDED":
			normalized["flowType"] = "SUBSCRIPTION_ADJUSTMENT"
		}
	}

	if _, ok := normalized["flowId"].(string); !ok {
		if requestID != "" {
			normalized["flowId"] = "payment-" + requestID
		} else if voucherCode != "" {
			normalized["flowId"] = "voucher-" + voucherCode
		} else if orderID != "" {
			normalized["flowId"] = "order-fee-" + orderID
		} else {
			normalized["flowId"] = fmt.Sprintf("history-%d", historyID)
		}
	}

	daysDelta := extractNumber(normalized["daysDelta"])
	if daysDelta == nil && previousPeriod.Valid && newPeriod.Valid {
		diff := newPeriod.Time.Sub(previousPeriod.Time).Hours() / 24
		value := int(math.Round(diff))
		daysDelta = &value
		normalized["daysDelta"] = value
	}

	periodFrom, _ := normalized["periodFrom"].(string)
	periodTo, _ := normalized["periodTo"].(string)

	if periodFrom == "" && previousPeriod.Valid {
		normalized["periodFrom"] = previousPeriod.Time.Format(time.RFC3339)
	}
	if periodTo == "" && newPeriod.Valid {
		normalized["periodTo"] = newPeriod.Time.Format(time.RFC3339)
	}

	if periodFrom == "" && periodTo != "" && daysDelta != nil {
		if parsed, err := time.Parse(time.RFC3339, periodTo); err == nil {
			normalized["periodFrom"] = parsed.AddDate(0, 0, -*daysDelta).Format(time.RFC3339)
		}
	}
	if periodTo == "" && periodFrom != "" && daysDelta != nil {
		if parsed, err := time.Parse(time.RFC3339, periodFrom); err == nil {
			normalized["periodTo"] = parsed.AddDate(0, 0, *daysDelta).Format(time.RFC3339)
		}
	}

	return normalized
}

func extractNumber(value any) *int {
	switch v := value.(type) {
	case float64:
		val := int(v)
		return &val
	case int:
		val := v
		return &val
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return &parsed
		}
	}
	return nil
}
