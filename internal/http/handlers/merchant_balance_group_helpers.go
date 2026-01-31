package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"genfity-order-services/internal/utils"

	"github.com/jackc/pgx/v5/pgtype"
)

type merchantGroupInfo struct {
	ID               int64
	Name             string
	Code             string
	Currency         string
	ParentMerchantID int64
}

func (h *Handler) fetchMerchantGroupInfo(ctx context.Context, ids []int64) (map[int64]merchantGroupInfo, error) {
	rows, err := h.DB.Query(ctx, `
        select id, name, code, currency, coalesce(parent_merchant_id, 0)
        from merchants
        where id = any($1)
    `, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]merchantGroupInfo)
	for rows.Next() {
		var info merchantGroupInfo
		if err := rows.Scan(&info.ID, &info.Name, &info.Code, &info.Currency, &info.ParentMerchantID); err == nil {
			out[info.ID] = info
		}
	}
	return out, nil
}

func (h *Handler) executeBalanceTransfer(ctx context.Context, fromMerchantID int64, toMerchantID int64, amount float64, descriptionFrom string, descriptionTo string, userID int64) error {
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var fromBalanceID int64
	var fromBalance pgtype.Numeric
	if err := tx.QueryRow(ctx, "select id, balance from merchant_balances where merchant_id = $1 for update", fromMerchantID).Scan(&fromBalanceID, &fromBalance); err != nil {
		return err
	}
	var toBalanceID int64
	var toBalance pgtype.Numeric
	if err := tx.QueryRow(ctx, "select id, balance from merchant_balances where merchant_id = $1 for update", toMerchantID).Scan(&toBalanceID, &toBalance); err != nil {
		return err
	}

	fromBefore := utils.NumericToFloat64(fromBalance)
	toBefore := utils.NumericToFloat64(toBalance)
	if fromBefore < amount {
		return errors.New("insufficient balance")
	}

	fromAfter := fromBefore - amount
	toAfter := toBefore + amount

	if _, err := tx.Exec(ctx, "update merchant_balances set balance = $1, updated_at = now() where id = $2", fromAfter, fromBalanceID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "update merchant_balances set balance = $1, updated_at = now() where id = $2", toAfter, toBalanceID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
        insert into balance_transactions (balance_id, type, amount, balance_before, balance_after, description, created_by_user_id)
        values ($1, 'ADJUSTMENT', $2, $3, $4, $5, $6)
    `, fromBalanceID, -amount, fromBefore, fromAfter, descriptionFrom, userID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
        insert into balance_transactions (balance_id, type, amount, balance_before, balance_after, description, created_by_user_id)
        values ($1, 'ADJUSTMENT', $2, $3, $4, $5, $6)
    `, toBalanceID, amount, toBefore, toAfter, descriptionTo, userID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (h *Handler) fetchOwnerMerchantIDs(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := h.DB.Query(ctx, `
        select merchant_id
        from merchant_users
        where user_id = $1 and role = 'OWNER' and is_active = true
    `, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (h *Handler) fetchMerchantGroups(ctx context.Context, merchantIDs []int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select id, code, name, is_active, country, currency, timezone, logo_url, coalesce(parent_merchant_id, 0)
        from merchants
        where id = any($1)
    `, merchantIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type merchantRow struct {
		ID       int64
		Code     string
		Name     string
		IsActive bool
		Country  pgtype.Text
		Currency string
		Timezone pgtype.Text
		LogoURL  pgtype.Text
		ParentID int64
	}

	merchants := make([]merchantRow, 0)
	ids := make([]int64, 0)
	for rows.Next() {
		var row merchantRow
		if err := rows.Scan(&row.ID, &row.Code, &row.Name, &row.IsActive, &row.Country, &row.Currency, &row.Timezone, &row.LogoURL, &row.ParentID); err == nil {
			merchants = append(merchants, row)
			ids = append(ids, row.ID)
		}
	}

	balanceMap := make(map[int64]merchantBalanceSummary)
	balanceRows, _ := h.DB.Query(ctx, `
        select merchant_id, balance, last_topup_at
        from merchant_balances
        where merchant_id = any($1)
    `, ids)
	if balanceRows != nil {
		defer balanceRows.Close()
		for balanceRows.Next() {
			var merchantID int64
			var balance pgtype.Numeric
			var lastTopup pgtype.Timestamptz
			if err := balanceRows.Scan(&merchantID, &balance, &lastTopup); err == nil {
				balanceMap[merchantID] = merchantBalanceSummary{
					ID:      fmt.Sprint(merchantID),
					Balance: utils.NumericToFloat64(balance),
					LastTopupAt: func() any {
						if lastTopup.Valid {
							return lastTopup.Time
						}
						return nil
					}(),
				}
			}
		}
	}

	subscriptionMap := make(map[int64]map[string]any)
	subRows, _ := h.DB.Query(ctx, `
        select merchant_id, type, status, trial_ends_at, current_period_end, suspend_reason
        from merchant_subscriptions
        where merchant_id = any($1)
    `, ids)
	if subRows != nil {
		defer subRows.Close()
		for subRows.Next() {
			var merchantID int64
			var subType string
			var status string
			var trialEnds pgtype.Timestamptz
			var periodEnd pgtype.Timestamptz
			var suspendReason pgtype.Text
			if err := subRows.Scan(&merchantID, &subType, &status, &trialEnds, &periodEnd, &suspendReason); err == nil {
				daysRemaining := any(nil)
				if subType == "TRIAL" && trialEnds.Valid {
					daysRemaining = daysRemainingInt(trialEnds.Time)
				}
				if subType == "MONTHLY" && periodEnd.Valid {
					daysRemaining = daysRemainingInt(periodEnd.Time)
				}
				subscriptionMap[merchantID] = map[string]any{
					"type":             subType,
					"status":           status,
					"daysRemaining":    daysRemaining,
					"trialEndsAt":      nullableTime(trialEnds),
					"currentPeriodEnd": nullableTime(periodEnd),
					"suspendReason":    nullIfEmptyText(suspendReason),
				}
			}
		}
	}

	earningsMap := make(map[int64]map[string]any)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -30)
	orderRows, _ := h.DB.Query(ctx, `
        select merchant_id, count(*) as paid_orders, coalesce(sum(total_amount), 0)
        from orders o
        join payments p on p.order_id = o.id and p.status = 'COMPLETED'
        where o.merchant_id = any($1) and o.placed_at >= $2 and o.placed_at <= $3
        group by merchant_id
    `, ids, startDate, endDate)
	if orderRows != nil {
		defer orderRows.Close()
		for orderRows.Next() {
			var merchantID int64
			var count int64
			var total pgtype.Numeric
			if err := orderRows.Scan(&merchantID, &count, &total); err == nil {
				earningsMap[merchantID] = map[string]any{
					"paidOrders30d": count,
					"revenue30d":    utils.NumericToFloat64(total),
				}
			}
		}
	}

	grouped := make(map[int64]map[string]any)
	for _, merchant := range merchants {
		parentID := merchant.ParentID
		branchType := "MAIN"
		if parentID != 0 {
			branchType = "BRANCH"
		}
		balance := balanceMap[merchant.ID]
		earnings := earningsMap[merchant.ID]
		if earnings == nil {
			earnings = map[string]any{"paidOrders30d": 0, "revenue30d": 0}
		}
		subscription := subscriptionMap[merchant.ID]
		if subscription == nil {
			subscription = map[string]any{
				"type":             "NONE",
				"status":           "CANCELLED",
				"daysRemaining":    nil,
				"trialEndsAt":      nil,
				"currentPeriodEnd": nil,
				"suspendReason":    nil,
			}
		}

		data := map[string]any{
			"id":         fmt.Sprint(merchant.ID),
			"code":       merchant.Code,
			"name":       merchant.Name,
			"branchType": branchType,
			"parentMerchantId": func() any {
				if parentID != 0 {
					return fmt.Sprint(parentID)
				}
				return nil
			}(),
			"isActive": merchant.IsActive,
			"country":  nullIfEmptyText(merchant.Country),
			"currency": merchant.Currency,
			"timezone": nullIfEmptyText(merchant.Timezone),
			"logoUrl":  nullIfEmptyText(merchant.LogoURL),
			"balance": map[string]any{
				"amount":      balance.Balance,
				"lastTopupAt": balance.LastTopupAt,
			},
			"earnings":     earnings,
			"subscription": subscription,
		}

		mainID := merchant.ID
		if parentID != 0 {
			mainID = parentID
		}
		group := grouped[mainID]
		if group == nil {
			group = map[string]any{"main": nil, "branches": []any{}}
		}
		if parentID != 0 {
			branches := group["branches"].([]any)
			group["branches"] = append(branches, data)
		} else {
			group["main"] = data
		}
		grouped[mainID] = group
	}

	groups := make([]map[string]any, 0, len(grouped))
	for _, group := range grouped {
		groups = append(groups, group)
	}
	return groups, nil
}

func daysRemainingInt(end time.Time) int {
	diff := end.Sub(time.Now())
	days := int(math.Ceil(diff.Hours() / 24))
	if days < 0 {
		return 0
	}
	return days
}

func normalizeMerchantCurrency(currency string) string {
	value := strings.TrimSpace(currency)
	if value == "" {
		return "IDR"
	}
	return value
}
