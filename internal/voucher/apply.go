package voucher

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ApplyParams struct {
	MerchantID          int64
	OrderID             int64
	Source              string
	Currency            string
	Label               string
	DiscountType        string
	DiscountValue       *float64
	DiscountAmount      float64
	VoucherTemplateID   *int64
	VoucherCodeID       *int64
	AppliedByUserID     *int64
	AppliedByCustomerID *int64
	ReplaceSources      []string
}

func ApplyOrderDiscount(ctx context.Context, db *pgxpool.Pool, params ApplyParams) *Error {
	return withTx(ctx, db, func(ctx context.Context, tx txQuery) *Error {
		return ApplyOrderDiscountTx(ctx, tx, params)
	})
}

type txQuery interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func ApplyOrderDiscountTx(ctx context.Context, tx txQuery, params ApplyParams) *Error {
	if len(params.ReplaceSources) > 0 {
		_, err := tx.Exec(ctx, `
			delete from order_discounts
			where merchant_id = $1 and order_id = $2 and source = any($3)
		`, params.MerchantID, params.OrderID, params.ReplaceSources)
		if err != nil {
			return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
		}
	}

	_, err := tx.Exec(ctx, `
		insert into order_discounts (
			merchant_id, order_id, source, voucher_template_id, voucher_code_id,
			label, discount_type, discount_value, discount_amount,
			applied_by_user_id, applied_by_customer_id, metadata
		) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`,
		params.MerchantID,
		params.OrderID,
		params.Source,
		params.VoucherTemplateID,
		params.VoucherCodeID,
		params.Label,
		params.DiscountType,
		params.DiscountValue,
		params.DiscountAmount,
		params.AppliedByUserID,
		params.AppliedByCustomerID,
		map[string]any{"currency": params.Currency},
	)
	if err != nil {
		return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
	}

	var total float64
	if err := tx.QueryRow(ctx, `
		select coalesce(sum(discount_amount), 0) from order_discounts
		where merchant_id = $1 and order_id = $2
	`, params.MerchantID, params.OrderID).Scan(&total); err != nil {
		return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
	}

	if _, err := tx.Exec(ctx, `update orders set discount_amount = $1 where id = $2`, total, params.OrderID); err != nil {
		return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
	}

	return nil
}

func withTx(ctx context.Context, db *pgxpool.Pool, fn func(ctx context.Context, tx txQuery) *Error) *Error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return ValidationError(ErrVoucherNotApplicable, "Failed to apply discount", nil)
	}
	return nil
}
