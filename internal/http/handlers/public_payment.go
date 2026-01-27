package handlers

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type publicPaymentSettings struct {
	PayAtCashierEnabled   bool    `json:"payAtCashierEnabled"`
	ManualTransferEnabled bool    `json:"manualTransferEnabled"`
	QrisEnabled           bool    `json:"qrisEnabled"`
	RequirePaymentProof   bool    `json:"requirePaymentProof"`
	QrisImageUrl          *string `json:"qrisImageUrl"`
}

type publicPaymentAccount struct {
	ID            int64   `json:"id"`
	Type          string  `json:"type"`
	ProviderName  string  `json:"providerName"`
	AccountName   string  `json:"accountName"`
	AccountNumber string  `json:"accountNumber"`
	BSB           *string `json:"bsb"`
	Country       *string `json:"country"`
	Currency      *string `json:"currency"`
	IsActive      bool    `json:"isActive"`
	SortOrder     int32   `json:"sortOrder"`
}

func (h *Handler) fetchPublicPaymentConfig(ctx context.Context, merchantID int64) (publicPaymentSettings, []publicPaymentAccount, error) {
	settings := publicPaymentSettings{PayAtCashierEnabled: true}

	var (
		payAtCashierEnabled   bool
		manualTransferEnabled bool
		qrisEnabled           bool
		requirePaymentProof   bool
		qrisImageUrl          pgtype.Text
	)

	err := h.DB.QueryRow(ctx, `
		select pay_at_cashier_enabled, manual_transfer_enabled, qris_enabled, require_payment_proof, qris_image_url
		from merchant_payment_settings
		where merchant_id = $1
	`, merchantID).Scan(
		&payAtCashierEnabled,
		&manualTransferEnabled,
		&qrisEnabled,
		&requirePaymentProof,
		&qrisImageUrl,
	)
	if err != nil && err != pgx.ErrNoRows {
		return settings, nil, err
	}
	if err == nil {
		settings.PayAtCashierEnabled = payAtCashierEnabled
		settings.ManualTransferEnabled = manualTransferEnabled
		settings.QrisEnabled = qrisEnabled
		settings.RequirePaymentProof = requirePaymentProof
		if qrisImageUrl.Valid && strings.TrimSpace(qrisImageUrl.String) != "" {
			value := qrisImageUrl.String
			settings.QrisImageUrl = &value
		}
	}

	rows, err := h.DB.Query(ctx, `
		select id, type, provider_name, account_name, account_number, bsb, country, currency, is_active, sort_order
		from merchant_payment_accounts
		where merchant_id = $1 and is_active = true
		order by sort_order asc, id asc
	`, merchantID)
	if err != nil {
		return settings, nil, err
	}
	defer rows.Close()

	accounts := make([]publicPaymentAccount, 0)
	for rows.Next() {
		var account publicPaymentAccount
		var bsb pgtype.Text
		var country pgtype.Text
		var currency pgtype.Text

		if err := rows.Scan(
			&account.ID,
			&account.Type,
			&account.ProviderName,
			&account.AccountName,
			&account.AccountNumber,
			&bsb,
			&country,
			&currency,
			&account.IsActive,
			&account.SortOrder,
		); err != nil {
			return settings, nil, err
		}

		if bsb.Valid {
			value := bsb.String
			account.BSB = &value
		}
		if country.Valid {
			value := country.String
			account.Country = &value
		}
		if currency.Valid {
			value := currency.String
			account.Currency = &value
		}

		accounts = append(accounts, account)
	}

	return settings, accounts, nil
}
