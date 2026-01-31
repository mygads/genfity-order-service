package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/storage"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type paymentAccountPayload struct {
	ID            *string `json:"id"`
	Type          string  `json:"type"`
	ProviderName  string  `json:"providerName"`
	AccountName   string  `json:"accountName"`
	AccountNumber string  `json:"accountNumber"`
	Bsb           *string `json:"bsb"`
	Country       *string `json:"country"`
	Currency      *string `json:"currency"`
	IsActive      *bool   `json:"isActive"`
	SortOrder     *int    `json:"sortOrder"`
}

type paymentSettingsPayload struct {
	PayAtCashierEnabled   bool    `json:"payAtCashierEnabled"`
	ManualTransferEnabled bool    `json:"manualTransferEnabled"`
	QrisEnabled           bool    `json:"qrisEnabled"`
	RequirePaymentProof   bool    `json:"requirePaymentProof"`
	QrisImageUrl          *string `json:"qrisImageUrl"`
	QrisImageMeta         any     `json:"qrisImageMeta"`
	QrisImageUploadedAt   *string `json:"qrisImageUploadedAt"`
}

type paymentSettingsRequest struct {
	Settings *paymentSettingsPayload `json:"settings"`
	Accounts []paymentAccountPayload `json:"accounts"`
}

type paymentSettingsInput struct {
	PayAtCashierEnabled   bool
	ManualTransferEnabled bool
	QrisEnabled           bool
	RequirePaymentProof   bool
	QrisImageUrl          *string
	QrisImageMeta         any
	QrisImageUploadedAt   *time.Time
}

type normalizedPaymentSettings struct {
	PayAtCashierEnabled   bool
	ManualTransferEnabled bool
	QrisEnabled           bool
	RequirePaymentProof   bool
	QrisImageUrl          *string
	QrisImageMeta         any
	QrisImageUploadedAt   *time.Time
}

func (h *Handler) MerchantPaymentSettingsGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		country  string
		currency string
	)
	if err := h.DB.QueryRow(ctx, `select country, currency from merchants where id = $1`, *authCtx.MerchantID).Scan(&country, &currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	isQrisEligible := strings.EqualFold(strings.TrimSpace(country), "indonesia") && strings.EqualFold(strings.TrimSpace(currency), "IDR")

	input := paymentSettingsInput{
		PayAtCashierEnabled:   true,
		ManualTransferEnabled: false,
		QrisEnabled:           false,
		RequirePaymentProof:   false,
		QrisImageUrl:          nil,
		QrisImageMeta:         nil,
		QrisImageUploadedAt:   nil,
	}

	if raw := h.fetchPaymentSettings(ctx, *authCtx.MerchantID); raw != nil {
		if settings, ok := raw.(merchantPaymentSettings); ok {
			input.PayAtCashierEnabled = settings.PayAtCashierEnabled
			input.ManualTransferEnabled = settings.ManualTransferEnabled
			input.QrisEnabled = settings.QrisEnabled
			input.RequirePaymentProof = settings.RequirePaymentProof
			if url, ok := settings.QrisImageUrl.(string); ok {
				trimmed := strings.TrimSpace(url)
				if trimmed != "" {
					input.QrisImageUrl = &trimmed
				}
			}
			if len(settings.QrisImageMeta) > 0 {
				input.QrisImageMeta = json.RawMessage(settings.QrisImageMeta)
			}
			if uploadedAt, ok := settings.QrisImageUploadedAt.(time.Time); ok {
				input.QrisImageUploadedAt = &uploadedAt
			}
		}
	}

	normalized := normalizePaymentSettings(input, isQrisEligible)
	accounts := h.fetchPaymentAccounts(ctx, *authCtx.MerchantID)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchantId": fmt.Sprint(*authCtx.MerchantID),
			"settings":   buildPaymentSettingsResponse(normalized),
			"accounts":   accounts,
		},
		"message":    "Payment settings retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantPaymentSettingsPut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body paymentSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "PAYMENT_SETTINGS_INVALID", "Invalid payment settings payload")
		return
	}
	if body.Settings == nil || body.Accounts == nil {
		response.Error(w, http.StatusBadRequest, "PAYMENT_SETTINGS_INVALID", "Invalid payment settings payload")
		return
	}

	var (
		country  string
		currency string
	)
	if err := h.DB.QueryRow(ctx, `select country, currency from merchants where id = $1`, *authCtx.MerchantID).Scan(&country, &currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	isQrisEligible := strings.EqualFold(strings.TrimSpace(country), "indonesia") && strings.EqualFold(strings.TrimSpace(currency), "IDR")

	input := paymentSettingsInput{
		PayAtCashierEnabled:   body.Settings.PayAtCashierEnabled,
		ManualTransferEnabled: body.Settings.ManualTransferEnabled,
		QrisEnabled:           body.Settings.QrisEnabled,
		RequirePaymentProof:   body.Settings.RequirePaymentProof,
		QrisImageUrl:          body.Settings.QrisImageUrl,
		QrisImageMeta:         body.Settings.QrisImageMeta,
		QrisImageUploadedAt:   nil,
	}

	normalized := normalizePaymentSettings(input, isQrisEligible)

	mappedAccounts := make([]mappedPaymentAccount, 0, len(body.Accounts))
	activeCount := 0
	for idx, account := range body.Accounts {
		accountType := strings.TrimSpace(account.Type)
		if accountType != "BANK" && accountType != "EWALLET" {
			response.Error(w, http.StatusBadRequest, "PAYMENT_SETTINGS_INVALID", "Invalid payment settings payload")
			return
		}
		providerName := strings.TrimSpace(account.ProviderName)
		accountName := strings.TrimSpace(account.AccountName)
		accountNumber := strings.TrimSpace(account.AccountNumber)
		if providerName == "" || accountName == "" || accountNumber == "" {
			response.Error(w, http.StatusBadRequest, "PAYMENT_SETTINGS_INVALID", "Invalid payment settings payload")
			return
		}

		bsb := trimOptionalString(account.Bsb)
		countryValue := trimOptionalString(account.Country)
		currencyValue := trimOptionalString(account.Currency)
		if countryValue == nil && strings.TrimSpace(country) != "" {
			countryValue = &country
		}
		if currencyValue == nil && strings.TrimSpace(currency) != "" {
			currencyValue = &currency
		}

		isActive := true
		if account.IsActive != nil {
			isActive = *account.IsActive
		}
		if isActive {
			activeCount++
		}

		sortOrder := idx
		if account.SortOrder != nil {
			sortOrder = *account.SortOrder
		}

		mappedAccounts = append(mappedAccounts, mappedPaymentAccount{
			Type:          accountType,
			ProviderName:  providerName,
			AccountName:   accountName,
			AccountNumber: accountNumber,
			Bsb:           bsb,
			Country:       countryValue,
			Currency:      currencyValue,
			IsActive:      isActive,
			SortOrder:     sortOrder,
		})
	}

	if normalized.ManualTransferEnabled && activeCount == 0 {
		response.Error(w, http.StatusBadRequest, "PAYMENT_ACCOUNTS_REQUIRED", "Add at least one active bank/e-wallet account.")
		return
	}

	if normalized.QrisEnabled && normalized.QrisImageUrl == nil {
		response.Error(w, http.StatusBadRequest, "QRIS_IMAGE_REQUIRED", "QRIS image is required when QRIS is enabled.")
		return
	}

	var previousQrisURL string
	var previousQris pgtype.Text
	if err := h.DB.QueryRow(ctx, `select qris_image_url from merchant_payment_settings where merchant_id = $1`, *authCtx.MerchantID).Scan(&previousQris); err == nil {
		if previousQris.Valid {
			previousQrisURL = previousQris.String
		}
	}

	now := time.Now()
	var uploadedAt *time.Time
	if normalized.QrisImageUrl != nil {
		uploadedAt = &now
	}

	var metaValue []byte
	if raw, ok := marshalOptionalJSON(normalized.QrisImageMeta); ok {
		metaValue = raw
	}

	qrisURL := any(nil)
	if normalized.QrisImageUrl != nil {
		qrisURL = *normalized.QrisImageUrl
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update payment settings")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		insert into merchant_payment_settings (
			merchant_id, pay_at_cashier_enabled, manual_transfer_enabled, qris_enabled, require_payment_proof,
			qris_image_url, qris_image_meta, qris_image_uploaded_at, created_at, updated_at
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8, now(), now()
		)
		on conflict (merchant_id) do update set
			pay_at_cashier_enabled = excluded.pay_at_cashier_enabled,
			manual_transfer_enabled = excluded.manual_transfer_enabled,
			qris_enabled = excluded.qris_enabled,
			require_payment_proof = excluded.require_payment_proof,
			qris_image_url = excluded.qris_image_url,
			qris_image_meta = excluded.qris_image_meta,
			qris_image_uploaded_at = excluded.qris_image_uploaded_at,
			updated_at = now()
	`,
		*authCtx.MerchantID,
		normalized.PayAtCashierEnabled,
		normalized.ManualTransferEnabled,
		normalized.QrisEnabled,
		normalized.RequirePaymentProof,
		qrisURL,
		metaValue,
		uploadedAt,
	)
	if err != nil {
		h.Logger.Error("payment settings upsert failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update payment settings")
		return
	}

	if _, err := tx.Exec(ctx, `delete from merchant_payment_accounts where merchant_id = $1`, *authCtx.MerchantID); err != nil {
		h.Logger.Error("payment accounts cleanup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update payment settings")
		return
	}

	for _, account := range mappedAccounts {
		if _, err := tx.Exec(ctx, `
			insert into merchant_payment_accounts (
				merchant_id, type, provider_name, account_name, account_number, bsb, country, currency, is_active, sort_order, created_at, updated_at
			) values (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now()
			)
		`,
			*authCtx.MerchantID,
			account.Type,
			account.ProviderName,
			account.AccountName,
			account.AccountNumber,
			account.Bsb,
			account.Country,
			account.Currency,
			account.IsActive,
			account.SortOrder,
		); err != nil {
			h.Logger.Error("payment account insert failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update payment settings")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("payment settings transaction commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update payment settings")
		return
	}

	accounts := h.fetchPaymentAccounts(ctx, *authCtx.MerchantID)
	updatedSettings := normalized
	updatedSettings.QrisImageUploadedAt = uploadedAt

	if previousQrisURL != "" {
		newURL := ""
		if normalized.QrisImageUrl != nil {
			newURL = *normalized.QrisImageUrl
		}
		if newURL != previousQrisURL {
			h.cleanupManagedObject(ctx, previousQrisURL)
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchantId": fmt.Sprint(*authCtx.MerchantID),
			"settings":   buildPaymentSettingsResponse(updatedSettings),
			"accounts":   accounts,
		},
		"message":    "Payment settings updated successfully",
		"statusCode": 200,
	})
}

type mappedPaymentAccount struct {
	Type          string
	ProviderName  string
	AccountName   string
	AccountNumber string
	Bsb           *string
	Country       *string
	Currency      *string
	IsActive      bool
	SortOrder     int
}

func normalizePaymentSettings(input paymentSettingsInput, isQrisEligible bool) normalizedPaymentSettings {
	manualTransferEnabled := input.ManualTransferEnabled
	qrisEnabled := false
	if isQrisEligible {
		qrisEnabled = input.QrisEnabled
	}
	hasOnline := manualTransferEnabled || qrisEnabled

	payAtCashierEnabled := input.PayAtCashierEnabled
	if !hasOnline {
		payAtCashierEnabled = true
	}

	var qrisImageUrl *string
	if qrisEnabled {
		if input.QrisImageUrl != nil {
			trimmed := strings.TrimSpace(*input.QrisImageUrl)
			if trimmed != "" {
				qrisImageUrl = &trimmed
			}
		}
	}

	var qrisImageMeta any
	if qrisEnabled {
		qrisImageMeta = input.QrisImageMeta
	}

	var uploadedAt *time.Time
	if qrisEnabled {
		uploadedAt = input.QrisImageUploadedAt
	}

	return normalizedPaymentSettings{
		PayAtCashierEnabled:   payAtCashierEnabled,
		ManualTransferEnabled: manualTransferEnabled,
		QrisEnabled:           qrisEnabled,
		RequirePaymentProof:   input.RequirePaymentProof,
		QrisImageUrl:          qrisImageUrl,
		QrisImageMeta:         qrisImageMeta,
		QrisImageUploadedAt:   uploadedAt,
	}
}

func buildPaymentSettingsResponse(settings normalizedPaymentSettings) map[string]any {
	var qrisUploadedAt any
	if settings.QrisImageUploadedAt != nil {
		qrisUploadedAt = *settings.QrisImageUploadedAt
	}

	return map[string]any{
		"payAtCashierEnabled":   settings.PayAtCashierEnabled,
		"manualTransferEnabled": settings.ManualTransferEnabled,
		"qrisEnabled":           settings.QrisEnabled,
		"requirePaymentProof":   settings.RequirePaymentProof,
		"qrisImageUrl":          settings.QrisImageUrl,
		"qrisImageMeta":         settings.QrisImageMeta,
		"qrisImageUploadedAt":   qrisUploadedAt,
	}
}

func trimOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func marshalOptionalJSON(value any) ([]byte, bool) {
	if value == nil {
		return nil, false
	}
	switch v := value.(type) {
	case []byte:
		if len(v) == 0 {
			return nil, false
		}
		return v, true
	case json.RawMessage:
		if len(v) == 0 {
			return nil, false
		}
		return v, true
	default:
		payload, err := json.Marshal(v)
		if err != nil || len(payload) == 0 {
			return nil, false
		}
		return payload, true
	}
}

func (h *Handler) cleanupManagedObject(ctx context.Context, url string) {
	store, err := storage.NewObjectStore(ctx, storage.Config{
		Endpoint:        h.Config.ObjectStoreEndpoint,
		Region:          h.Config.ObjectStoreRegion,
		AccessKeyID:     h.Config.ObjectStoreAccessKeyID,
		SecretAccessKey: h.Config.ObjectStoreSecretAccessKey,
		Bucket:          h.Config.ObjectStoreBucket,
		PublicBaseURL:   h.Config.ObjectStorePublicBaseURL,
		StorageClass:    h.Config.ObjectStoreStorageClass,
	})
	if err != nil {
		return
	}
	_ = store.DeleteURL(ctx, url)
}
