package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/storage"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type merchantOpeningHourRow struct {
	ID         int64     `json:"id"`
	MerchantID int64     `json:"merchantId"`
	DayOfWeek  int       `json:"dayOfWeek"`
	OpenTime   *string   `json:"openTime"`
	CloseTime  *string   `json:"closeTime"`
	IsClosed   bool      `json:"isClosed"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

type merchantUserSummary struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type merchantPaymentSettings struct {
	ID                    string          `json:"id"`
	MerchantID            string          `json:"merchantId"`
	PayAtCashierEnabled   bool            `json:"payAtCashierEnabled"`
	ManualTransferEnabled bool            `json:"manualTransferEnabled"`
	QrisEnabled           bool            `json:"qrisEnabled"`
	RequirePaymentProof   bool            `json:"requirePaymentProof"`
	QrisImageUrl          any             `json:"qrisImageUrl"`
	QrisImageMeta         json.RawMessage `json:"qrisImageMeta"`
	QrisImageUploadedAt   any             `json:"qrisImageUploadedAt"`
	CreatedAt             time.Time       `json:"createdAt"`
	UpdatedAt             time.Time       `json:"updatedAt"`
}

type merchantPaymentAccount struct {
	ID            string    `json:"id"`
	MerchantID    string    `json:"merchantId"`
	Type          string    `json:"type"`
	ProviderName  string    `json:"providerName"`
	AccountName   string    `json:"accountName"`
	AccountNumber string    `json:"accountNumber"`
	Bsb           any       `json:"bsb"`
	Country       any       `json:"country"`
	Currency      any       `json:"currency"`
	IsActive      bool      `json:"isActive"`
	SortOrder     int       `json:"sortOrder"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

type merchantBalanceSummary struct {
	ID          string  `json:"id"`
	Balance     float64 `json:"balance"`
	LastTopupAt any     `json:"lastTopupAt"`
}

func (h *Handler) MerchantProfileGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	payload, err := h.buildMerchantProfilePayload(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       payload,
		"message":    "Merchant profile retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantProfilePut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	var body map[string]any
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchant, err := h.fetchMerchantRow(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	nextReceiptSettings := asMap(body["receiptSettings"])
	if nextReceiptSettings != nil {
		if sendCompleted, ok := nextReceiptSettings["sendCompletedOrderEmailToCustomer"].(bool); ok && sendCompleted {
			pricing := h.fetchCompletedOrderEmailFee(ctx, merchant.Currency)
			if pricing <= 0 {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Completed-order email fee is not configured. Please contact support.")
				return
			}
			currentBalance := h.fetchMerchantBalanceAmount(ctx, *authCtx.MerchantID)
			if currentBalance < pricing {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Insufficient balance to enable completed-order email.")
				return
			}
		}
	}

	nextPromoUrls := asStringSlice(body["promoBannerUrls"], merchant.PromoBannerUrls)
	if len(nextPromoUrls) > 0 || body["promoBannerUrls"] != nil {
		h.cleanupRemovedPromoBanners(ctx, merchant.PromoBannerUrls, nextPromoUrls)
	}

	update := make(map[string]any)
	setIfPresent(update, "name", body)
	setIfPresent(update, "description", body)
	setIfPresent(update, "address", body)
	setIfPresent(update, "email", body)
	setIfPresent(update, "country", body)
	setIfPresent(update, "currency", body)
	setIfPresent(update, "timezone", body)
	setIfPresent(update, "isDineInEnabled", body)
	setIfPresent(update, "isTakeawayEnabled", body)
	setIfPresent(update, "requireTableNumberForDineIn", body)
	setIfPresent(update, "dineInLabel", body)
	setIfPresent(update, "takeawayLabel", body)
	setIfPresent(update, "deliveryLabel", body)
	setIfPresent(update, "dineInScheduleStart", body)
	setIfPresent(update, "dineInScheduleEnd", body)
	setIfPresent(update, "takeawayScheduleStart", body)
	setIfPresent(update, "takeawayScheduleEnd", body)
	setIfPresent(update, "deliveryScheduleStart", body)
	setIfPresent(update, "deliveryScheduleEnd", body)
	setIfPresent(update, "totalTables", body)
	setIfPresent(update, "posPayImmediately", body)
	setIfPresent(update, "isReservationEnabled", body)
	setIfPresent(update, "reservationMenuRequired", body)
	setIfPresent(update, "reservationMinItemCount", body)
	setIfPresent(update, "isScheduledOrderEnabled", body)
	setIfPresent(update, "enableTax", body)
	setIfPresent(update, "taxPercentage", body)
	setIfPresent(update, "enableServiceCharge", body)
	setIfPresent(update, "serviceChargePercent", body)
	setIfPresent(update, "enablePackagingFee", body)
	setIfPresent(update, "packagingFeeAmount", body)
	setIfPresent(update, "latitude", body)
	setIfPresent(update, "longitude", body)
	setIfPresent(update, "isDeliveryEnabled", body)
	setIfPresent(update, "enforceDeliveryZones", body)
	setIfPresent(update, "deliveryMaxDistanceKm", body)
	setIfPresent(update, "deliveryFeeBase", body)
	setIfPresent(update, "deliveryFeePerKm", body)
	setIfPresent(update, "deliveryFeeMin", body)
	setIfPresent(update, "deliveryFeeMax", body)
	if body["phoneNumber"] != nil {
		update["phone"] = toStringValueProfile(body["phoneNumber"])
	}
	if body["promoBannerUrls"] != nil {
		update["promoBannerUrls"] = nextPromoUrls
	}
	if body["receiptSettings"] != nil {
		update["receiptSettings"] = body["receiptSettings"]
	}

	updated, err := h.applyMerchantProfileUpdate(ctx, merchant.ID, update)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update merchant profile")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    "Merchant profile updated successfully",
		"statusCode": 200,
	})
}

func (h *Handler) fetchOpeningHours(ctx context.Context, merchantID int64) []merchantOpeningHourRow {
	rows, err := h.DB.Query(ctx, `
		select id, merchant_id, day_of_week, open_time, close_time, is_closed, created_at, updated_at
		from merchant_opening_hours
		where merchant_id = $1
		order by day_of_week asc
	`, merchantID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make([]merchantOpeningHourRow, 0)
	for rows.Next() {
		var row merchantOpeningHourRow
		var openTime pgtype.Text
		var closeTime pgtype.Text
		if err := rows.Scan(
			&row.ID,
			&row.MerchantID,
			&row.DayOfWeek,
			&openTime,
			&closeTime,
			&row.IsClosed,
			&row.CreatedAt,
			&row.UpdatedAt,
		); err == nil {
			if openTime.Valid {
				row.OpenTime = &openTime.String
			}
			if closeTime.Valid {
				row.CloseTime = &closeTime.String
			}
			result = append(result, row)
		}
	}
	return result
}

func (h *Handler) fetchParentMerchant(ctx context.Context, parentID pgtype.Int8) any {
	if !parentID.Valid {
		return nil
	}
	var id int64
	var code, name, branchType string
	if err := h.DB.QueryRow(ctx, `
		select id, code, name, branch_type
		from merchants
		where id = $1
	`, parentID.Int64).Scan(&id, &code, &name, &branchType); err != nil {
		return nil
	}
	return map[string]any{
		"id":         fmt.Sprint(id),
		"code":       code,
		"name":       name,
		"branchType": branchType,
	}
}

func (h *Handler) fetchBranchMerchants(ctx context.Context, merchantID int64) []map[string]any {
	rows, err := h.DB.Query(ctx, `
		select id, code, name, branch_type, is_active
		from merchants
		where parent_merchant_id = $1
	`, merchantID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var code, name, branchType string
		var isActive bool
		if err := rows.Scan(&id, &code, &name, &branchType, &isActive); err == nil {
			result = append(result, map[string]any{
				"id":         fmt.Sprint(id),
				"code":       code,
				"name":       name,
				"branchType": branchType,
				"isActive":   isActive,
			})
		}
	}
	return result
}

func (h *Handler) fetchPaymentSettings(ctx context.Context, merchantID int64) any {
	var (
		id                  int64
		payAtCashier        bool
		manualTransfer      bool
		qrisEnabled         bool
		requireProof        bool
		qrisImageURL        pgtype.Text
		qrisImageMeta       []byte
		qrisImageUploadedAt pgtype.Timestamptz
		createdAt           time.Time
		updatedAt           time.Time
	)
	if err := h.DB.QueryRow(ctx, `
		select id, pay_at_cashier_enabled, manual_transfer_enabled, qris_enabled, require_payment_proof,
		       qris_image_url, qris_image_meta, qris_image_uploaded_at, created_at, updated_at
		from merchant_payment_settings
		where merchant_id = $1
	`, merchantID).Scan(&id, &payAtCashier, &manualTransfer, &qrisEnabled, &requireProof, &qrisImageURL, &qrisImageMeta, &qrisImageUploadedAt, &createdAt, &updatedAt); err != nil {
		return nil
	}

	var meta json.RawMessage
	if len(qrisImageMeta) > 0 {
		meta = json.RawMessage(qrisImageMeta)
	}

	return merchantPaymentSettings{
		ID:                    fmt.Sprint(id),
		MerchantID:            fmt.Sprint(merchantID),
		PayAtCashierEnabled:   payAtCashier,
		ManualTransferEnabled: manualTransfer,
		QrisEnabled:           qrisEnabled,
		RequirePaymentProof:   requireProof,
		QrisImageUrl:          nullIfEmptyText(qrisImageURL),
		QrisImageMeta:         meta,
		QrisImageUploadedAt: func() any {
			if qrisImageUploadedAt.Valid {
				return qrisImageUploadedAt.Time
			}
			return nil
		}(),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

func (h *Handler) fetchPaymentAccounts(ctx context.Context, merchantID int64) []merchantPaymentAccount {
	rows, err := h.DB.Query(ctx, `
		select id, type, provider_name, account_name, account_number, bsb, country, currency, is_active, sort_order, created_at, updated_at
		from merchant_payment_accounts
		where merchant_id = $1
		order by sort_order asc
	`, merchantID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make([]merchantPaymentAccount, 0)
	for rows.Next() {
		var row merchantPaymentAccount
		var bsb pgtype.Text
		var country pgtype.Text
		var currency pgtype.Text
		if err := rows.Scan(&row.ID, &row.Type, &row.ProviderName, &row.AccountName, &row.AccountNumber, &bsb, &country, &currency, &row.IsActive, &row.SortOrder, &row.CreatedAt, &row.UpdatedAt); err == nil {
			row.MerchantID = fmt.Sprint(merchantID)
			row.Bsb = nullIfEmptyText(bsb)
			row.Country = nullIfEmptyText(country)
			row.Currency = nullIfEmptyText(currency)
			result = append(result, row)
		}
	}
	return result
}

func (h *Handler) fetchMerchantBalance(ctx context.Context, merchantID int64) any {
	var id int64
	var balance pgtype.Numeric
	var lastTopupAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select id, balance, last_topup_at
		from merchant_balances
		where merchant_id = $1
	`, merchantID).Scan(&id, &balance, &lastTopupAt); err != nil {
		return nil
	}
	return merchantBalanceSummary{
		ID:      fmt.Sprint(id),
		Balance: utils.NumericToFloat64(balance),
		LastTopupAt: func() any {
			if lastTopupAt.Valid {
				return lastTopupAt.Time
			}
			return nil
		}(),
	}
}

func (h *Handler) fetchMerchantBalanceAmount(ctx context.Context, merchantID int64) float64 {
	var balance pgtype.Numeric
	if err := h.DB.QueryRow(ctx, `
		select balance from merchant_balances where merchant_id = $1
	`, merchantID).Scan(&balance); err != nil {
		return 0
	}
	return utils.NumericToFloat64(balance)
}

func (h *Handler) fetchMerchantTeam(ctx context.Context, merchantID int64) ([]merchantUserSummary, []merchantUserSummary) {
	rows, err := h.DB.Query(ctx, `
		select u.id, u.name, u.email, u.role
		from merchant_users mu
		join users u on u.id = mu.user_id
		where mu.merchant_id = $1
	`, merchantID)
	if err != nil {
		return nil, nil
	}
	defer rows.Close()

	owners := make([]merchantUserSummary, 0)
	staff := make([]merchantUserSummary, 0)
	for rows.Next() {
		var id int64
		var name, email, role string
		if err := rows.Scan(&id, &name, &email, &role); err == nil {
			item := merchantUserSummary{ID: fmt.Sprint(id), Name: name, Email: email}
			switch role {
			case "MERCHANT_OWNER":
				owners = append(owners, item)
			case "MERCHANT_STAFF":
				staff = append(staff, item)
			}
		}
	}
	return owners, staff
}

func (h *Handler) fetchCompletedOrderEmailFee(ctx context.Context, currency string) float64 {
	var fee pgtype.Numeric
	query := "select completed_order_email_fee_idr from subscription_plans order by id asc limit 1"
	if strings.EqualFold(currency, "AUD") {
		query = "select completed_order_email_fee_aud from subscription_plans order by id asc limit 1"
	}
	if err := h.DB.QueryRow(ctx, query).Scan(&fee); err != nil {
		return 0
	}
	return utils.NumericToFloat64(fee)
}

type merchantRowSnapshot struct {
	ID              int64
	Currency        string
	PromoBannerUrls []string
}

func (h *Handler) fetchMerchantRow(ctx context.Context, merchantID int64) (merchantRowSnapshot, error) {
	var (
		id              int64
		currency        string
		promoBannerURLs []string
	)
	if err := h.DB.QueryRow(ctx, `
		select id, currency, promo_banner_urls
		from merchants
		where id = $1
	`, merchantID).Scan(&id, &currency, &promoBannerURLs); err != nil {
		return merchantRowSnapshot{}, err
	}

	urls := make([]string, 0, len(promoBannerURLs))
	if promoBannerURLs != nil {
		urls = append(urls, promoBannerURLs...)
	}

	return merchantRowSnapshot{ID: id, Currency: currency, PromoBannerUrls: urls}, nil
}

func (h *Handler) cleanupRemovedPromoBanners(ctx context.Context, existing []string, next []string) {
	if len(existing) == 0 {
		return
	}
	lookup := make(map[string]bool)
	for _, url := range next {
		lookup[url] = true
	}
	removed := make([]string, 0)
	for _, url := range existing {
		if !lookup[url] {
			removed = append(removed, url)
		}
	}
	if len(removed) == 0 {
		return
	}

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

	for _, url := range removed {
		_ = store.DeleteURL(ctx, url)
	}
}

func (h *Handler) applyMerchantProfileUpdate(ctx context.Context, merchantID int64, update map[string]any) (map[string]any, error) {
	if len(update) == 0 {
		return h.buildMerchantProfilePayload(ctx, merchantID)
	}

	columns := make([]string, 0, len(update)+1)
	args := make([]any, 0, len(update)+1)
	idx := 1

	if value, ok := update["promoBannerUrls"]; ok {
		update["promo_banner_urls"] = value
		delete(update, "promoBannerUrls")
	}
	if value, ok := update["phone"]; ok {
		update["phone"] = value
	}
	if value, ok := update["receiptSettings"]; ok {
		raw, _ := json.Marshal(value)
		update["receipt_settings"] = raw
		delete(update, "receiptSettings")
	}

	fieldMap := map[string]string{
		"name":                        "name",
		"description":                 "description",
		"address":                     "address",
		"email":                       "email",
		"phone":                       "phone",
		"country":                     "country",
		"currency":                    "currency",
		"timezone":                    "timezone",
		"isDineInEnabled":             "is_dine_in_enabled",
		"isTakeawayEnabled":           "is_takeaway_enabled",
		"requireTableNumberForDineIn": "require_table_number_for_dine_in",
		"dineInLabel":                 "dine_in_label",
		"takeawayLabel":               "takeaway_label",
		"deliveryLabel":               "delivery_label",
		"dineInScheduleStart":         "dine_in_schedule_start",
		"dineInScheduleEnd":           "dine_in_schedule_end",
		"takeawayScheduleStart":       "takeaway_schedule_start",
		"takeawayScheduleEnd":         "takeaway_schedule_end",
		"deliveryScheduleStart":       "delivery_schedule_start",
		"deliveryScheduleEnd":         "delivery_schedule_end",
		"totalTables":                 "total_tables",
		"posPayImmediately":           "pos_pay_immediately",
		"isReservationEnabled":        "is_reservation_enabled",
		"reservationMenuRequired":     "reservation_menu_required",
		"reservationMinItemCount":     "reservation_min_item_count",
		"isScheduledOrderEnabled":     "is_scheduled_order_enabled",
		"enableTax":                   "enable_tax",
		"taxPercentage":               "tax_percentage",
		"enableServiceCharge":         "enable_service_charge",
		"serviceChargePercent":        "service_charge_percent",
		"enablePackagingFee":          "enable_packaging_fee",
		"packagingFeeAmount":          "packaging_fee_amount",
		"latitude":                    "latitude",
		"longitude":                   "longitude",
		"isDeliveryEnabled":           "is_delivery_enabled",
		"enforceDeliveryZones":        "enforce_delivery_zones",
		"deliveryMaxDistanceKm":       "delivery_max_distance_km",
		"deliveryFeeBase":             "delivery_fee_base",
		"deliveryFeePerKm":            "delivery_fee_per_km",
		"deliveryFeeMin":              "delivery_fee_min",
		"deliveryFeeMax":              "delivery_fee_max",
		"promo_banner_urls":           "promo_banner_urls",
		"receipt_settings":            "receipt_settings",
	}

	keys := make([]string, 0, len(update))
	for key := range update {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		col, ok := fieldMap[key]
		if !ok {
			continue
		}
		columns = append(columns, fmt.Sprintf("%s = $%d", col, idx))
		args = append(args, update[key])
		idx++
	}

	columns = append(columns, "updated_at = now()")
	args = append(args, merchantID)

	query := "update merchants set " + strings.Join(columns, ", ") + fmt.Sprintf(" where id = $%d", idx)
	if _, err := h.DB.Exec(ctx, query, args...); err != nil {
		return nil, err
	}

	return h.buildMerchantProfilePayload(ctx, merchantID)
}

func (h *Handler) buildMerchantProfilePayload(ctx context.Context, merchantID int64) (map[string]any, error) {
	var (
		code                        string
		name                        string
		email                       string
		phone                       pgtype.Text
		address                     pgtype.Text
		city                        pgtype.Text
		state                       pgtype.Text
		postalCode                  pgtype.Text
		country                     string
		logoURL                     pgtype.Text
		bannerURL                   pgtype.Text
		promoBannerURLs             []string
		mapURL                      pgtype.Text
		description                 pgtype.Text
		isActive                    bool
		enableTax                   bool
		taxPercentage               pgtype.Numeric
		enableServiceCharge         bool
		serviceChargePercent        pgtype.Numeric
		enablePackagingFee          bool
		packagingFeeAmount          pgtype.Numeric
		currency                    string
		createdAt                   time.Time
		updatedAt                   time.Time
		isOpen                      bool
		isManualOverride            bool
		isDineInEnabled             bool
		isTakeawayEnabled           bool
		dineInLabel                 pgtype.Text
		takeawayLabel               pgtype.Text
		deliveryLabel               pgtype.Text
		dineInScheduleStart         pgtype.Text
		dineInScheduleEnd           pgtype.Text
		takeawayScheduleStart       pgtype.Text
		takeawayScheduleEnd         pgtype.Text
		deliveryScheduleStart       pgtype.Text
		deliveryScheduleEnd         pgtype.Text
		isPerDayModeScheduleEnabled bool
		parentMerchantID            pgtype.Int8
		branchType                  string
		totalTables                 pgtype.Int4
		requireTableNumberForDineIn bool
		posPayImmediately           bool
		latitude                    pgtype.Numeric
		longitude                   pgtype.Numeric
		timezone                    string
		isReservationEnabled        bool
		reservationMenuRequired     bool
		reservationMinItemCount     int32
		isScheduledOrderEnabled     bool
		isDeliveryEnabled           bool
		enforceDeliveryZones        bool
		deliveryMaxDistanceKm       pgtype.Numeric
		deliveryFeeBase             pgtype.Numeric
		deliveryFeePerKm            pgtype.Numeric
		deliveryFeeMin              pgtype.Numeric
		deliveryFeeMax              pgtype.Numeric
		deletePin                   pgtype.Text
		receiptSettings             []byte
		featuresBytes               []byte
	)

	err := h.DB.QueryRow(ctx, `
		select id, code, name, email, phone, address, city, state, postal_code, country,
		       logo_url, banner_url, promo_banner_urls, map_url, description,
		       is_active, enable_tax, tax_percentage, enable_service_charge, service_charge_percent,
		       enable_packaging_fee, packaging_fee_amount, currency, created_at, updated_at,
		       is_open, is_manual_override, is_dine_in_enabled, is_takeaway_enabled,
		       dine_in_label, takeaway_label, delivery_label,
		       dine_in_schedule_start, dine_in_schedule_end,
		       takeaway_schedule_start, takeaway_schedule_end,
		       is_per_day_mode_schedule_enabled,
		       parent_merchant_id, branch_type,
		       delivery_schedule_start, delivery_schedule_end,
		       total_tables, require_table_number_for_dine_in,
		       pos_pay_immediately, latitude, longitude, timezone,
		       is_reservation_enabled, reservation_menu_required, reservation_min_item_count,
		       is_scheduled_order_enabled,
		       is_delivery_enabled, enforce_delivery_zones,
		       delivery_max_distance_km, delivery_fee_base, delivery_fee_per_km, delivery_fee_min, delivery_fee_max,
		       delete_pin, receipt_settings, features
		from merchants
		where id = $1
	`, merchantID).Scan(
		&merchantID,
		&code,
		&name,
		&email,
		&phone,
		&address,
		&city,
		&state,
		&postalCode,
		&country,
		&logoURL,
		&bannerURL,
		&promoBannerURLs,
		&mapURL,
		&description,
		&isActive,
		&enableTax,
		&taxPercentage,
		&enableServiceCharge,
		&serviceChargePercent,
		&enablePackagingFee,
		&packagingFeeAmount,
		&currency,
		&createdAt,
		&updatedAt,
		&isOpen,
		&isManualOverride,
		&isDineInEnabled,
		&isTakeawayEnabled,
		&dineInLabel,
		&takeawayLabel,
		&deliveryLabel,
		&dineInScheduleStart,
		&dineInScheduleEnd,
		&takeawayScheduleStart,
		&takeawayScheduleEnd,
		&isPerDayModeScheduleEnabled,
		&parentMerchantID,
		&branchType,
		&deliveryScheduleStart,
		&deliveryScheduleEnd,
		&totalTables,
		&requireTableNumberForDineIn,
		&posPayImmediately,
		&latitude,
		&longitude,
		&timezone,
		&isReservationEnabled,
		&reservationMenuRequired,
		&reservationMinItemCount,
		&isScheduledOrderEnabled,
		&isDeliveryEnabled,
		&enforceDeliveryZones,
		&deliveryMaxDistanceKm,
		&deliveryFeeBase,
		&deliveryFeePerKm,
		&deliveryFeeMin,
		&deliveryFeeMax,
		&deletePin,
		&receiptSettings,
		&featuresBytes,
	)
	if err != nil {
		return nil, err
	}

	openingHours := h.fetchOpeningHours(ctx, merchantID)
	parentMerchant := h.fetchParentMerchant(ctx, parentMerchantID)
	branches := h.fetchBranchMerchants(ctx, merchantID)
	paymentSettings := h.fetchPaymentSettings(ctx, merchantID)
	paymentAccounts := h.fetchPaymentAccounts(ctx, merchantID)
	merchantBalance := h.fetchMerchantBalance(ctx, merchantID)
	owners, staff := h.fetchMerchantTeam(ctx, merchantID)
	completedOrderEmailFee := h.fetchCompletedOrderEmailFee(ctx, currency)

	promoURLs := make([]string, 0, len(promoBannerURLs))
	if promoBannerURLs != nil {
		promoURLs = append(promoURLs, promoBannerURLs...)
	}

	var receiptSettingsValue any = nil
	if len(receiptSettings) > 0 {
		receiptSettingsValue = json.RawMessage(receiptSettings)
	}

	var featuresValue any = nil
	if len(featuresBytes) > 0 {
		featuresValue = json.RawMessage(featuresBytes)
	}

	return map[string]any{
		"id":                          fmt.Sprint(merchantID),
		"code":                        code,
		"name":                        name,
		"email":                       email,
		"phone":                       nullIfEmptyText(phone),
		"phoneNumber":                 defaultStringProfile(nullIfEmptyText(phone)),
		"address":                     nullIfEmptyText(address),
		"city":                        nullIfEmptyText(city),
		"state":                       nullIfEmptyText(state),
		"postalCode":                  nullIfEmptyText(postalCode),
		"country":                     country,
		"logoUrl":                     nullIfEmptyText(logoURL),
		"bannerUrl":                   nullIfEmptyText(bannerURL),
		"promoBannerUrls":             promoURLs,
		"mapUrl":                      nullIfEmptyText(mapURL),
		"description":                 nullIfEmptyText(description),
		"isActive":                    isActive,
		"isOpen":                      isOpen,
		"isManualOverride":            isManualOverride,
		"isDineInEnabled":             isDineInEnabled,
		"isTakeawayEnabled":           isTakeawayEnabled,
		"requireTableNumberForDineIn": requireTableNumberForDineIn,
		"dineInLabel":                 nullIfEmptyText(dineInLabel),
		"takeawayLabel":               nullIfEmptyText(takeawayLabel),
		"deliveryLabel":               nullIfEmptyText(deliveryLabel),
		"dineInScheduleStart":         nullIfEmptyText(dineInScheduleStart),
		"dineInScheduleEnd":           nullIfEmptyText(dineInScheduleEnd),
		"takeawayScheduleStart":       nullIfEmptyText(takeawayScheduleStart),
		"takeawayScheduleEnd":         nullIfEmptyText(takeawayScheduleEnd),
		"deliveryScheduleStart":       nullIfEmptyText(deliveryScheduleStart),
		"deliveryScheduleEnd":         nullIfEmptyText(deliveryScheduleEnd),
		"totalTables":                 nullIfEmptyInt32(totalTables),
		"posPayImmediately":           posPayImmediately,
		"latitude":                    nullableNumeric(latitude),
		"longitude":                   nullableNumeric(longitude),
		"timezone":                    timezone,
		"isReservationEnabled":        isReservationEnabled,
		"reservationMenuRequired":     reservationMenuRequired,
		"reservationMinItemCount":     reservationMinItemCount,
		"isScheduledOrderEnabled":     isScheduledOrderEnabled,
		"isPerDayModeScheduleEnabled": isPerDayModeScheduleEnabled,
		"isDeliveryEnabled":           isDeliveryEnabled,
		"enforceDeliveryZones":        enforceDeliveryZones,
		"deliveryMaxDistanceKm":       nullableNumeric(deliveryMaxDistanceKm),
		"deliveryFeeBase":             nullableNumeric(deliveryFeeBase),
		"deliveryFeePerKm":            nullableNumeric(deliveryFeePerKm),
		"deliveryFeeMin":              nullableNumeric(deliveryFeeMin),
		"deliveryFeeMax":              nullableNumeric(deliveryFeeMax),
		"enableTax":                   enableTax,
		"taxPercentage":               nullableNumeric(taxPercentage),
		"enableServiceCharge":         enableServiceCharge,
		"serviceChargePercent":        nullableNumeric(serviceChargePercent),
		"enablePackagingFee":          enablePackagingFee,
		"packagingFeeAmount":          nullableNumeric(packagingFeeAmount),
		"currency":                    currency,
		"createdAt":                   createdAt,
		"updatedAt":                   updatedAt,
		"parentMerchantId": func() any {
			if parentMerchantID.Valid {
				return fmt.Sprint(parentMerchantID.Int64)
			}
			return nil
		}(),
		"branchType":      branchType,
		"parentMerchant":  parentMerchant,
		"branches":        branches,
		"openingHours":    openingHours,
		"paymentSettings": paymentSettings,
		"paymentAccounts": paymentAccounts,
		"merchantBalance": merchantBalance,
		"features":        featuresValue,
		"receiptSettings": receiptSettingsValue,
		"hasDeletePin":    deletePin.Valid && strings.TrimSpace(deletePin.String) != "",
		"teamSummary": map[string]any{
			"ownerCount": len(owners),
			"staffCount": len(staff),
			"owners":     owners,
			"staff":      staff,
		},
		"completedOrderEmailFee": completedOrderEmailFee,
	}, nil
}

func defaultStringProfile(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func asMap(value any) map[string]any {
	if value == nil {
		return nil
	}
	if m, ok := value.(map[string]any); ok {
		return m
	}
	return nil
}

func asStringSlice(value any, fallback []string) []string {
	if value == nil {
		return fallback
	}
	slice, ok := value.([]any)
	if !ok {
		if strSlice, ok := value.([]string); ok {
			return strSlice
		}
		return fallback
	}
	out := make([]string, 0, len(slice))
	for _, v := range slice {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func toStringValueProfile(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func setIfPresent(dest map[string]any, key string, body map[string]any) {
	if value, ok := body[key]; ok {
		dest[key] = value
	}
}

type responseCapture struct {
	status int
	data   map[string]any
}

func newResponseCapture() *responseCapture {
	return &responseCapture{status: http.StatusOK}
}

func (r *responseCapture) Header() http.Header {
	return http.Header{}
}

func (r *responseCapture) WriteHeader(statusCode int) {
	r.status = statusCode
}

func (r *responseCapture) Write(b []byte) (int, error) {
	var payload map[string]any
	_ = json.Unmarshal(b, &payload)
	r.data = payload
	return len(b), nil
}

func (r *responseCapture) bodyData() map[string]any {
	if r.data == nil {
		return nil
	}
	if data, ok := r.data["data"].(map[string]any); ok {
		return data
	}
	return nil
}
