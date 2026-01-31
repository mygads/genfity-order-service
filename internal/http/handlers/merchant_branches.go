package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type branchCreatePayload struct {
	Name             string
	Code             string
	Description      *string
	Address          *string
	PhoneNumber      *string
	Email            *string
	IsOpen           *bool
	Country          *string
	Currency         *string
	Timezone         *string
	Latitude         *float64
	Longitude        *float64
	ParentMerchantID *int64
}

func (h *Handler) MerchantBranchesList(w http.ResponseWriter, r *http.Request) {
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

	ownerMerchants := make([]int64, 0)
	rows, err := h.DB.Query(ctx, `
		select merchant_id
		from merchant_users
		where user_id = $1 and role = 'OWNER' and is_active = true
	`, authCtx.UserID)
	if err != nil {
		h.Logger.Error("merchant branches owner lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch branches")
		return
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ownerMerchants = append(ownerMerchants, id)
	}
	rows.Close()

	if len(ownerMerchants) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"merchants": []map[string]any{},
				"groups":    []map[string]any{},
			},
			"message":    "Branches retrieved successfully",
			"statusCode": 200,
		})
		return
	}

	merchantRows, err := h.DB.Query(ctx, `
		select m.id, m.code, m.name, m.branch_type, m.parent_merchant_id, pm.name,
		       m.is_active, m.currency, m.timezone, m.address, m.city, m.country, m.logo_url
		from merchants m
		left join merchants pm on pm.id = m.parent_merchant_id
		where m.id = any($1)
	`, ownerMerchants)
	if err != nil {
		h.Logger.Error("merchant branches list fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch branches")
		return
	}
	defer merchantRows.Close()

	merchants := make([]map[string]any, 0)
	merchantMap := make(map[int64]map[string]any)
	for merchantRows.Next() {
		var (
			id         int64
			code       string
			name       string
			branchType string
			parentID   pgtype.Int8
			parentName pgtype.Text
			isActive   bool
			currency   string
			timezone   string
			address    pgtype.Text
			city       pgtype.Text
			country    string
			logoURL    pgtype.Text
		)
		if err := merchantRows.Scan(&id, &code, &name, &branchType, &parentID, &parentName, &isActive, &currency, &timezone, &address, &city, &country, &logoURL); err != nil {
			continue
		}
		record := map[string]any{
			"id":                 int64ToString(id),
			"code":               code,
			"name":               name,
			"branchType":         branchType,
			"parentMerchantId":   nil,
			"parentMerchantName": nil,
			"isActive":           isActive,
			"currency":           currency,
			"timezone":           timezone,
			"address":            textOrDefault(address, ""),
			"city":               textOrDefault(city, ""),
			"country":            country,
			"logoUrl":            textOrDefault(logoURL, ""),
			"branches":           []map[string]any{},
		}
		if parentID.Valid {
			record["parentMerchantId"] = int64ToString(parentID.Int64)
			if parentName.Valid {
				record["parentMerchantName"] = parentName.String
			}
		}
		merchants = append(merchants, record)
		merchantMap[id] = record
	}

	branches := make(map[int64][]map[string]any)
	branchRows, err := h.DB.Query(ctx, `
		select id, code, name, branch_type, is_active, parent_merchant_id
		from merchants
		where parent_merchant_id = any($1)
	`, ownerMerchants)
	if err == nil {
		defer branchRows.Close()
		for branchRows.Next() {
			var (
				id         int64
				code       string
				name       string
				branchType string
				isActive   bool
				parentID   int64
			)
			if err := branchRows.Scan(&id, &code, &name, &branchType, &isActive, &parentID); err != nil {
				continue
			}
			branches[parentID] = append(branches[parentID], map[string]any{
				"id":         int64ToString(id),
				"code":       code,
				"name":       name,
				"branchType": branchType,
				"isActive":   isActive,
			})
		}
	}

	for id, merchant := range merchantMap {
		merchant["branches"] = branches[id]
	}

	grouped := make(map[string]map[string]any)
	for _, merchant := range merchants {
		mainID := StringValue(merchant["parentMerchantId"])
		if mainID == "" {
			mainID = StringValue(merchant["id"])
		}
		group := grouped[mainID]
		if group == nil {
			group = map[string]any{
				"main":     merchant,
				"branches": []map[string]any{},
			}
			grouped[mainID] = group
		}
		if StringValue(merchant["parentMerchantId"]) != "" {
			group["branches"] = append(group["branches"].([]map[string]any), merchant)
		} else {
			group["main"] = merchant
		}
	}

	groups := make([]map[string]any, 0, len(grouped))
	for _, group := range grouped {
		groups = append(groups, group)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchants": merchants,
			"groups":    groups,
		},
		"message":    "Branches retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantBranchesCreate(w http.ResponseWriter, r *http.Request) {
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

	payload, err := decodeBranchCreatePayload(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	if payload.Name == "" || payload.Code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant name and code are required")
		return
	}

	parentID := payload.ParentMerchantID
	if parentID == nil {
		parentID = authCtx.MerchantID
	}
	if parentID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_REQUIRED", "Parent merchant is required")
		return
	}

	var (
		parentMerchantID pgtype.Int8
		parentCurrency   pgtype.Text
		parentTimezone   pgtype.Text
		parentCountry    pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `
		select parent_merchant_id, currency, timezone, country
		from merchants where id = $1
	`, *parentID).Scan(&parentMerchantID, &parentCurrency, &parentTimezone, &parentCountry); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Parent merchant not found")
		return
	}

	mainID := *parentID
	if parentMerchantID.Valid {
		mainID = parentMerchantID.Int64
	}

	var ownerLink bool
	if err := h.DB.QueryRow(ctx, `
		select exists(
			select 1 from merchant_users
			where user_id = $1 and merchant_id = $2 and role = 'OWNER' and is_active = true
		)
	`, authCtx.UserID, mainID).Scan(&ownerLink); err != nil || !ownerLink {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "You do not have access to the parent merchant")
		return
	}

	code := strings.ToUpper(strings.TrimSpace(payload.Code))
	if code == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	if exists, _ := h.merchantCodeExists(ctx, code); exists {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < 10; i++ {
			code = randomMerchantCode(rng, 4)
			if exists, _ := h.merchantCodeExists(ctx, code); !exists {
				break
			}
		}
		if exists, _ := h.merchantCodeExists(ctx, code); exists {
			response.Error(w, http.StatusConflict, "MERCHANT_CODE_EXISTS", "Unable to generate unique merchant code")
			return
		}
	}

	currency := textOrDefault(parentCurrency, "AUD")
	if payload.Currency != nil && strings.TrimSpace(*payload.Currency) != "" {
		currency = strings.TrimSpace(*payload.Currency)
	}

	timezone := textOrDefault(parentTimezone, "Australia/Sydney")
	if payload.Timezone != nil && strings.TrimSpace(*payload.Timezone) != "" {
		timezone = strings.TrimSpace(*payload.Timezone)
	}

	country := textOrDefault(parentCountry, "Australia")
	if payload.Country != nil && strings.TrimSpace(*payload.Country) != "" {
		country = strings.TrimSpace(*payload.Country)
	}

	isOpen := true
	if payload.IsOpen != nil {
		isOpen = *payload.IsOpen
	}

	var ownerEmail string
	_ = h.DB.QueryRow(ctx, `select email from users where id = $1`, authCtx.UserID).Scan(&ownerEmail)

	receiptSettings := defaultReceiptSettings()
	if currency == "IDR" {
		receiptSettings["receiptLanguage"] = "id"
	} else {
		receiptSettings["receiptLanguage"] = "en"
	}
	receiptSettings["showEmail"] = true
	receiptSettings["showCustomerPhone"] = true
	receiptSettings["showAddonPrices"] = true
	receiptSettings["showUnitPrice"] = true
	receiptSettings["showCustomFooterText"] = false
	receiptSettings["customFooterText"] = nil
	receiptSettings["customThankYouMessage"] = nil
	receiptSettings["sendCompletedOrderEmailToCustomer"] = false

	features := map[string]any{
		"orderVouchers": map[string]any{
			"posDiscountsEnabled": true,
			"customerEnabled":     false,
		},
		"pos": map[string]any{
			"customItems": map[string]any{
				"enabled": false,
			},
		},
	}

	receiptJSON, _ := json.Marshal(receiptSettings)
	featuresJSON, _ := json.Marshal(features)

	var newID int64
	err = h.DB.QueryRow(ctx, `
		insert into merchants (
			code, name, description, address, phone, email, is_open, country, currency, timezone,
			latitude, longitude, branch_type, parent_merchant_id,
			enable_tax, tax_percentage, enable_service_charge, service_charge_percent,
			enable_packaging_fee, packaging_fee_amount,
			is_dine_in_enabled, is_takeaway_enabled, is_delivery_enabled, enforce_delivery_zones,
			require_table_number_for_dine_in, pos_pay_immediately, is_reservation_enabled,
			is_scheduled_order_enabled, reservation_menu_required, reservation_min_item_count,
			features, receipt_settings, is_active, created_at, updated_at
		) values (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,'BRANCH',$13,
			false,null,false,null,
			false,null,
			true,true,false,true,
			true,true,false,
			false,false,0,
			$14,$15,true,now(),now()
		) returning id
	`,
		code,
		payload.Name,
		payload.Description,
		payload.Address,
		payload.PhoneNumber,
		stringPtrOrNil(payload.Email),
		isOpen,
		country,
		currency,
		timezone,
		payload.Latitude,
		payload.Longitude,
		mainID,
		featuresJSON,
		receiptJSON,
	).Scan(&newID)
	if err != nil {
		h.Logger.Error("branch create failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create branch")
		return
	}

	_, _ = h.DB.Exec(ctx, `
		insert into merchant_users (merchant_id, user_id, role, is_active, permissions, invitation_status, created_at, updated_at)
		values ($1,$2,'OWNER',true,$3,'ACCEPTED',now(),now())
	`, newID, authCtx.UserID, []string{})

	if payload.Email == nil || strings.TrimSpace(*payload.Email) == "" {
		if ownerEmail != "" {
			_, _ = h.DB.Exec(ctx, `update merchants set email = $1 where id = $2`, ownerEmail, newID)
		}
	}

	branch, err := h.fetchBranchSummary(ctx, newID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create branch")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchant": branch,
		},
		"message":    "Branch created successfully",
		"statusCode": 201,
	})
}

func (h *Handler) MerchantBranchesMove(w http.ResponseWriter, r *http.Request) {
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

	body, err := decodeJSONMap(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantID, ok := parseOptionalInt64(body["merchantId"])
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "merchantId is required")
		return
	}
	targetID, ok := parseOptionalInt64(body["targetMainMerchantId"])
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "targetMainMerchantId is required")
		return
	}
	if merchantID == targetID {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Target main merchant must be different")
		return
	}

	var (
		merchantParent pgtype.Int8
		merchantBranch string
		targetParent   pgtype.Int8
		targetBranch   string
	)
	if err := h.DB.QueryRow(ctx, `select parent_merchant_id, branch_type from merchants where id = $1`, merchantID).Scan(&merchantParent, &merchantBranch); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}
	if err := h.DB.QueryRow(ctx, `select parent_merchant_id, branch_type from merchants where id = $1`, targetID).Scan(&targetParent, &targetBranch); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Target main merchant not found")
		return
	}
	if targetParent.Valid || targetBranch != "MAIN" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Target merchant must be a MAIN merchant")
		return
	}

	merchantGroupMain := merchantID
	if merchantParent.Valid {
		merchantGroupMain = merchantParent.Int64
	}

	if !h.ownerHasMerchant(ctx, authCtx.UserID, merchantGroupMain) || !h.ownerHasMerchant(ctx, authCtx.UserID, targetID) {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "You do not have access to this operation")
		return
	}

	if !merchantParent.Valid && merchantBranch == "MAIN" {
		var branchCount int
		_ = h.DB.QueryRow(ctx, `select count(*) from merchants where parent_merchant_id = $1`, merchantID).Scan(&branchCount)
		if branchCount > 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Main merchant still has branches")
			return
		}
	}

	if _, err := h.DB.Exec(ctx, `update merchants set branch_type = 'BRANCH', parent_merchant_id = $1 where id = $2`, targetID, merchantID); err != nil {
		h.Logger.Error("merchant move failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to move merchant")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Merchant moved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantBranchesSetMain(w http.ResponseWriter, r *http.Request) {
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

	body, err := decodeJSONMap(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	merchantID, ok := parseOptionalInt64(body["merchantId"])
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "merchantId is required")
		return
	}

	var (
		currentParent pgtype.Int8
		branchType    string
	)
	if err := h.DB.QueryRow(ctx, `select parent_merchant_id, branch_type from merchants where id = $1`, merchantID).Scan(&currentParent, &branchType); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	currentMain := merchantID
	if currentParent.Valid {
		currentMain = currentParent.Int64
	}

	if !h.ownerHasMerchant(ctx, authCtx.UserID, currentMain) {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "You do not have access to this merchant group")
		return
	}

	if currentMain == merchantID && branchType == "MAIN" {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"message":    "Main branch updated successfully",
			"statusCode": 200,
		})
		return
	}

	batch := `
		update merchants set branch_type = 'MAIN', parent_merchant_id = null where id = $1;
	`
	if _, err := h.DB.Exec(ctx, batch, merchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update main branch")
		return
	}

	if currentMain != merchantID {
		_, _ = h.DB.Exec(ctx, `update merchants set branch_type = 'BRANCH', parent_merchant_id = $1 where id = $2`, merchantID, currentMain)
		_, _ = h.DB.Exec(ctx, `update merchants set parent_merchant_id = $1, branch_type = 'BRANCH' where parent_merchant_id = $2 and id <> $1`, merchantID, currentMain)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Main branch updated successfully",
		"statusCode": 200,
	})
}

func decodeBranchCreatePayload(r *http.Request) (branchCreatePayload, error) {
	body, err := decodeJSONMap(r)
	if err != nil {
		return branchCreatePayload{}, err
	}

	payload := branchCreatePayload{}
	payload.Name = readStringField(body["name"])
	payload.Code = readStringField(body["code"])
	payload.Description = readOptionalString(body["description"])
	payload.Address = readOptionalString(body["address"])
	payload.PhoneNumber = readOptionalString(body["phoneNumber"])
	payload.Email = readOptionalString(body["email"])
	payload.IsOpen = readOptionalBool(body["isOpen"])
	payload.Country = readOptionalString(body["country"])
	payload.Currency = readOptionalString(body["currency"])
	payload.Timezone = readOptionalString(body["timezone"])
	payload.Latitude = readOptionalFloat(body["latitude"])
	payload.Longitude = readOptionalFloat(body["longitude"])
	if parentID, ok := parseOptionalInt64(body["parentMerchantId"]); ok {
		payload.ParentMerchantID = &parentID
	}

	return payload, nil
}

func (h *Handler) merchantCodeExists(ctx context.Context, code string) (bool, error) {
	var exists bool
	err := h.DB.QueryRow(ctx, `select exists(select 1 from merchants where code = $1)`, code).Scan(&exists)
	return exists, err
}

func randomMerchantCode(rng *rand.Rand, length int) string {
	letters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	out := make([]rune, length)
	for i := 0; i < length; i++ {
		out[i] = letters[rng.Intn(len(letters))]
	}
	return string(out)
}

func (h *Handler) ownerHasMerchant(ctx context.Context, userID int64, merchantID int64) bool {
	var ok bool
	_ = h.DB.QueryRow(ctx, `
		select exists(
			select 1 from merchant_users
			where user_id = $1 and merchant_id = $2 and role = 'OWNER' and is_active = true
		)
	`, userID, merchantID).Scan(&ok)
	return ok
}

func (h *Handler) fetchBranchSummary(ctx context.Context, merchantID int64) (map[string]any, error) {
	var (
		id         int64
		code       string
		name       string
		branchType string
		parentID   pgtype.Int8
		isActive   bool
		currency   string
		timezone   string
		address    pgtype.Text
		city       pgtype.Text
		country    string
		logoURL    pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `
		select id, code, name, branch_type, parent_merchant_id, is_active, currency, timezone, address, city, country, logo_url
		from merchants where id = $1
	`, merchantID).Scan(&id, &code, &name, &branchType, &parentID, &isActive, &currency, &timezone, &address, &city, &country, &logoURL); err != nil {
		return nil, err
	}

	result := map[string]any{
		"id":               int64ToString(id),
		"code":             code,
		"name":             name,
		"branchType":       branchType,
		"parentMerchantId": nil,
		"isActive":         isActive,
		"currency":         currency,
		"timezone":         timezone,
		"address":          textOrDefault(address, ""),
		"city":             textOrDefault(city, ""),
		"country":          country,
		"logoUrl":          textOrDefault(logoURL, ""),
	}
	if parentID.Valid {
		result["parentMerchantId"] = int64ToString(parentID.Int64)
	}
	return result, nil
}

func readStringField(value any) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return strings.TrimSpace(str)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func readOptionalString(value any) *string {
	if value == nil {
		return nil
	}
	str := readStringField(value)
	if str == "" {
		return nil
	}
	return &str
}

func readOptionalBool(value any) *bool {
	if value == nil {
		return nil
	}
	if v, ok := value.(bool); ok {
		return &v
	}
	return nil
}

func readOptionalFloat(value any) *float64 {
	if value == nil {
		return nil
	}
	if v, ok := value.(float64); ok {
		return &v
	}
	return nil
}

func parseOptionalInt64(value any) (int64, bool) {
	return parseNumericID(value)
}

func stringPtrOrNil(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func decodeJSONMap(r *http.Request) (map[string]any, error) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body, nil
}
