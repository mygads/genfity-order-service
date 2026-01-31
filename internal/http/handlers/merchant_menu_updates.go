package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/go-chi/chi/v5"
)

func (h *Handler) MerchantMenuAddAddonCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	addonCategoryID, ok := parseNumericID(payload["addonCategoryId"])
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "addonCategoryId is required")
		return
	}

	isRequired := false
	if value, ok := payload["isRequired"].(bool); ok {
		isRequired = value
	}

	displayOrder := int32(0)
	if value, ok := payload["displayOrder"].(float64); ok {
		displayOrder = int32(value)
	}

	var menuMerchantID int64
	if err := h.DB.QueryRow(ctx, `select merchant_id from menus where id = $1 and deleted_at is null`, menuID).Scan(&menuMerchantID); err != nil || menuMerchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	var addonMerchantID int64
	if err := h.DB.QueryRow(ctx, `select merchant_id from addon_categories where id = $1`, addonCategoryID).Scan(&addonMerchantID); err != nil || addonMerchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	var createdMenuID int64
	var createdAddonID int64
	var createdDisplayOrder int32
	var createdIsRequired bool
	if err := h.DB.QueryRow(ctx, `
		insert into menu_addon_categories (menu_id, addon_category_id, display_order, is_required, created_at, updated_at)
		values ($1,$2,$3,$4,now(),now())
		on conflict (menu_id, addon_category_id)
		do update set display_order = excluded.display_order, is_required = excluded.is_required, updated_at = now()
		returning menu_id, addon_category_id, display_order, is_required
	`, menuID, addonCategoryID, displayOrder, isRequired).Scan(
		&createdMenuID, &createdAddonID, &createdDisplayOrder, &createdIsRequired,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to add addon category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"menuId":          int64ToString(createdMenuID),
			"addonCategoryId": int64ToString(createdAddonID),
			"displayOrder":    createdDisplayOrder,
			"isRequired":      createdIsRequired,
		},
		"message":    "Addon category added to menu successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantMenuRemoveAddonCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}
	addonCategoryID, err := parseStringToInt64(chi.URLParam(r, "categoryId"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var menuMerchantID int64
	if err := h.DB.QueryRow(ctx, `select merchant_id from menus where id = $1 and deleted_at is null`, menuID).Scan(&menuMerchantID); err != nil || menuMerchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	result, err := h.DB.Exec(ctx, `
		delete from menu_addon_categories where menu_id = $1 and addon_category_id = $2
	`, menuID, addonCategoryID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove addon category")
		return
	}
	if result.RowsAffected() == 0 {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu addon category relationship not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Addon category removed from menu successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantMenuUpdateCategories(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	categoryIDs := make([]int64, 0)
	raw, ok := body["categoryIds"]
	if !ok {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "categoryIds must be an array")
		return
	}
	switch val := raw.(type) {
	case []any:
		for _, item := range val {
			parsed, ok := parseNumericID(item)
			if !ok {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid categoryIds")
				return
			}
			categoryIDs = append(categoryIDs, parsed)
		}
	default:
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "categoryIds must be an array")
		return
	}

	var menuMerchantID int64
	if err := h.DB.QueryRow(ctx, `select merchant_id from menus where id = $1 and deleted_at is null`, menuID).Scan(&menuMerchantID); err != nil || menuMerchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	if len(categoryIDs) > 0 {
		var count int
		if err := h.DB.QueryRow(ctx, `
			select count(*) from menu_categories where id = any($1) and merchant_id = $2 and deleted_at is null
		`, categoryIDs, *authCtx.MerchantID).Scan(&count); err != nil || count != len(categoryIDs) {
			response.Error(w, http.StatusBadRequest, "NOT_FOUND", "One or more categories not found")
			return
		}
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update categories")
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, `delete from menu_category_items where menu_id = $1`, menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update categories")
		return
	}

	for _, categoryID := range categoryIDs {
		if _, err = tx.Exec(ctx, `
			insert into menu_category_items (menu_id, category_id, created_at)
			values ($1,$2,now())
		`, menuID, categoryID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update categories")
			return
		}
	}

	if _, err = tx.Exec(ctx, `
		update menus set updated_by_user_id = $1, updated_at = now() where id = $2
	`, authCtx.UserID, menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update categories")
		return
	}

	if err = tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update categories")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, false)
	if err != nil || len(menus) == 0 {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
		return
	}

	var updated any
	for _, item := range menus {
		if item.ID == menuID {
			updated = item
			break
		}
	}
	if updated == nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    "Menu categories updated successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantMenuDuplicate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuID, err := parseStringToInt64(chi.URLParam(r, "id"))
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var (
		name               string
		description        *string
		price              float64
		imageURL           *string
		imageThumbURL      *string
		imageThumbMeta     []byte
		stockPhotoID       *int64
		isActive           bool
		trackStock         bool
		stockQty           *int32
		dailyStockTemplate *int32
		autoResetStock     bool
		isSpicy            bool
		isBestSeller       bool
		isSignature        bool
		isRecommended      bool
		scheduleEnabled    bool
		scheduleStartTime  *string
		scheduleEndTime    *string
		lowStockThreshold  *int32
		costPrice          *float64
	)

	if err := h.DB.QueryRow(ctx, `
		select name, description, price, image_url, image_thumb_url, image_thumb_meta,
		       stock_photo_id, is_active, track_stock, stock_qty, daily_stock_template,
		       auto_reset_stock, is_spicy, is_best_seller, is_signature, is_recommended,
		       schedule_enabled, schedule_start_time, schedule_end_time, low_stock_threshold, cost_price
		from menus
		where id = $1 and merchant_id = $2 and deleted_at is null
	`, menuID, *authCtx.MerchantID).Scan(
		&name, &description, &price, &imageURL, &imageThumbURL, &imageThumbMeta,
		&stockPhotoID, &isActive, &trackStock, &stockQty, &dailyStockTemplate,
		&autoResetStock, &isSpicy, &isBestSeller, &isSignature, &isRecommended,
		&scheduleEnabled, &scheduleStartTime, &scheduleEndTime, &lowStockThreshold, &costPrice,
	); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	newName := strings.TrimSpace(name + " (Copy)")

	var newStockQty *int32
	if trackStock {
		if dailyStockTemplate != nil {
			value := *dailyStockTemplate
			newStockQty = &value
		} else {
			value := int32(0)
			newStockQty = &value
		}
	}

	var newMenuID int64
	err = h.DB.QueryRow(ctx, `
		insert into menus (
			merchant_id, name, description, price, image_url, image_thumb_url, image_thumb_meta,
			stock_photo_id, is_active, track_stock, stock_qty, daily_stock_template, auto_reset_stock,
			is_spicy, is_best_seller, is_signature, is_recommended,
			schedule_enabled, schedule_start_time, schedule_end_time, low_stock_threshold, cost_price,
			created_by_user_id, updated_by_user_id, created_at, updated_at
		) values (
			$1,$2,$3,$4,$5,$6,$7,
			$8,$9,$10,$11,$12,$13,
			$14,$15,$16,$17,
			$18,$19,$20,$21,$22,
			$23,$24,now(),now()
		) returning id
	`,
		*authCtx.MerchantID, newName, description, price, imageURL, imageThumbURL, imageThumbMeta,
		stockPhotoID, false, trackStock, newStockQty, dailyStockTemplate, autoResetStock,
		isSpicy, isBestSeller, isSignature, isRecommended,
		scheduleEnabled, scheduleStartTime, scheduleEndTime, lowStockThreshold, costPrice,
		authCtx.UserID, authCtx.UserID,
	).Scan(&newMenuID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to duplicate menu")
		return
	}

	_, _ = h.DB.Exec(ctx, `
		insert into menu_category_items (menu_id, category_id, created_at)
		select $1, category_id, now() from menu_category_items where menu_id = $2
	`, newMenuID, menuID)

	_, _ = h.DB.Exec(ctx, `
		insert into menu_addon_categories (menu_id, addon_category_id, display_order, is_required, created_at, updated_at)
		select $1, addon_category_id, display_order, is_required, now(), now() from menu_addon_categories where menu_id = $2
	`, newMenuID, menuID)

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, false)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to duplicate menu")
		return
	}

	var duplicated any
	for _, item := range menus {
		if item.ID == newMenuID {
			duplicated = item
			break
		}
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       duplicated,
		"message":    "Menu item duplicated successfully",
		"statusCode": 201,
	})
}
