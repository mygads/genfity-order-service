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
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type bulkMenuOperationPayload struct {
	Operation      string              `json:"operation"`
	MenuIDs        []any               `json:"menuIds"`
	PriceChange    *bulkPriceChange    `json:"priceChange"`
	StockChange    *bulkStockChange    `json:"stockChange"`
	StatusChange   *bulkStatusChange   `json:"statusChange"`
	ScheduleChange *bulkScheduleChange `json:"scheduleChange"`
}

type bulkPriceChange struct {
	Type      string  `json:"type"`
	Value     float64 `json:"value"`
	Direction string  `json:"direction"`
}

type bulkStockChange struct {
	Type           string `json:"type"`
	Value          int32  `json:"value"`
	UpdateTemplate *bool  `json:"updateTemplate"`
}

type bulkStatusChange struct {
	IsActive bool `json:"isActive"`
}

type bulkScheduleChange struct {
	ScheduleEnabled   bool    `json:"scheduleEnabled"`
	ScheduleStartTime *string `json:"scheduleStartTime"`
	ScheduleEndTime   *string `json:"scheduleEndTime"`
	ScheduleDays      []int32 `json:"scheduleDays"`
}

type bulkMenuDeletePayload struct {
	IDs []any `json:"ids"`
}

type bulkMenuSoftDeletePayload struct {
	IDs               []any  `json:"ids"`
	ConfirmationToken string `json:"confirmationToken"`
}

type bulkMenuUpdateStatusPayload struct {
	IDs      []any `json:"ids"`
	IsActive *bool `json:"isActive"`
}

type bulkMenuUploadPayload struct {
	Items        []bulkMenuUploadInput `json:"items"`
	UpsertByName bool                  `json:"upsertByName"`
}

type bulkMenuUploadInput struct {
	ID                 *string  `json:"id"`
	Name               string   `json:"name"`
	Description        *string  `json:"description"`
	Price              float64  `json:"price"`
	CategoryIDs        []string `json:"categoryIds"`
	IsActive           *bool    `json:"isActive"`
	IsSpicy            *bool    `json:"isSpicy"`
	IsBestSeller       *bool    `json:"isBestSeller"`
	IsSignature        *bool    `json:"isSignature"`
	IsRecommended      *bool    `json:"isRecommended"`
	TrackStock         *bool    `json:"trackStock"`
	StockQty           *int32   `json:"stockQty"`
	DailyStockTemplate *int32   `json:"dailyStockTemplate"`
	AutoResetStock     *bool    `json:"autoResetStock"`
}

type bulkStockUpdatePayload struct {
	Updates []bulkStockUpdateItem `json:"updates"`
}

type bulkStockUpdateItem struct {
	Type            string `json:"type"`
	ID              int64  `json:"id"`
	StockQty        *int32 `json:"stockQty"`
	ResetToTemplate *bool  `json:"resetToTemplate"`
}

type bulkStockUpdateResult struct {
	Success       bool    `json:"success"`
	Type          string  `json:"type"`
	ID            int64   `json:"id"`
	Name          string  `json:"name"`
	PreviousStock *int32  `json:"previousStock"`
	NewStock      *int32  `json:"newStock"`
	Error         *string `json:"error,omitempty"`
}

func (h *Handler) MerchantMenuBulk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkMenuOperationPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.MenuIDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "No menu items selected")
		return
	}

	count, err := h.countMenus(ctx, menuIDs, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate menus")
		return
	}
	if count != int64(len(menuIDs)) {
		response.Error(w, http.StatusBadRequest, "INVALID_MENUS", "Some menus do not exist or do not belong to your merchant")
		return
	}

	var affected int64
	switch strings.ToUpper(strings.TrimSpace(payload.Operation)) {
	case "UPDATE_PRICE":
		if payload.PriceChange == nil {
			response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Price change parameters required")
			return
		}
		count, err := h.handleMenuBulkPriceUpdate(ctx, menuIDs, payload.PriceChange, authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update prices")
			return
		}
		affected = count
	case "UPDATE_STOCK":
		if payload.StockChange == nil {
			response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Stock change parameters required")
			return
		}
		count, err := h.handleMenuBulkStockUpdate(ctx, menuIDs, payload.StockChange, authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update stock")
			return
		}
		affected = count
	case "TOGGLE_STATUS":
		if payload.StatusChange == nil {
			response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Status change parameters required")
			return
		}
		count, err := h.handleMenuBulkStatusUpdate(ctx, menuIDs, payload.StatusChange, authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update status")
			return
		}
		affected = count
	case "DELETE":
		count, err := h.handleMenuBulkSoftDelete(ctx, menuIDs, authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete menus")
			return
		}
		affected = count
	case "UPDATE_SCHEDULE":
		if payload.ScheduleChange == nil {
			response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Schedule change parameters required")
			return
		}
		count, err := h.handleMenuBulkScheduleUpdate(ctx, menuIDs, payload.ScheduleChange, authCtx.UserID)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update schedule")
			return
		}
		affected = count
	default:
		response.Error(w, http.StatusBadRequest, "INVALID_OPERATION", "Unknown operation type")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"operation":     strings.ToUpper(strings.TrimSpace(payload.Operation)),
			"affectedCount": affected,
		},
		"message": fmt.Sprintf("Successfully updated %d menu items", affected),
	})
}

func (h *Handler) MerchantMenuBulkDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkMenuDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.IDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Menu item IDs are required")
		return
	}

	count, err := h.countMenus(ctx, menuIDs, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate menus")
		return
	}
	if count != int64(len(menuIDs)) {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Some menu items not found or already deleted")
		return
	}

	result, err := h.DB.Exec(ctx, `
		update menus
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1) and merchant_id = $3 and deleted_at is null
	`, toInt8Array(menuIDs), authCtx.UserID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete menu items")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d menu item(s)", result.RowsAffected()),
		"data": map[string]any{
			"count": result.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantMenuBulkRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload struct {
		IDs []any `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.IDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of menu IDs")
		return
	}
	if len(menuIDs) > 100 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Cannot restore more than 100 items at once")
		return
	}

	result, err := h.DB.Exec(ctx, `
		update menus
		set deleted_at = null,
			deleted_by_user_id = null,
			restored_at = now(),
			restored_by_user_id = $2,
			is_active = true,
			updated_at = now(),
			updated_by_user_id = $2
		where id = any($1) and deleted_at is not null
	`, toInt8Array(menuIDs), authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "BULK_RESTORE_FAILED", "Failed to bulk restore menus")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully restored %d menu items", result.RowsAffected()),
		"data": map[string]any{
			"success":       true,
			"restoredCount": result.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantMenuBulkSoftDeleteToken(w http.ResponseWriter, r *http.Request) {
	idsParam := strings.TrimSpace(r.URL.Query().Get("ids"))
	if idsParam == "" {
		response.Error(w, http.StatusBadRequest, "MISSING_IDS", "Please provide ids as query parameter")
		return
	}

	ids := parseIDQueryList(idsParam)
	if len(ids) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "No valid IDs provided")
		return
	}

	token := buildBulkMenuToken(ids)

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"itemCount":         len(ids),
			"confirmationToken": token,
			"message":           fmt.Sprintf("This will delete %d menu items. Use this token to confirm.", len(ids)),
		},
	})
}

func (h *Handler) MerchantMenuBulkSoftDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkMenuSoftDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.IDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of menu IDs")
		return
	}
	if len(menuIDs) > 100 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Cannot delete more than 100 items at once")
		return
	}

	expectedToken := buildBulkMenuToken(menuIDs)
	if payload.ConfirmationToken != expectedToken {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "CONFIRMATION_REQUIRED",
			"message": "Please confirm this bulk delete operation",
			"data": map[string]any{
				"itemCount":         len(menuIDs),
				"confirmationToken": expectedToken,
			},
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	result, err := h.DB.Exec(ctx, `
		update menus
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1) and deleted_at is null
	`, toInt8Array(menuIDs), authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "BULK_DELETE_FAILED", "Failed to bulk delete menus")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d menu items", result.RowsAffected()),
		"data": map[string]any{
			"success":      true,
			"deletedCount": result.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantMenuBulkUpdateStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkMenuUpdateStatusPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.IDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Menu item IDs are required")
		return
	}
	if payload.IsActive == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "isActive must be a boolean")
		return
	}

	count, err := h.countMenus(ctx, menuIDs, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate menus")
		return
	}
	if count != int64(len(menuIDs)) {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Some menu items not found or unauthorized")
		return
	}

	result, err := h.DB.Exec(ctx, `
		update menus
		set is_active = $2, updated_at = now(), updated_by_user_id = $3
		where id = any($1) and merchant_id = $4
	`, toInt8Array(menuIDs), *payload.IsActive, authCtx.UserID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu items")
		return
	}

	statusLabel := "deactivated"
	if *payload.IsActive {
		statusLabel = "activated"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully %s %d menu item(s)", statusLabel, result.RowsAffected()),
		"data": map[string]any{
			"count": result.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantMenuBulkUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	var payload bulkMenuUploadPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if len(payload.Items) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "No items provided")
		return
	}
	if len(payload.Items) > 100 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Maximum 100 items allowed per upload")
		return
	}

	validCategoryIDs, err := h.fetchMerchantCategoryIDs(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate categories")
		return
	}

	menuNameMap, err := h.fetchMerchantMenuNames(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate menus")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to start bulk upload")
		return
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	createdCount := 0
	updatedCount := 0
	processedIDs := make([]int64, 0)

	for _, item := range payload.Items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Menu name is required")
			return
		}
		if item.Price < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Valid price is required for \"%s\"", name))
			return
		}

		validatedCategoryIDs := make([]int64, 0)
		for _, rawID := range item.CategoryIDs {
			parsed, err := parseStringToInt64(rawID)
			if err != nil {
				continue
			}
			if validCategoryIDs[parsed] {
				validatedCategoryIDs = append(validatedCategoryIDs, parsed)
			}
		}

		var primaryCategoryID *int64
		if len(validatedCategoryIDs) > 0 {
			primaryCategoryID = &validatedCategoryIDs[0]
		}

		existingID := int64(0)
		if item.ID != nil {
			parsed, err := parseStringToInt64(*item.ID)
			if err != nil {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
				return
			}
			existingID = parsed
		} else if payload.UpsertByName {
			if matched, ok := menuNameMap[strings.ToLower(strings.TrimSpace(name))]; ok {
				existingID = matched
			}
		}

		isActive := true
		if item.IsActive != nil {
			isActive = *item.IsActive
		}
		isSpicy := item.IsSpicy != nil && *item.IsSpicy
		isBestSeller := item.IsBestSeller != nil && *item.IsBestSeller
		isSignature := item.IsSignature != nil && *item.IsSignature
		isRecommended := item.IsRecommended != nil && *item.IsRecommended
		trackStock := item.TrackStock != nil && *item.TrackStock
		autoResetStock := trackStock && item.AutoResetStock != nil && *item.AutoResetStock

		var stockQty *int32
		var dailyTemplate *int32
		if trackStock {
			if item.StockQty != nil {
				stockQty = item.StockQty
			} else {
				zero := int32(0)
				stockQty = &zero
			}
			dailyTemplate = item.DailyStockTemplate
		}

		if existingID != 0 {
			_, err := tx.Exec(ctx, `
				update menus set
					name = $1,
					description = $2,
					price = $3,
					category_id = $4,
					is_active = $5,
					is_spicy = $6,
					is_best_seller = $7,
					is_signature = $8,
					is_recommended = $9,
					track_stock = $10,
					stock_qty = $11,
					daily_stock_template = $12,
					auto_reset_stock = $13,
					updated_at = now()
				where id = $14
			`, name, item.Description, item.Price, primaryCategoryID, isActive, isSpicy, isBestSeller,
				isSignature, isRecommended, trackStock, stockQty, dailyTemplate, autoResetStock, existingID)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu")
				return
			}

			_, _ = tx.Exec(ctx, "delete from menu_category_items where menu_id = $1", existingID)
			if len(validatedCategoryIDs) > 0 {
				for _, categoryID := range validatedCategoryIDs {
					_, _ = tx.Exec(ctx, `
						insert into menu_category_items (menu_id, category_id) values ($1, $2)
					`, existingID, categoryID)
				}
			}

			updatedCount++
			processedIDs = append(processedIDs, existingID)
		} else {
			var newID int64
			err := tx.QueryRow(ctx, `
				insert into menus (
					merchant_id, name, description, price, category_id, is_active, is_spicy, is_best_seller,
					is_signature, is_recommended, track_stock, stock_qty, daily_stock_template, auto_reset_stock,
					created_at, updated_at, created_by_user_id, updated_by_user_id
				) values (
					$1, $2, $3, $4, $5, $6, $7, $8,
					$9, $10, $11, $12, $13, $14,
					now(), now(), $15, $15
				) returning id
			`, *authCtx.MerchantID, name, item.Description, item.Price, primaryCategoryID, isActive, isSpicy,
				isBestSeller, isSignature, isRecommended, trackStock, stockQty, dailyTemplate, autoResetStock,
				authCtx.UserID).Scan(&newID)
			if err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu")
				return
			}

			if len(validatedCategoryIDs) > 0 {
				for _, categoryID := range validatedCategoryIDs {
					_, _ = tx.Exec(ctx, `
						insert into menu_category_items (menu_id, category_id) values ($1, $2)
					`, newID, categoryID)
				}
			}
			createdCount++
			processedIDs = append(processedIDs, newID)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to process menu items")
		return
	}

	processed := make([]merchantMenu, 0)
	if len(processedIDs) > 0 {
		menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
		if err == nil {
			idSet := make(map[int64]bool)
			for _, id := range processedIDs {
				idSet[id] = true
			}
			for _, menu := range menus {
				if idSet[menu.ID] {
					processed = append(processed, menu)
				}
			}
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"message":      fmt.Sprintf("Successfully processed %d menu items (%d created, %d updated)", len(payload.Items), createdCount, updatedCount),
		"createdCount": createdCount,
		"updatedCount": updatedCount,
		"totalCount":   len(payload.Items),
		"data":         processed,
	})
}

func (h *Handler) MerchantMenuResetStock(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var merchantExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from merchants where id = $1)", *authCtx.MerchantID).Scan(&merchantExists); err != nil || !merchantExists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	resetCount, err := h.resetMenuDailyStock(ctx, *authCtx.MerchantID, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reset stock")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"resetCount": resetCount,
			"merchantId": fmt.Sprintf("%d", *authCtx.MerchantID),
			"resetAt":    time.Now().UTC().Format(time.RFC3339),
		},
		"message":    fmt.Sprintf("Successfully reset stock for %d menu items", resetCount),
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuStockOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var merchant struct {
		ID       int64
		Name     string
		Currency string
	}

	if err := h.DB.QueryRow(ctx, "select id, name, currency from merchants where id = $1", *authCtx.MerchantID).Scan(&merchant.ID, &merchant.Name, &merchant.Currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	menuItems, err := h.fetchMenuStockItems(ctx, merchant.ID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock overview")
		return
	}
	addonItems, err := h.fetchAddonStockItems(ctx, merchant.ID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock overview")
		return
	}

	allItems := append(menuItems, addonItems...)

	stats := map[string]any{
		"total":        len(allItems),
		"menus":        len(menuItems),
		"addons":       len(addonItems),
		"lowStock":     countLowStock(allItems),
		"outOfStock":   countOutOfStock(allItems),
		"healthy":      countHealthyStock(allItems),
		"withTemplate": countWithTemplate(allItems),
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"items":    allItems,
			"stats":    stats,
			"merchant": merchant,
		},
		"message":    "Stock overview retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuStockBulkUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Anda harus login terlebih dahulu")
		return
	}
	if authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID tidak ditemukan")
		return
	}

	var payload bulkStockUpdatePayload
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "VALIDATION_ERROR",
			"message":    "Data tidak valid",
			"details":    []string{err.Error()},
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	if len(payload.Updates) == 0 {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "VALIDATION_ERROR",
			"message":    "Data tidak valid",
			"details":    []string{"updates must not be empty"},
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	results := make([]bulkStockUpdateResult, 0, len(payload.Updates))

	for _, update := range payload.Updates {
		switch update.Type {
		case "menu":
			result := h.applyMenuStockUpdate(ctx, authCtx, update)
			results = append(results, result)
		case "addon":
			result := h.applyAddonStockUpdate(ctx, authCtx, update)
			results = append(results, result)
		default:
			errMsg := "Tipe item tidak valid"
			results = append(results, bulkStockUpdateResult{
				Success: false,
				Type:    update.Type,
				ID:      update.ID,
				Name:    "Unknown",
				Error:   &errMsg,
			})
		}
	}

	successCount := 0
	failCount := 0
	for _, res := range results {
		if res.Success {
			successCount++
		} else {
			failCount++
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"results": results,
			"summary": map[string]any{
				"total":   len(results),
				"success": successCount,
				"failed":  failCount,
			},
		},
		"message":    fmt.Sprintf("Berhasil update %d item, %d gagal", successCount, failCount),
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuAddStock(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var payload struct {
		Quantity int32 `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Quantity <= 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Quantity must be greater than 0")
		return
	}

	var trackStock bool
	var stockQty pgtype.Int4
	if err := h.DB.QueryRow(ctx, `
		select track_stock, stock_qty
		from menus
		where id = $1 and merchant_id = $2 and deleted_at is null
	`, menuID, *authCtx.MerchantID).Scan(&trackStock, &stockQty); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu item not found")
		return
	}

	if !trackStock {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Menu item does not track stock")
		return
	}

	currentStock := int32(0)
	if stockQty.Valid {
		currentStock = stockQty.Int32
	}

	newStock := currentStock + payload.Quantity

	if _, err := h.DB.Exec(ctx, `
		update menus
		set stock_qty = $2, is_active = true, updated_at = now(), updated_by_user_id = $3
		where id = $1
	`, menuID, newStock, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to add stock")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to add stock")
		return
	}

	var updated *merchantMenu
	for i := range menus {
		if menus[i].ID == menuID {
			updated = &menus[i]
			break
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    fmt.Sprintf("Added %d stock successfully", payload.Quantity),
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuRebuildThumbnails(w http.ResponseWriter, r *http.Request) {
	response.JSON(w, http.StatusNotImplemented, map[string]any{
		"success":    false,
		"error":      "NOT_IMPLEMENTED",
		"message":    "Thumbnail rebuild is not implemented yet.",
		"statusCode": http.StatusNotImplemented,
	})
}

func (h *Handler) countMenus(ctx context.Context, menuIDs []int64, merchantID int64) (int64, error) {
	var count int64
	if err := h.DB.QueryRow(ctx, `
		select count(*)
		from menus
		where id = any($1) and merchant_id = $2 and deleted_at is null
	`, toInt8Array(menuIDs), merchantID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (h *Handler) handleMenuBulkPriceUpdate(ctx context.Context, menuIDs []int64, change *bulkPriceChange, userID int64) (int64, error) {
	direction := strings.ToUpper(strings.TrimSpace(change.Direction))
	changeType := strings.ToUpper(strings.TrimSpace(change.Type))

	if direction == "SET" {
		result, err := h.DB.Exec(ctx, `
			update menus
			set price = $2, updated_at = now(), updated_by_user_id = $3
			where id = any($1)
		`, toInt8Array(menuIDs), change.Value, userID)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected(), nil
	}

	rows, err := h.DB.Query(ctx, `
		select id, price
		from menus
		where id = any($1)
	`, toInt8Array(menuIDs))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	updated := int64(0)
	for rows.Next() {
		var id int64
		var price pgtype.Numeric
		if err := rows.Scan(&id, &price); err != nil {
			continue
		}

		current := utils.NumericToFloat64(price)
		newPrice := current

		if changeType == "FIXED" {
			if direction == "INCREASE" {
				newPrice = current + change.Value
			} else {
				newPrice = current - change.Value
			}
		} else {
			changeAmount := current * (change.Value / 100)
			if direction == "INCREASE" {
				newPrice = current + changeAmount
			} else {
				newPrice = current - changeAmount
			}
		}

		if newPrice < 0 {
			newPrice = 0
		}

		if _, err := h.DB.Exec(ctx, `
			update menus
			set price = $2, updated_at = now(), updated_by_user_id = $3
			where id = $1
		`, id, newPrice, userID); err == nil {
			updated++
		}
	}

	return updated, nil
}

func (h *Handler) handleMenuBulkStockUpdate(ctx context.Context, menuIDs []int64, change *bulkStockChange, userID int64) (int64, error) {
	changeType := strings.ToUpper(strings.TrimSpace(change.Type))
	updateTemplate := change.UpdateTemplate != nil && *change.UpdateTemplate

	if changeType == "SET" {
		setQuery := `
			update menus
			set stock_qty = $2, track_stock = true, updated_at = now(), updated_by_user_id = $3`
		if updateTemplate {
			setQuery += ", daily_stock_template = $2"
		}
		setQuery += " where id = any($1)"

		result, err := h.DB.Exec(ctx, setQuery, toInt8Array(menuIDs), change.Value, userID)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected(), nil
	}

	rows, err := h.DB.Query(ctx, `
		select id, stock_qty, daily_stock_template
		from menus
		where id = any($1)
	`, toInt8Array(menuIDs))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	updated := int64(0)
	for rows.Next() {
		var id int64
		var stockQty pgtype.Int4
		var template pgtype.Int4
		if err := rows.Scan(&id, &stockQty, &template); err != nil {
			continue
		}

		current := int32(0)
		if stockQty.Valid {
			current = stockQty.Int32
		}
		newStock := current
		if changeType == "ADD" {
			newStock = current + change.Value
		} else {
			newStock = current - change.Value
		}
		if newStock < 0 {
			newStock = 0
		}

		query := `
			update menus
			set stock_qty = $2, track_stock = true, updated_at = now(), updated_by_user_id = $3`
		args := []any{id, newStock, userID}
		if updateTemplate {
			query += ", daily_stock_template = $4"
			args = append(args, newStock)
		}
		query += " where id = $1"

		if _, err := h.DB.Exec(ctx, query, args...); err == nil {
			updated++
		}
	}

	return updated, nil
}

func (h *Handler) handleMenuBulkStatusUpdate(ctx context.Context, menuIDs []int64, change *bulkStatusChange, userID int64) (int64, error) {
	result, err := h.DB.Exec(ctx, `
		update menus
		set is_active = $2, updated_at = now(), updated_by_user_id = $3
		where id = any($1)
	`, toInt8Array(menuIDs), change.IsActive, userID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (h *Handler) handleMenuBulkSoftDelete(ctx context.Context, menuIDs []int64, userID int64) (int64, error) {
	result, err := h.DB.Exec(ctx, `
		update menus
		set deleted_at = now(), deleted_by_user_id = $2, is_active = false
		where id = any($1)
	`, toInt8Array(menuIDs), userID)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (h *Handler) handleMenuBulkScheduleUpdate(ctx context.Context, menuIDs []int64, change *bulkScheduleChange, userID int64) (int64, error) {
	setClauses := []string{"schedule_enabled = $2", "updated_at = now()", "updated_by_user_id = $3"}
	args := []any{toInt8Array(menuIDs), change.ScheduleEnabled, userID}
	idx := 4

	if change.ScheduleStartTime != nil {
		setClauses = append(setClauses, fmt.Sprintf("schedule_start_time = $%d", idx))
		args = append(args, change.ScheduleStartTime)
		idx++
	}
	if change.ScheduleEndTime != nil {
		setClauses = append(setClauses, fmt.Sprintf("schedule_end_time = $%d", idx))
		args = append(args, change.ScheduleEndTime)
		idx++
	}
	if change.ScheduleDays != nil {
		setClauses = append(setClauses, fmt.Sprintf("schedule_days = $%d", idx))
		args = append(args, change.ScheduleDays)
		idx++
	}

	query := "update menus set " + strings.Join(setClauses, ", ") + " where id = any($1)"
	result, err := h.DB.Exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (h *Handler) fetchMerchantCategoryIDs(ctx context.Context, merchantID int64) (map[int64]bool, error) {
	rows, err := h.DB.Query(ctx, `
		select id from menu_categories where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make(map[int64]bool)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			ids[id] = true
		}
	}
	return ids, nil
}

func (h *Handler) fetchMerchantMenuNames(ctx context.Context, merchantID int64) (map[string]int64, error) {
	rows, err := h.DB.Query(ctx, `
		select id, name from menus where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nameMap := make(map[string]int64)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err == nil {
			nameMap[strings.ToLower(strings.TrimSpace(name))] = id
		}
	}
	return nameMap, nil
}

func (h *Handler) resetMenuDailyStock(ctx context.Context, merchantID int64, userID int64) (int64, error) {
	rows, err := h.DB.Query(ctx, `
		select id, daily_stock_template
		from menus
		where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	resetCount := int64(0)
	for rows.Next() {
		var id int64
		var template pgtype.Int4
		if err := rows.Scan(&id, &template); err != nil {
			continue
		}
		if !template.Valid {
			continue
		}
		isActive := template.Int32 > 0
		if result, err := h.DB.Exec(ctx, `
			update menus
			set stock_qty = $2,
				is_active = $3,
				last_stock_reset_at = now(),
				updated_at = now(),
				updated_by_user_id = $4
			where id = $1 and auto_reset_stock = true
		`, id, template.Int32, isActive, userID); err == nil {
			if result.RowsAffected() > 0 {
				resetCount++
			}
		}
	}
	return resetCount, nil
}

type stockOverviewItem struct {
	ID                 int64   `json:"id"`
	Type               string  `json:"type"`
	Name               string  `json:"name"`
	CategoryName       string  `json:"categoryName"`
	StockQty           *int32  `json:"stockQty"`
	DailyStockTemplate *int32  `json:"dailyStockTemplate"`
	AutoResetStock     bool    `json:"autoResetStock"`
	IsActive           bool    `json:"isActive"`
	ImageURL           *string `json:"imageUrl"`
}

func (h *Handler) fetchMenuStockItems(ctx context.Context, merchantID int64) ([]stockOverviewItem, error) {
	rows, err := h.DB.Query(ctx, `
		select m.id, m.name, m.category_id, c.name, m.stock_qty, m.daily_stock_template,
			m.auto_reset_stock, m.is_active, m.image_url
		from menus m
		left join menu_categories c on c.id = m.category_id
		where m.merchant_id = $1 and m.track_stock = true and m.deleted_at is null
		order by m.name asc
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]stockOverviewItem, 0)
	menuIDs := make([]int64, 0)
	categoryFallback := make(map[int64]string)

	for rows.Next() {
		var item stockOverviewItem
		var categoryID pgtype.Int8
		var categoryName pgtype.Text
		var stockQty pgtype.Int4
		var dailyTemplate pgtype.Int4
		var imageURL pgtype.Text

		if err := rows.Scan(&item.ID, &item.Name, &categoryID, &categoryName, &stockQty, &dailyTemplate, &item.AutoResetStock, &item.IsActive, &imageURL); err != nil {
			continue
		}
		item.Type = "menu"
		item.CategoryName = "Uncategorized"
		if categoryName.Valid {
			item.CategoryName = categoryName.String
		}
		if stockQty.Valid {
			item.StockQty = &stockQty.Int32
		}
		if dailyTemplate.Valid {
			item.DailyStockTemplate = &dailyTemplate.Int32
		}
		if imageURL.Valid {
			item.ImageURL = &imageURL.String
		}
		items = append(items, item)
		menuIDs = append(menuIDs, item.ID)
	}

	if len(menuIDs) == 0 {
		return items, nil
	}

	categoryFallback, _ = h.fetchMenuCategoryNames(ctx, menuIDs)
	for i := range items {
		if items[i].CategoryName == "Uncategorized" {
			if name, ok := categoryFallback[items[i].ID]; ok {
				items[i].CategoryName = name
			}
		}
	}

	return items, nil
}

func (h *Handler) fetchMenuCategoryNames(ctx context.Context, menuIDs []int64) (map[int64]string, error) {
	rows, err := h.DB.Query(ctx, `
		select mci.menu_id, c.name
		from menu_category_items mci
		join menu_categories c on c.id = mci.category_id
		where mci.menu_id = any($1) and c.deleted_at is null
		order by c.name asc
	`, toInt8Array(menuIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]string)
	for rows.Next() {
		var menuID int64
		var name string
		if err := rows.Scan(&menuID, &name); err == nil {
			if _, exists := result[menuID]; !exists {
				result[menuID] = name
			}
		}
	}
	return result, nil
}

func (h *Handler) fetchAddonStockItems(ctx context.Context, merchantID int64) ([]stockOverviewItem, error) {
	rows, err := h.DB.Query(ctx, `
		select ai.id, ai.name, ac.name, ai.stock_qty, ai.daily_stock_template,
			ai.auto_reset_stock, ai.is_active
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ac.merchant_id = $1 and ai.track_stock = true and ai.deleted_at is null
		order by ai.name asc
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]stockOverviewItem, 0)
	for rows.Next() {
		var item stockOverviewItem
		var categoryName string
		var stockQty pgtype.Int4
		var dailyTemplate pgtype.Int4

		if err := rows.Scan(&item.ID, &item.Name, &categoryName, &stockQty, &dailyTemplate, &item.AutoResetStock, &item.IsActive); err != nil {
			continue
		}
		item.Type = "addon"
		item.CategoryName = categoryName
		if stockQty.Valid {
			item.StockQty = &stockQty.Int32
		}
		if dailyTemplate.Valid {
			item.DailyStockTemplate = &dailyTemplate.Int32
		}
		items = append(items, item)
	}
	return items, nil
}

func (h *Handler) applyMenuStockUpdate(ctx context.Context, authCtx *middleware.AuthContext, update bulkStockUpdateItem) bulkStockUpdateResult {
	result := bulkStockUpdateResult{
		Success: false,
		Type:    "menu",
		ID:      update.ID,
		Name:    "Unknown",
	}

	var name string
	var stockQty pgtype.Int4
	var template pgtype.Int4
	var lastReset pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select name, stock_qty, daily_stock_template, last_stock_reset_at
		from menus
		where id = $1 and merchant_id = $2 and deleted_at is null
	`, update.ID, *authCtx.MerchantID).Scan(&name, &stockQty, &template, &lastReset); err != nil {
		errMsg := "Menu tidak ditemukan"
		result.Error = &errMsg
		return result
	}

	result.Name = name
	if stockQty.Valid {
		result.PreviousStock = &stockQty.Int32
	}

	newStockQty := (*int32)(nil)
	if update.ResetToTemplate != nil && *update.ResetToTemplate {
		if template.Valid {
			newStockQty = &template.Int32
		} else {
			errMsg := "Template stok harian belum diatur"
			result.Error = &errMsg
			return result
		}
	} else if update.StockQty != nil {
		newStockQty = update.StockQty
	} else {
		errMsg := "stockQty atau resetToTemplate harus diisi"
		result.Error = &errMsg
		return result
	}

	_, err := h.DB.Exec(ctx, `
		update menus
		set stock_qty = $2,
			last_stock_reset_at = case when $3 then now() else last_stock_reset_at end,
			updated_by_user_id = $4
		where id = $1
	`, update.ID, newStockQty, update.ResetToTemplate != nil && *update.ResetToTemplate, authCtx.UserID)
	if err != nil {
		errMsg := "Terjadi kesalahan saat update stok"
		result.Error = &errMsg
		return result
	}

	result.Success = true
	result.NewStock = newStockQty
	return result
}

func (h *Handler) applyAddonStockUpdate(ctx context.Context, authCtx *middleware.AuthContext, update bulkStockUpdateItem) bulkStockUpdateResult {
	result := bulkStockUpdateResult{
		Success: false,
		Type:    "addon",
		ID:      update.ID,
		Name:    "Unknown",
	}

	var name string
	var stockQty pgtype.Int4
	var template pgtype.Int4
	var lastReset pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select ai.name, ai.stock_qty, ai.daily_stock_template, ai.last_stock_reset_at
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = $1 and ac.merchant_id = $2 and ai.deleted_at is null
	`, update.ID, *authCtx.MerchantID).Scan(&name, &stockQty, &template, &lastReset); err != nil {
		errMsg := "Addon tidak ditemukan"
		result.Error = &errMsg
		return result
	}

	result.Name = name
	if stockQty.Valid {
		result.PreviousStock = &stockQty.Int32
	}

	newStockQty := (*int32)(nil)
	if update.ResetToTemplate != nil && *update.ResetToTemplate {
		if template.Valid {
			newStockQty = &template.Int32
		} else {
			errMsg := "Template stok harian belum diatur"
			result.Error = &errMsg
			return result
		}
	} else if update.StockQty != nil {
		newStockQty = update.StockQty
	} else {
		errMsg := "stockQty atau resetToTemplate harus diisi"
		result.Error = &errMsg
		return result
	}

	_, err := h.DB.Exec(ctx, `
		update addon_items
		set stock_qty = $2,
			last_stock_reset_at = case when $3 then now() else last_stock_reset_at end,
			updated_by_user_id = $4
		where id = $1
	`, update.ID, newStockQty, update.ResetToTemplate != nil && *update.ResetToTemplate, authCtx.UserID)
	if err != nil {
		errMsg := "Terjadi kesalahan saat update stok"
		result.Error = &errMsg
		return result
	}

	result.Success = true
	result.NewStock = newStockQty
	return result
}

func toInt8Array(ids []int64) []int64 {
	return ids
}

func parseIDQueryList(idsParam string) []int64 {
	ids := strings.Split(idsParam, ",")
	parsed := make([]int64, 0, len(ids))
	for _, raw := range ids {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		id, err := parseStringToInt64(trimmed)
		if err == nil {
			parsed = append(parsed, id)
		}
	}
	return parsed
}

func buildBulkMenuToken(ids []int64) string {
	sorted := make([]int64, len(ids))
	copy(sorted, ids)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return fmt.Sprintf("DELETE_%d_ITEMS_%d_%d", len(sorted), sorted[0], sorted[len(sorted)-1])
}

func countLowStock(items []stockOverviewItem) int {
	count := 0
	for _, item := range items {
		if item.StockQty != nil && *item.StockQty > 0 && *item.StockQty <= 5 {
			count++
		}
	}
	return count
}

func countOutOfStock(items []stockOverviewItem) int {
	count := 0
	for _, item := range items {
		if item.StockQty == nil || *item.StockQty == 0 {
			count++
		}
	}
	return count
}

func countHealthyStock(items []stockOverviewItem) int {
	count := 0
	for _, item := range items {
		if item.StockQty != nil && *item.StockQty > 5 {
			count++
		}
	}
	return count
}

func countWithTemplate(items []stockOverviewItem) int {
	count := 0
	for _, item := range items {
		if item.DailyStockTemplate != nil && item.AutoResetStock {
			count++
		}
	}
	return count
}
