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
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type addonCategoryCount struct {
	AddonItems          int64 `json:"addonItems"`
	MenuAddonCategories int64 `json:"menuAddonCategories"`
}

type addonCategoryRef struct {
	ID               int64      `json:"id"`
	MerchantID       *int64     `json:"merchantId,omitempty"`
	Name             string     `json:"name"`
	Description      *string    `json:"description,omitempty"`
	MinSelection     *int32     `json:"minSelection,omitempty"`
	MaxSelection     *int32     `json:"maxSelection,omitempty"`
	IsActive         *bool      `json:"isActive,omitempty"`
	CreatedAt        *time.Time `json:"createdAt,omitempty"`
	UpdatedAt        *time.Time `json:"updatedAt,omitempty"`
	CreatedByUserID  *int64     `json:"createdByUserId,omitempty"`
	UpdatedByUserID  *int64     `json:"updatedByUserId,omitempty"`
	DeletedAt        *time.Time `json:"deletedAt,omitempty"`
	DeletedByUserID  *int64     `json:"deletedByUserId,omitempty"`
	RestoredAt       *time.Time `json:"restoredAt,omitempty"`
	RestoredByUserID *int64     `json:"restoredByUserId,omitempty"`
}

type merchantAddonItem struct {
	ID                 int64             `json:"id"`
	AddonCategoryID    int64             `json:"addonCategoryId"`
	Name               string            `json:"name"`
	Description        *string           `json:"description"`
	Price              float64           `json:"price"`
	IsActive           bool              `json:"isActive"`
	TrackStock         bool              `json:"trackStock"`
	StockQty           *int32            `json:"stockQty"`
	LowStockThreshold  *int32            `json:"lowStockThreshold"`
	InputType          string            `json:"inputType"`
	DisplayOrder       int32             `json:"displayOrder"`
	AutoResetStock     bool              `json:"autoResetStock"`
	DailyStockTemplate *int32            `json:"dailyStockTemplate"`
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
	CreatedByUserID    *int64            `json:"createdByUserId"`
	UpdatedByUserID    *int64            `json:"updatedByUserId"`
	DeletedAt          *time.Time        `json:"deletedAt"`
	DeletedByUserID    *int64            `json:"deletedByUserId"`
	RestoredAt         *time.Time        `json:"restoredAt"`
	RestoredByUserID   *int64            `json:"restoredByUserId"`
	LastStockResetAt   *time.Time        `json:"lastStockResetAt"`
	AddonCategory      *addonCategoryRef `json:"addonCategory,omitempty"`
}

type merchantAddonCategory struct {
	ID               int64               `json:"id"`
	MerchantID       int64               `json:"merchantId"`
	Name             string              `json:"name"`
	Description      *string             `json:"description"`
	MinSelection     int32               `json:"minSelection"`
	MaxSelection     *int32              `json:"maxSelection"`
	IsActive         bool                `json:"isActive"`
	CreatedAt        time.Time           `json:"createdAt"`
	UpdatedAt        time.Time           `json:"updatedAt"`
	CreatedByUserID  *int64              `json:"createdByUserId"`
	UpdatedByUserID  *int64              `json:"updatedByUserId"`
	DeletedAt        *time.Time          `json:"deletedAt"`
	DeletedByUserID  *int64              `json:"deletedByUserId"`
	RestoredAt       *time.Time          `json:"restoredAt"`
	RestoredByUserID *int64              `json:"restoredByUserId"`
	AddonItems       []merchantAddonItem `json:"addonItems"`
	Count            addonCategoryCount  `json:"_count"`
}

type addonCategoryCreatePayload struct {
	Name         string  `json:"name"`
	Description  *string `json:"description"`
	MinSelection *int32  `json:"minSelection"`
	MaxSelection *int32  `json:"maxSelection"`
}

type addonCategoryUpdatePayload struct {
	Name         *string `json:"name"`
	Description  *string `json:"description"`
	MinSelection *int32  `json:"minSelection"`
	MaxSelection *int32  `json:"maxSelection"`
	IsActive     *bool   `json:"isActive"`
}

type addonCategoryReorderPayload struct {
	ItemOrders []struct {
		ID           string `json:"id"`
		DisplayOrder int    `json:"displayOrder"`
	} `json:"itemOrders"`
}

type addonCategoryBulkDeletePayload struct {
	IDs []any `json:"ids"`
}

type addonCategoryBulkSoftDeletePayload struct {
	IDs               []any  `json:"ids"`
	ConfirmationToken string `json:"confirmationToken"`
}

type addonItemCreatePayload struct {
	AddonCategoryID    any      `json:"addonCategoryId"`
	Name               string   `json:"name"`
	Description        *string  `json:"description"`
	Price              *float64 `json:"price"`
	InputType          *string  `json:"inputType"`
	TrackStock         *bool    `json:"trackStock"`
	StockQty           *int32   `json:"stockQty"`
	LowStockThreshold  *int32   `json:"lowStockThreshold"`
	DailyStockTemplate *int32   `json:"dailyStockTemplate"`
	AutoResetStock     *bool    `json:"autoResetStock"`
}

type addonItemUpdatePayload struct {
	Name               *string  `json:"name"`
	Description        *string  `json:"description"`
	Price              *float64 `json:"price"`
	InputType          *string  `json:"inputType"`
	DisplayOrder       *int32   `json:"displayOrder"`
	IsActive           *bool    `json:"isActive"`
	TrackStock         *bool    `json:"trackStock"`
	StockQty           *int32   `json:"stockQty"`
	LowStockThreshold  *int32   `json:"lowStockThreshold"`
	DailyStockTemplate *int32   `json:"dailyStockTemplate"`
	AutoResetStock     *bool    `json:"autoResetStock"`
}

type addonItemBulkSoftDeletePayload struct {
	IDs               []any  `json:"ids"`
	ConfirmationToken string `json:"confirmationToken"`
}

type bulkAddonItemInput struct {
	ID                 *string `json:"id"`
	AddonCategoryID    string  `json:"addonCategoryId"`
	Name               string  `json:"name"`
	Description        *string `json:"description"`
	Price              float64 `json:"price"`
	InputType          *string `json:"inputType"`
	IsActive           *bool   `json:"isActive"`
	TrackStock         *bool   `json:"trackStock"`
	StockQty           *int32  `json:"stockQty"`
	DailyStockTemplate *int32  `json:"dailyStockTemplate"`
	AutoResetStock     *bool   `json:"autoResetStock"`
	DisplayOrder       *int32  `json:"displayOrder"`
}

type bulkAddonUploadPayload struct {
	Items        []bulkAddonItemInput `json:"items"`
	UpsertByName bool                 `json:"upsertByName"`
}

func (h *Handler) MerchantAddonCategoriesList(w http.ResponseWriter, r *http.Request) {
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

	query := `
		select
			ac.id, ac.merchant_id, ac.name, ac.description, ac.min_selection, ac.max_selection, ac.is_active,
			ac.created_at, ac.updated_at, ac.created_by_user_id, ac.updated_by_user_id,
			ac.deleted_at, ac.deleted_by_user_id, ac.restored_at, ac.restored_by_user_id,
			coalesce((select count(*) from addon_items ai where ai.addon_category_id = ac.id and ai.deleted_at is null), 0) as addon_items_count,
			coalesce((select count(*) from menu_addon_categories mac where mac.addon_category_id = ac.id), 0) as menu_addon_categories_count
		from addon_categories ac
		where ac.merchant_id = $1 and ac.deleted_at is null
		order by ac.name asc
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve addon categories")
		return
	}
	defer rows.Close()

	items := make([]merchantAddonCategory, 0)
	for rows.Next() {
		var (
			category         merchantAddonCategory
			description      pgtype.Text
			maxSelection     pgtype.Int4
			createdByUserID  pgtype.Int8
			updatedByUserID  pgtype.Int8
			deletedAt        pgtype.Timestamptz
			deletedByUserID  pgtype.Int8
			restoredAt       pgtype.Timestamptz
			restoredByUserID pgtype.Int8
			addonItemsCount  int64
			menuAddonCount   int64
		)

		if err := rows.Scan(
			&category.ID,
			&category.MerchantID,
			&category.Name,
			&description,
			&category.MinSelection,
			&maxSelection,
			&category.IsActive,
			&category.CreatedAt,
			&category.UpdatedAt,
			&createdByUserID,
			&updatedByUserID,
			&deletedAt,
			&deletedByUserID,
			&restoredAt,
			&restoredByUserID,
			&addonItemsCount,
			&menuAddonCount,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve addon categories")
			return
		}

		category.Description = textPtr(description)
		category.MaxSelection = int4Ptr(maxSelection)
		category.CreatedByUserID = int8Ptr(createdByUserID)
		category.UpdatedByUserID = int8Ptr(updatedByUserID)
		category.DeletedAt = timePtr(deletedAt)
		category.DeletedByUserID = int8Ptr(deletedByUserID)
		category.RestoredAt = timePtr(restoredAt)
		category.RestoredByUserID = int8Ptr(restoredByUserID)
		category.Count = addonCategoryCount{AddonItems: addonItemsCount, MenuAddonCategories: menuAddonCount}

		addonItems, err := h.fetchAddonItems(ctx, category.ID, true, false)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve addon categories")
			return
		}
		category.AddonItems = addonItems

		items = append(items, category)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       items,
		"message":    "Addon categories retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload addonCategoryCreatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name is required")
		return
	}
	if len(name) > 100 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name must be less than 100 characters")
		return
	}
	if payload.MinSelection != nil && *payload.MinSelection < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Minimum selection cannot be negative")
		return
	}
	if payload.MaxSelection != nil {
		if *payload.MaxSelection < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Maximum selection cannot be negative")
			return
		}
		minSel := int32(0)
		if payload.MinSelection != nil {
			minSel = *payload.MinSelection
		}
		if *payload.MaxSelection < minSel {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Maximum selection must be greater than or equal to minimum selection")
			return
		}
	}

	minSelection := int32(0)
	if payload.MinSelection != nil {
		minSelection = *payload.MinSelection
	}

	query := `
		insert into addon_categories (
			merchant_id, name, description, min_selection, max_selection, is_active,
			created_at, updated_at, created_by_user_id, updated_by_user_id
		) values ($1, $2, $3, $4, $5, true, now(), now(), $6, $6)
		returning id, merchant_id, name, description, min_selection, max_selection, is_active,
			created_at, updated_at, created_by_user_id
	`

	var (
		category        merchantAddonCategory
		description     pgtype.Text
		maxSelection    pgtype.Int4
		createdByUserID pgtype.Int8
	)

	if err := h.DB.QueryRow(ctx, query, *authCtx.MerchantID, name, payload.Description, minSelection, payload.MaxSelection, authCtx.UserID).
		Scan(&category.ID, &category.MerchantID, &category.Name, &description, &category.MinSelection, &maxSelection, &category.IsActive, &category.CreatedAt, &category.UpdatedAt, &createdByUserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon category")
		return
	}

	category.Description = textPtr(description)
	category.MaxSelection = int4Ptr(maxSelection)
	category.CreatedByUserID = int8Ptr(createdByUserID)
	category.AddonItems = []merchantAddonItem{}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       category,
		"message":    "Addon category created successfully",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantAddonCategoriesDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	category, err := h.fetchAddonCategoryDetail(ctx, *authCtx.MerchantID, categoryID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       category,
		"message":    "Addon category retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var payload addonCategoryUpdatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Name != nil && strings.TrimSpace(*payload.Name) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name is required")
		return
	}
	if payload.Name != nil && len(strings.TrimSpace(*payload.Name)) > 100 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name must be less than 100 characters")
		return
	}
	if payload.MinSelection != nil && *payload.MinSelection < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Minimum selection cannot be negative")
		return
	}
	if payload.MaxSelection != nil {
		if *payload.MaxSelection < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Maximum selection cannot be negative")
			return
		}
		if payload.MinSelection != nil && *payload.MaxSelection < *payload.MinSelection {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Maximum selection must be greater than or equal to minimum selection")
			return
		}
	}

	setClauses := []string{"updated_at = now()"}
	args := []any{}

	if payload.Name != nil {
		setClauses = append(setClauses, "name = $"+intToString(len(args)+1))
		args = append(args, strings.TrimSpace(*payload.Name))
	}
	if payload.Description != nil {
		setClauses = append(setClauses, "description = $"+intToString(len(args)+1))
		args = append(args, payload.Description)
	}
	if payload.MinSelection != nil {
		setClauses = append(setClauses, "min_selection = $"+intToString(len(args)+1))
		args = append(args, *payload.MinSelection)
	}
	if payload.MaxSelection != nil {
		setClauses = append(setClauses, "max_selection = $"+intToString(len(args)+1))
		args = append(args, payload.MaxSelection)
	}
	if payload.IsActive != nil {
		setClauses = append(setClauses, "is_active = $"+intToString(len(args)+1))
		args = append(args, *payload.IsActive)
	}

	setClauses = append(setClauses, "updated_by_user_id = $"+intToString(len(args)+1))
	args = append(args, authCtx.UserID)

	args = append(args, categoryID, *authCtx.MerchantID)

	query := `update addon_categories set ` + strings.Join(setClauses, ", ") + ` where id = $` + intToString(len(args)-1) + ` and merchant_id = $` + intToString(len(args))

	if _, err := h.DB.Exec(ctx, query, args...); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	category, err := h.fetchAddonCategoryDetail(ctx, *authCtx.MerchantID, categoryID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       category,
		"message":    "Addon category updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2 and deleted_at is null)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon category")
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "delete from menu_addon_categories where addon_category_id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon category")
		return
	}

	if _, err := tx.Exec(ctx, `
		update addon_categories
		set deleted_at = now(), deleted_by_user_id = $2
		where id = $1
	`, categoryID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon category")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Addon category deleted successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesToggleActive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var isActive bool
	if err := h.DB.QueryRow(ctx, "select is_active from addon_categories where id = $1 and merchant_id = $2", categoryID, *authCtx.MerchantID).Scan(&isActive); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "update addon_categories set is_active = $2, updated_by_user_id = $3, updated_at = now() where id = $1", categoryID, !isActive, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle addon category status")
		return
	}

	category, err := h.fetchAddonCategoryDetail(ctx, *authCtx.MerchantID, categoryID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	message := "Addon category deactivated successfully"
	if category.IsActive {
		message = "Addon category activated successfully"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       category,
		"message":    message,
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var merchantID int64
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, "select merchant_id, deleted_at from addon_categories where id = $1", categoryID).Scan(&merchantID, &deletedAt); err != nil || merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	if !deletedAt.Valid {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Addon category is not deleted")
		return
	}

	if _, err := h.DB.Exec(ctx, `
		update addon_categories
		set deleted_at = null, deleted_by_user_id = null, restored_at = now(), restored_by_user_id = $2,
			is_active = true, updated_at = now(), updated_by_user_id = $2
		where id = $1
	`, categoryID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore addon category")
		return
	}

	category, err := h.fetchAddonCategoryDetail(ctx, *authCtx.MerchantID, categoryID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore addon category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Addon category restored successfully",
		"data":    category,
	})
}

func (h *Handler) MerchantAddonCategoriesPermanentDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var name string
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select name, deleted_at
		from addon_categories
		where id = $1 and merchant_id = $2
	`, categoryID, *authCtx.MerchantID).Scan(&name, &deletedAt); err != nil || !deletedAt.Valid {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Archived addon category not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from menu_addon_categories where addon_category_id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete addon category permanently")
		return
	}
	if _, err := h.DB.Exec(ctx, "delete from addon_items where addon_category_id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete addon category permanently")
		return
	}
	if _, err := h.DB.Exec(ctx, "delete from addon_categories where id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete addon category permanently")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Addon category permanently deleted",
		"data": map[string]any{
			"id":   categoryID,
			"name": name,
		},
	})
}

func (h *Handler) MerchantAddonCategoriesDeletePreview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var categoryName string
	if err := h.DB.QueryRow(ctx, `
		select name
		from addon_categories
		where id = $1 and merchant_id = $2 and deleted_at is null
	`, categoryID, *authCtx.MerchantID).Scan(&categoryName); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	menuRows, err := h.DB.Query(ctx, `
		select distinct m.id, m.name, m.image_url, m.is_active
		from menu_addon_categories mac
		join menus m on m.id = mac.menu_id
		where mac.addon_category_id = $1
		order by m.id asc
	`, categoryID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get delete preview")
		return
	}
	defer menuRows.Close()

	type menuPreview struct {
		ID       int64   `json:"id"`
		Name     string  `json:"name"`
		ImageURL *string `json:"imageUrl"`
		IsActive bool    `json:"isActive"`
	}

	menus := make([]menuPreview, 0)
	for menuRows.Next() {
		var (
			menuID   int64
			menuName string
			imageURL pgtype.Text
			isActive bool
		)
		if err := menuRows.Scan(&menuID, &menuName, &imageURL, &isActive); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get delete preview")
			return
		}
		menus = append(menus, menuPreview{ID: menuID, Name: menuName, ImageURL: textPtr(imageURL), IsActive: isActive})
	}

	itemRows, err := h.DB.Query(ctx, `
		select id, name, price, is_active
		from addon_items
		where addon_category_id = $1 and deleted_at is null
		order by id asc
	`, categoryID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get delete preview")
		return
	}
	defer itemRows.Close()

	type itemPreview struct {
		ID       int64   `json:"id"`
		Name     string  `json:"name"`
		Price    float64 `json:"price"`
		IsActive bool    `json:"isActive"`
	}

	addonItems := make([]itemPreview, 0)
	for itemRows.Next() {
		var item itemPreview
		if err := itemRows.Scan(&item.ID, &item.Name, &item.Price, &item.IsActive); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get delete preview")
			return
		}
		addonItems = append(addonItems, item)
	}

	previewMenus := menus
	if len(menus) > 10 {
		previewMenus = menus[:10]
	}
	previewItems := addonItems
	if len(addonItems) > 5 {
		previewItems = addonItems[:5]
	}

	warnings := make([]string, 0)
	if len(menus) > 0 {
		warnings = append(warnings, fmt.Sprintf("This addon category is assigned to %d menu item(s).", len(menus)))
	}
	if len(addonItems) > 0 {
		warnings = append(warnings, fmt.Sprintf("This will also delete %d addon item(s) in this category.", len(addonItems)))
	}
	message := "This addon category is not assigned to any menu items and has no addon items. It can be safely deleted."
	if len(warnings) > 0 {
		message = strings.Join(warnings, " ")
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"addonCategory": map[string]any{
				"id":   categoryID,
				"name": categoryName,
			},
			"affectedMenusCount": len(menus),
			"affectedMenus":      previewMenus,
			"hasMoreMenus":       len(menus) > 10,
			"addonItemsCount":    len(addonItems),
			"addonItems":         previewItems,
			"hasMoreItems":       len(addonItems) > 5,
			"message":            message,
			"canDelete":          true,
		},
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesItemsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Addon category not found")
		return
	}

	items, err := h.fetchAddonItems(ctx, categoryID, false, false)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "An error occurred while retrieving addon items")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       items,
		"message":    "Addon items retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesReorderItems(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var payload addonCategoryReorderPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.ItemOrders == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "itemOrders array is required")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder addon items")
		return
	}
	defer tx.Rollback(ctx)

	for _, item := range payload.ItemOrders {
		if item.DisplayOrder < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Display order cannot be negative")
			return
		}
		itemID, err := parseStringToInt64(item.ID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid item id")
			return
		}
		if _, err := tx.Exec(ctx, "update addon_items set display_order = $1 where id = $2 and addon_category_id = $3", item.DisplayOrder, itemID, categoryID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder addon items")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder addon items")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Addon items reordered successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesRelationships(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon category id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2 and deleted_at is null)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Addon category not found")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select m.id, m.name, m.description, m.price, mac.is_required, mac.display_order
		from menu_addon_categories mac
		join menus m on m.id = mac.menu_id
		where mac.addon_category_id = $1 and m.merchant_id = $2 and m.deleted_at is null
		order by mac.display_order asc
	`, categoryID, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu relationships")
		return
	}
	defer rows.Close()

	type menuRelation struct {
		ID           int64   `json:"id"`
		Name         string  `json:"name"`
		Description  *string `json:"description"`
		Price        float64 `json:"price"`
		IsRequired   bool    `json:"isRequired"`
		DisplayOrder int32   `json:"displayOrder"`
	}

	menus := make([]menuRelation, 0)
	for rows.Next() {
		var (
			row         menuRelation
			description pgtype.Text
		)
		if err := rows.Scan(&row.ID, &row.Name, &description, &row.Price, &row.IsRequired, &row.DisplayOrder); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch menu relationships")
			return
		}
		row.Description = textPtr(description)
		menus = append(menus, row)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       menus,
		"message":    "Menu relationships retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonCategoriesBulkDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload addonCategoryBulkDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if len(payload.IDs) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Addon category IDs are required")
		return
	}

	categoryIDs, err := parseAnyInt64Slice(payload.IDs)
	if err != nil || len(categoryIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Addon category IDs are required")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select id
		from addon_categories
		where id = any($1) and merchant_id = $2 and deleted_at is null
	`, categoryIDs, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
		return
	}
	defer rows.Close()

	found := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
			return
		}
		found[id] = struct{}{}
	}

	if len(found) != len(categoryIDs) {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Some addon categories not found or already deleted")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		update addon_categories
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1) and merchant_id = $3 and deleted_at is null
	`, categoryIDs, authCtx.UserID, *authCtx.MerchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
		return
	}

	if _, err := tx.Exec(ctx, `
		update addon_items
		set deleted_at = now(), deleted_by_user_id = $2
		where addon_category_id = any($1) and deleted_at is null
	`, categoryIDs, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon categories")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d addon category(ies) and their items", len(categoryIDs)),
		"data": map[string]any{
			"count": len(categoryIDs),
		},
	})
}

func (h *Handler) MerchantAddonCategoriesBulkSoftDeleteToken(w http.ResponseWriter, r *http.Request) {
	idsParam := strings.TrimSpace(r.URL.Query().Get("ids"))
	if idsParam == "" {
		response.Error(w, http.StatusBadRequest, "MISSING_IDS", "Please provide ids as query parameter")
		return
	}

	ids, err := parseCommaSeparatedInt64(idsParam)
	if err != nil || len(ids) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "No valid IDs provided")
		return
	}

	token := generateAddonCategoryConfirmationToken(ids)
	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"itemCount":         len(ids),
			"confirmationToken": token,
			"message":           fmt.Sprintf("This will delete %d addon categories. Use this token to confirm.", len(ids)),
		},
	})
}

func (h *Handler) MerchantAddonCategoriesBulkSoftDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload addonCategoryBulkSoftDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	categoryIDs, err := parseAnyInt64Slice(payload.IDs)
	if err != nil || len(categoryIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of addon category IDs")
		return
	}

	if len(categoryIDs) > 50 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Cannot delete more than 50 addon categories at once")
		return
	}

	expected := generateAddonCategoryConfirmationToken(categoryIDs)
	if payload.ConfirmationToken != expected {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "CONFIRMATION_REQUIRED",
			"message":    "Please confirm this bulk delete operation",
			"statusCode": http.StatusBadRequest,
			"data": map[string]any{
				"itemCount":         len(categoryIDs),
				"confirmationToken": expected,
			},
		})
		return
	}

	cmd := `
		update addon_categories
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1) and deleted_at is null
	`

	res, err := h.DB.Exec(ctx, cmd, categoryIDs, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "BULK_DELETE_FAILED", "Failed to bulk delete addon categories")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d addon categories", res.RowsAffected()),
		"data": map[string]any{
			"success":      true,
			"deletedCount": res.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantAddonItemsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryIDParam := strings.TrimSpace(r.URL.Query().Get("categoryId"))
	if categoryIDParam != "" {
		categoryID, err := parseStringToInt64(categoryIDParam)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid categoryId")
			return
		}

		var exists bool
		if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
			response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Addon category not found")
			return
		}

		items, err := h.fetchAddonItems(ctx, categoryID, false, false)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get addon items")
			return
		}

		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       items,
			"message":    "Addon items retrieved successfully",
			"statusCode": http.StatusOK,
		})
		return
	}

	items, err := h.fetchAddonItemsByMerchant(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get addon items")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       items,
		"message":    "Addon items retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonItemsCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload addonItemCreatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.AddonCategoryID == nil || strings.TrimSpace(payload.Name) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Addon category ID and name are required")
		return
	}

	addonCategoryID, err := parseAnyInt64(payload.AddonCategoryID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addonCategoryId")
		return
	}

	var categoryExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from addon_categories where id = $1 and merchant_id = $2)", addonCategoryID, *authCtx.MerchantID).Scan(&categoryExists); err != nil || !categoryExists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon category not found")
		return
	}

	price := 0.0
	if payload.Price != nil {
		price = *payload.Price
	}
	if price < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Price cannot be negative")
		return
	}

	trackStock := payload.TrackStock != nil && *payload.TrackStock
	if trackStock {
		if payload.StockQty != nil && *payload.StockQty < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Stock quantity cannot be negative")
			return
		}
		if payload.LowStockThreshold != nil && *payload.LowStockThreshold < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Low stock threshold cannot be negative")
			return
		}
	}

	var maxOrder pgtype.Int4
	if err := h.DB.QueryRow(ctx, "select max(display_order) from addon_items where addon_category_id = $1", addonCategoryID).Scan(&maxOrder); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon item")
		return
	}
	order := int32(0)
	if maxOrder.Valid {
		order = maxOrder.Int32 + 1
	}

	inputType := "SELECT"
	if payload.InputType != nil && strings.TrimSpace(*payload.InputType) != "" {
		inputType = strings.TrimSpace(*payload.InputType)
	}

	autoResetStock := payload.AutoResetStock != nil && *payload.AutoResetStock
	stockQty := payload.StockQty
	lowStockThreshold := payload.LowStockThreshold
	dailyStockTemplate := payload.DailyStockTemplate
	if !trackStock {
		stockQty = nil
		lowStockThreshold = nil
		dailyStockTemplate = nil
		autoResetStock = false
	}

	query := `
		insert into addon_items (
			addon_category_id, name, description, price, input_type, display_order, is_active,
			track_stock, stock_qty, low_stock_threshold, daily_stock_template, auto_reset_stock,
			created_at, updated_at, created_by_user_id, updated_by_user_id
		) values ($1, $2, $3, $4, $5, $6, true, $7, $8, $9, $10, $11, now(), now(), $12, $12)
		returning id
	`

	var newID int64
	if err := h.DB.QueryRow(ctx, query, addonCategoryID, strings.TrimSpace(payload.Name), payload.Description, price, inputType, order, trackStock, stockQty, lowStockThreshold, dailyStockTemplate, autoResetStock, authCtx.UserID).Scan(&newID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon item")
		return
	}

	item, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, newID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon item")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       item,
		"message":    "Addon item created successfully",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantAddonItemsDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	item, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon item not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       item,
		"message":    "Addon item retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonItemsUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	var payload addonItemUpdatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Name != nil && strings.TrimSpace(*payload.Name) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Addon item name is required")
		return
	}
	if payload.Price != nil && *payload.Price < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Price cannot be negative")
		return
	}
	if payload.StockQty != nil && *payload.StockQty < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Stock quantity cannot be negative")
		return
	}
	if payload.LowStockThreshold != nil && *payload.LowStockThreshold < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Low stock threshold cannot be negative")
		return
	}

	existing, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon item not found")
		return
	}

	setClauses := []string{"updated_at = now()"}
	args := []any{}

	if payload.Name != nil {
		setClauses = append(setClauses, "name = $"+intToString(len(args)+1))
		args = append(args, strings.TrimSpace(*payload.Name))
	}
	if payload.Description != nil {
		setClauses = append(setClauses, "description = $"+intToString(len(args)+1))
		args = append(args, payload.Description)
	}
	if payload.Price != nil {
		setClauses = append(setClauses, "price = $"+intToString(len(args)+1))
		args = append(args, *payload.Price)
	}
	if payload.InputType != nil {
		setClauses = append(setClauses, "input_type = $"+intToString(len(args)+1))
		args = append(args, strings.TrimSpace(*payload.InputType))
	}
	if payload.IsActive != nil {
		setClauses = append(setClauses, "is_active = $"+intToString(len(args)+1))
		args = append(args, *payload.IsActive)
	}

	if payload.TrackStock != nil {
		setClauses = append(setClauses, "track_stock = $"+intToString(len(args)+1))
		args = append(args, *payload.TrackStock)

		if *payload.TrackStock {
			stockQty := int32(0)
			if payload.StockQty != nil {
				stockQty = *payload.StockQty
			}
			setClauses = append(setClauses, "stock_qty = $"+intToString(len(args)+1))
			args = append(args, stockQty)

			setClauses = append(setClauses, "low_stock_threshold = $"+intToString(len(args)+1))
			args = append(args, payload.LowStockThreshold)

			setClauses = append(setClauses, "daily_stock_template = $"+intToString(len(args)+1))
			args = append(args, payload.DailyStockTemplate)

			autoResetStock := payload.AutoResetStock != nil && *payload.AutoResetStock
			if payload.DailyStockTemplate == nil {
				autoResetStock = false
			}
			setClauses = append(setClauses, "auto_reset_stock = $"+intToString(len(args)+1))
			args = append(args, autoResetStock)
		} else {
			setClauses = append(setClauses, "stock_qty = $"+intToString(len(args)+1))
			args = append(args, nil)
			setClauses = append(setClauses, "low_stock_threshold = $"+intToString(len(args)+1))
			args = append(args, nil)
			setClauses = append(setClauses, "daily_stock_template = $"+intToString(len(args)+1))
			args = append(args, nil)
			setClauses = append(setClauses, "auto_reset_stock = $"+intToString(len(args)+1))
			args = append(args, false)
		}
	} else if payload.StockQty != nil && existing.TrackStock {
		setClauses = append(setClauses, "stock_qty = $"+intToString(len(args)+1))
		args = append(args, *payload.StockQty)
	}

	if existing.TrackStock {
		if payload.LowStockThreshold != nil {
			setClauses = append(setClauses, "low_stock_threshold = $"+intToString(len(args)+1))
			args = append(args, payload.LowStockThreshold)
		}
		if payload.DailyStockTemplate != nil {
			setClauses = append(setClauses, "daily_stock_template = $"+intToString(len(args)+1))
			args = append(args, payload.DailyStockTemplate)
		}
		if payload.AutoResetStock != nil {
			setClauses = append(setClauses, "auto_reset_stock = $"+intToString(len(args)+1))
			args = append(args, *payload.AutoResetStock)
		}
	}

	setClauses = append(setClauses, "updated_by_user_id = $"+intToString(len(args)+1))
	args = append(args, authCtx.UserID)
	args = append(args, itemID)

	query := `update addon_items set ` + strings.Join(setClauses, ", ") + ` where id = $` + intToString(len(args))
	if _, err := h.DB.Exec(ctx, query, args...); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update addon item")
		return
	}

	item, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update addon item")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       item,
		"message":    "Addon item updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonItemsDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	_, err = h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon item not found")
		return
	}

	var orderCount int64
	if err := h.DB.QueryRow(ctx, "select count(*) from order_item_addons where addon_item_id = $1", itemID).Scan(&orderCount); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon item")
		return
	}
	if orderCount > 0 {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Cannot delete addon item that has been used in orders. Consider deactivating instead.")
		return
	}

	if _, err := h.DB.Exec(ctx, "update addon_items set deleted_at = now(), deleted_by_user_id = $2 where id = $1", itemID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete addon item")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Addon item deleted successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonItemsToggleActive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	var isActive bool
	if err := h.DB.QueryRow(ctx, `
		select ai.is_active
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = $1 and ac.merchant_id = $2
	`, itemID, *authCtx.MerchantID).Scan(&isActive); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon item not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "update addon_items set is_active = $2, updated_by_user_id = $3, updated_at = now() where id = $1", itemID, !isActive, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle addon item status")
		return
	}

	item, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle addon item status")
		return
	}

	message := "Addon item deactivated successfully"
	if item.IsActive {
		message = "Addon item activated successfully"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       item,
		"message":    message,
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantAddonItemsRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	var merchantID int64
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select ac.merchant_id, ai.deleted_at
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = $1
	`, itemID).Scan(&merchantID, &deletedAt); err != nil || merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Addon item not found")
		return
	}

	if !deletedAt.Valid {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Addon item is not deleted")
		return
	}

	if _, err := h.DB.Exec(ctx, `
		update addon_items
		set deleted_at = null, deleted_by_user_id = null, restored_at = now(), restored_by_user_id = $2,
			is_active = true, updated_at = now(), updated_by_user_id = $2
		where id = $1
	`, itemID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore addon item")
		return
	}

	item, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, itemID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore addon item")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Addon item restored successfully",
		"data":    item,
	})
}

func (h *Handler) MerchantAddonItemsPermanentDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	itemID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid addon item id")
		return
	}

	var name string
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select ai.name, ai.deleted_at
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = $1 and ac.merchant_id = $2
	`, itemID, *authCtx.MerchantID).Scan(&name, &deletedAt); err != nil || !deletedAt.Valid {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Archived addon item not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from order_item_addons where addon_item_id = $1", itemID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete addon item permanently")
		return
	}
	if _, err := h.DB.Exec(ctx, "delete from addon_items where id = $1", itemID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete addon item permanently")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Addon item permanently deleted",
		"data": map[string]any{
			"id":   itemID,
			"name": name,
		},
	})
}

func (h *Handler) MerchantAddonItemsBulkSoftDeleteToken(w http.ResponseWriter, r *http.Request) {
	idsParam := strings.TrimSpace(r.URL.Query().Get("ids"))
	if idsParam == "" {
		response.Error(w, http.StatusBadRequest, "MISSING_IDS", "Please provide ids as query parameter")
		return
	}

	ids, err := parseCommaSeparatedInt64(idsParam)
	if err != nil || len(ids) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "No valid IDs provided")
		return
	}

	token := generateAddonItemConfirmationToken(ids)
	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"itemCount":         len(ids),
			"confirmationToken": token,
			"message":           fmt.Sprintf("This will delete %d addon items. Use this token to confirm.", len(ids)),
		},
	})
}

func (h *Handler) MerchantAddonItemsBulkSoftDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload addonItemBulkSoftDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	itemIDs, err := parseAnyInt64Slice(payload.IDs)
	if err != nil || len(itemIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of addon item IDs")
		return
	}

	if len(itemIDs) > 100 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Cannot delete more than 100 addon items at once")
		return
	}

	expected := generateAddonItemConfirmationToken(itemIDs)
	if payload.ConfirmationToken != expected {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "CONFIRMATION_REQUIRED",
			"message":    "Please confirm this bulk delete operation",
			"statusCode": http.StatusBadRequest,
			"data": map[string]any{
				"itemCount":         len(itemIDs),
				"confirmationToken": expected,
			},
		})
		return
	}

	res, err := h.DB.Exec(ctx, `
		update addon_items
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1) and deleted_at is null
	`, itemIDs, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "BULK_DELETE_FAILED", "Failed to bulk delete addon items")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": fmt.Sprintf("Successfully deleted %d addon items", res.RowsAffected()),
		"data": map[string]any{
			"success":      true,
			"deletedCount": res.RowsAffected(),
		},
	})
}

func (h *Handler) MerchantAddonItemsBulkUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	var payload bulkAddonUploadPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if len(payload.Items) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "No items provided")
		return
	}

	if len(payload.Items) > 200 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Maximum 200 items allowed per upload")
		return
	}

	categoryRows, err := h.DB.Query(ctx, `
		select id
		from addon_categories
		where merchant_id = $1 and deleted_at is null
	`, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
		return
	}
	defer categoryRows.Close()

	validCategoryIDs := make(map[string]struct{})
	for categoryRows.Next() {
		var id int64
		if err := categoryRows.Scan(&id); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
			return
		}
		validCategoryIDs[int64ToString(id)] = struct{}{}
	}

	addonRows, err := h.DB.Query(ctx, `
		select ai.id, ai.name, ai.addon_category_id
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ac.merchant_id = $1 and ai.deleted_at is null
	`, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
		return
	}
	defer addonRows.Close()

	nameMap := make(map[string]int64)
	for addonRows.Next() {
		var id int64
		var name string
		var categoryID int64
		if err := addonRows.Scan(&id, &name, &categoryID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
			return
		}
		key := fmt.Sprintf("%s::%d", strings.ToLower(strings.TrimSpace(name)), categoryID)
		nameMap[key] = id
	}

	createdCount := 0
	updatedCount := 0

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
		return
	}
	defer tx.Rollback(ctx)

	processed := make([]merchantAddonItem, 0, len(payload.Items))

	for _, item := range payload.Items {
		if item.AddonCategoryID == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Valid addon category is required for \"%s\"", item.Name))
			return
		}

		if _, ok := validCategoryIDs[item.AddonCategoryID]; !ok {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Valid addon category is required for \"%s\"", item.Name))
			return
		}

		if strings.TrimSpace(item.Name) == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Addon item name is required")
			return
		}

		if item.Price < 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Valid price is required for \"%s\"", item.Name))
			return
		}

		categoryID, err := parseStringToInt64(item.AddonCategoryID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", fmt.Sprintf("Valid addon category is required for \"%s\"", item.Name))
			return
		}

		var existingID *int64
		if item.ID != nil && strings.TrimSpace(*item.ID) != "" {
			parsedID, err := parseStringToInt64(*item.ID)
			if err == nil {
				existingID = &parsedID
			}
		} else if payload.UpsertByName {
			lookupKey := fmt.Sprintf("%s::%s", strings.ToLower(strings.TrimSpace(item.Name)), item.AddonCategoryID)
			if matched, ok := nameMap[lookupKey]; ok {
				existingID = &matched
			}
		}

		inputType := "SELECT"
		if item.InputType != nil && strings.TrimSpace(*item.InputType) != "" {
			inputType = strings.TrimSpace(*item.InputType)
		}

		isActive := true
		if item.IsActive != nil {
			isActive = *item.IsActive
		}

		trackStock := item.TrackStock != nil && *item.TrackStock
		stockQty := item.StockQty
		dailyStockTemplate := item.DailyStockTemplate
		autoResetStock := item.AutoResetStock != nil && *item.AutoResetStock
		if !trackStock {
			stockQty = nil
			dailyStockTemplate = nil
			autoResetStock = false
		}

		displayOrder := int32(0)
		if item.DisplayOrder != nil {
			displayOrder = *item.DisplayOrder
		}

		if existingID != nil {
			if _, err := tx.Exec(ctx, `
				update addon_items
				set addon_category_id = $1, name = $2, description = $3, price = $4, input_type = $5,
					is_active = $6, track_stock = $7, stock_qty = $8, daily_stock_template = $9, auto_reset_stock = $10,
					display_order = $11, updated_at = now(), updated_by_user_id = $12
				where id = $13
			`, categoryID, strings.TrimSpace(item.Name), item.Description, item.Price, inputType, isActive, trackStock, stockQty, dailyStockTemplate, autoResetStock, displayOrder, authCtx.UserID, *existingID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
				return
			}
			updatedCount++
			updatedItem, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, *existingID)
			if err == nil {
				processed = append(processed, updatedItem)
			}
		} else {
			var newID int64
			if err := tx.QueryRow(ctx, `
				insert into addon_items (
					addon_category_id, name, description, price, input_type, is_active, track_stock, stock_qty,
					daily_stock_template, auto_reset_stock, display_order, created_at, updated_at, created_by_user_id
				) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now(), $12)
				returning id
			`, categoryID, strings.TrimSpace(item.Name), item.Description, item.Price, inputType, isActive, trackStock, stockQty, dailyStockTemplate, autoResetStock, displayOrder, authCtx.UserID).Scan(&newID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
				return
			}
			createdCount++
			createdItem, err := h.fetchAddonItemByID(ctx, *authCtx.MerchantID, newID)
			if err == nil {
				processed = append(processed, createdItem)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create addon items")
		return
	}

	message := ""
	if createdCount > 0 && updatedCount > 0 {
		message = fmt.Sprintf("Successfully created %d and updated %d addon items", createdCount, updatedCount)
	} else if updatedCount > 0 {
		message = fmt.Sprintf("Successfully updated %d addon items", updatedCount)
	} else {
		message = fmt.Sprintf("Successfully created %d addon items", createdCount)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"message":      message,
		"createdCount": createdCount,
		"updatedCount": updatedCount,
		"data":         processed,
	})
}

func (h *Handler) fetchAddonCategoryDetail(ctx context.Context, merchantID int64, categoryID int64) (*merchantAddonCategory, error) {
	query := `
		select
			ac.id, ac.merchant_id, ac.name, ac.description, ac.min_selection, ac.max_selection, ac.is_active,
			ac.created_at, ac.updated_at, ac.created_by_user_id, ac.updated_by_user_id,
			ac.deleted_at, ac.deleted_by_user_id, ac.restored_at, ac.restored_by_user_id,
			coalesce((select count(*) from addon_items ai where ai.addon_category_id = ac.id), 0) as addon_items_count,
			coalesce((select count(*) from menu_addon_categories mac where mac.addon_category_id = ac.id), 0) as menu_addon_categories_count
		from addon_categories ac
		where ac.id = $1 and ac.merchant_id = $2
		limit 1
	`

	var (
		category         merchantAddonCategory
		description      pgtype.Text
		maxSelection     pgtype.Int4
		createdByUserID  pgtype.Int8
		updatedByUserID  pgtype.Int8
		deletedAt        pgtype.Timestamptz
		deletedByUserID  pgtype.Int8
		restoredAt       pgtype.Timestamptz
		restoredByUserID pgtype.Int8
		addonItemsCount  int64
		menuAddonCount   int64
	)

	if err := h.DB.QueryRow(ctx, query, categoryID, merchantID).Scan(
		&category.ID,
		&category.MerchantID,
		&category.Name,
		&description,
		&category.MinSelection,
		&maxSelection,
		&category.IsActive,
		&category.CreatedAt,
		&category.UpdatedAt,
		&createdByUserID,
		&updatedByUserID,
		&deletedAt,
		&deletedByUserID,
		&restoredAt,
		&restoredByUserID,
		&addonItemsCount,
		&menuAddonCount,
	); err != nil {
		return nil, err
	}

	category.Description = textPtr(description)
	category.MaxSelection = int4Ptr(maxSelection)
	category.CreatedByUserID = int8Ptr(createdByUserID)
	category.UpdatedByUserID = int8Ptr(updatedByUserID)
	category.DeletedAt = timePtr(deletedAt)
	category.DeletedByUserID = int8Ptr(deletedByUserID)
	category.RestoredAt = timePtr(restoredAt)
	category.RestoredByUserID = int8Ptr(restoredByUserID)
	category.Count = addonCategoryCount{AddonItems: addonItemsCount, MenuAddonCategories: menuAddonCount}

	items, err := h.fetchAddonItems(ctx, categoryID, false, true)
	if err != nil {
		return nil, err
	}
	category.AddonItems = items

	return &category, nil
}

func (h *Handler) fetchAddonItems(ctx context.Context, categoryID int64, activeOnly bool, includeDeleted bool) ([]merchantAddonItem, error) {
	query := `
		select
			id, addon_category_id, name, description, price, is_active, track_stock, stock_qty, low_stock_threshold,
			input_type, display_order, auto_reset_stock, daily_stock_template, created_at, updated_at,
			created_by_user_id, updated_by_user_id, deleted_at, deleted_by_user_id, restored_at, restored_by_user_id,
			last_stock_reset_at
		from addon_items
		where addon_category_id = $1
	`

	args := []any{categoryID}
	if activeOnly {
		query += " and is_active = true and deleted_at is null"
	} else if !includeDeleted {
		query += " and deleted_at is null"
	}
	query += " order by display_order asc"

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]merchantAddonItem, 0)
	for rows.Next() {
		var (
			item               merchantAddonItem
			description        pgtype.Text
			stockQty           pgtype.Int4
			lowStockThreshold  pgtype.Int4
			dailyStockTemplate pgtype.Int4
			createdByUserID    pgtype.Int8
			updatedByUserID    pgtype.Int8
			deletedAt          pgtype.Timestamptz
			deletedByUserID    pgtype.Int8
			restoredAt         pgtype.Timestamptz
			restoredByUserID   pgtype.Int8
			lastStockResetAt   pgtype.Timestamptz
		)

		if err := rows.Scan(
			&item.ID,
			&item.AddonCategoryID,
			&item.Name,
			&description,
			&item.Price,
			&item.IsActive,
			&item.TrackStock,
			&stockQty,
			&lowStockThreshold,
			&item.InputType,
			&item.DisplayOrder,
			&item.AutoResetStock,
			&dailyStockTemplate,
			&item.CreatedAt,
			&item.UpdatedAt,
			&createdByUserID,
			&updatedByUserID,
			&deletedAt,
			&deletedByUserID,
			&restoredAt,
			&restoredByUserID,
			&lastStockResetAt,
		); err != nil {
			return nil, err
		}

		item.Description = textPtr(description)
		item.StockQty = int4Ptr(stockQty)
		item.LowStockThreshold = int4Ptr(lowStockThreshold)
		item.DailyStockTemplate = int4Ptr(dailyStockTemplate)
		item.CreatedByUserID = int8Ptr(createdByUserID)
		item.UpdatedByUserID = int8Ptr(updatedByUserID)
		item.DeletedAt = timePtr(deletedAt)
		item.DeletedByUserID = int8Ptr(deletedByUserID)
		item.RestoredAt = timePtr(restoredAt)
		item.RestoredByUserID = int8Ptr(restoredByUserID)
		item.LastStockResetAt = timePtr(lastStockResetAt)

		items = append(items, item)
	}

	return items, nil
}

func (h *Handler) fetchAddonItemsByMerchant(ctx context.Context, merchantID int64) ([]merchantAddonItem, error) {
	rows, err := h.DB.Query(ctx, `
		select
			ai.id, ai.addon_category_id, ai.name, ai.description, ai.price, ai.is_active, ai.track_stock, ai.stock_qty,
			ai.low_stock_threshold, ai.input_type, ai.display_order, ai.auto_reset_stock, ai.daily_stock_template,
			ai.created_at, ai.updated_at, ai.created_by_user_id, ai.updated_by_user_id, ai.deleted_at, ai.deleted_by_user_id,
			ai.restored_at, ai.restored_by_user_id, ai.last_stock_reset_at,
			ac.id, ac.name, ac.merchant_id
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.deleted_at is null and ac.merchant_id = $1 and ac.deleted_at is null
		order by ac.name asc, ai.display_order asc
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]merchantAddonItem, 0)
	for rows.Next() {
		var (
			item               merchantAddonItem
			description        pgtype.Text
			stockQty           pgtype.Int4
			lowStockThreshold  pgtype.Int4
			dailyStockTemplate pgtype.Int4
			createdByUserID    pgtype.Int8
			updatedByUserID    pgtype.Int8
			deletedAt          pgtype.Timestamptz
			deletedByUserID    pgtype.Int8
			restoredAt         pgtype.Timestamptz
			restoredByUserID   pgtype.Int8
			lastStockResetAt   pgtype.Timestamptz
			categoryID         int64
			categoryName       string
			categoryMerchantID int64
		)

		if err := rows.Scan(
			&item.ID,
			&item.AddonCategoryID,
			&item.Name,
			&description,
			&item.Price,
			&item.IsActive,
			&item.TrackStock,
			&stockQty,
			&lowStockThreshold,
			&item.InputType,
			&item.DisplayOrder,
			&item.AutoResetStock,
			&dailyStockTemplate,
			&item.CreatedAt,
			&item.UpdatedAt,
			&createdByUserID,
			&updatedByUserID,
			&deletedAt,
			&deletedByUserID,
			&restoredAt,
			&restoredByUserID,
			&lastStockResetAt,
			&categoryID,
			&categoryName,
			&categoryMerchantID,
		); err != nil {
			return nil, err
		}

		item.Description = textPtr(description)
		item.StockQty = int4Ptr(stockQty)
		item.LowStockThreshold = int4Ptr(lowStockThreshold)
		item.DailyStockTemplate = int4Ptr(dailyStockTemplate)
		item.CreatedByUserID = int8Ptr(createdByUserID)
		item.UpdatedByUserID = int8Ptr(updatedByUserID)
		item.DeletedAt = timePtr(deletedAt)
		item.DeletedByUserID = int8Ptr(deletedByUserID)
		item.RestoredAt = timePtr(restoredAt)
		item.RestoredByUserID = int8Ptr(restoredByUserID)
		item.LastStockResetAt = timePtr(lastStockResetAt)

		item.AddonCategory = &addonCategoryRef{
			ID:         categoryID,
			Name:       categoryName,
			MerchantID: &categoryMerchantID,
		}

		items = append(items, item)
	}

	return items, nil
}

func (h *Handler) fetchAddonItemByID(ctx context.Context, merchantID int64, itemID int64) (merchantAddonItem, error) {
	var (
		item               merchantAddonItem
		description        pgtype.Text
		stockQty           pgtype.Int4
		lowStockThreshold  pgtype.Int4
		dailyStockTemplate pgtype.Int4
		createdByUserID    pgtype.Int8
		updatedByUserID    pgtype.Int8
		deletedAt          pgtype.Timestamptz
		deletedByUserID    pgtype.Int8
		restoredAt         pgtype.Timestamptz
		restoredByUserID   pgtype.Int8
		lastStockResetAt   pgtype.Timestamptz

		catID             int64
		catMerchantID     int64
		catName           string
		catDescription    pgtype.Text
		catMinSelection   int32
		catMaxSelection   pgtype.Int4
		catIsActive       bool
		catCreatedAt      time.Time
		catUpdatedAt      time.Time
		catCreatedByUser  pgtype.Int8
		catUpdatedByUser  pgtype.Int8
		catDeletedAt      pgtype.Timestamptz
		catDeletedByUser  pgtype.Int8
		catRestoredAt     pgtype.Timestamptz
		catRestoredByUser pgtype.Int8
	)

	if err := h.DB.QueryRow(ctx, `
		select
			ai.id, ai.addon_category_id, ai.name, ai.description, ai.price, ai.is_active, ai.track_stock, ai.stock_qty,
			ai.low_stock_threshold, ai.input_type, ai.display_order, ai.auto_reset_stock, ai.daily_stock_template,
			ai.created_at, ai.updated_at, ai.created_by_user_id, ai.updated_by_user_id, ai.deleted_at, ai.deleted_by_user_id,
			ai.restored_at, ai.restored_by_user_id, ai.last_stock_reset_at,
			ac.id, ac.merchant_id, ac.name, ac.description, ac.min_selection, ac.max_selection, ac.is_active,
			ac.created_at, ac.updated_at, ac.created_by_user_id, ac.updated_by_user_id, ac.deleted_at, ac.deleted_by_user_id,
			ac.restored_at, ac.restored_by_user_id
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = $1 and ac.merchant_id = $2
	`, itemID, merchantID).Scan(
		&item.ID,
		&item.AddonCategoryID,
		&item.Name,
		&description,
		&item.Price,
		&item.IsActive,
		&item.TrackStock,
		&stockQty,
		&lowStockThreshold,
		&item.InputType,
		&item.DisplayOrder,
		&item.AutoResetStock,
		&dailyStockTemplate,
		&item.CreatedAt,
		&item.UpdatedAt,
		&createdByUserID,
		&updatedByUserID,
		&deletedAt,
		&deletedByUserID,
		&restoredAt,
		&restoredByUserID,
		&lastStockResetAt,
		&catID,
		&catMerchantID,
		&catName,
		&catDescription,
		&catMinSelection,
		&catMaxSelection,
		&catIsActive,
		&catCreatedAt,
		&catUpdatedAt,
		&catCreatedByUser,
		&catUpdatedByUser,
		&catDeletedAt,
		&catDeletedByUser,
		&catRestoredAt,
		&catRestoredByUser,
	); err != nil {
		return item, err
	}

	item.Description = textPtr(description)
	item.StockQty = int4Ptr(stockQty)
	item.LowStockThreshold = int4Ptr(lowStockThreshold)
	item.DailyStockTemplate = int4Ptr(dailyStockTemplate)
	item.CreatedByUserID = int8Ptr(createdByUserID)
	item.UpdatedByUserID = int8Ptr(updatedByUserID)
	item.DeletedAt = timePtr(deletedAt)
	item.DeletedByUserID = int8Ptr(deletedByUserID)
	item.RestoredAt = timePtr(restoredAt)
	item.RestoredByUserID = int8Ptr(restoredByUserID)
	item.LastStockResetAt = timePtr(lastStockResetAt)

	isActive := catIsActive
	item.AddonCategory = &addonCategoryRef{
		ID:               catID,
		MerchantID:       &catMerchantID,
		Name:             catName,
		Description:      textPtr(catDescription),
		MinSelection:     &catMinSelection,
		MaxSelection:     int4Ptr(catMaxSelection),
		IsActive:         &isActive,
		CreatedAt:        &catCreatedAt,
		UpdatedAt:        &catUpdatedAt,
		CreatedByUserID:  int8Ptr(catCreatedByUser),
		UpdatedByUserID:  int8Ptr(catUpdatedByUser),
		DeletedAt:        timePtr(catDeletedAt),
		DeletedByUserID:  int8Ptr(catDeletedByUser),
		RestoredAt:       timePtr(catRestoredAt),
		RestoredByUserID: int8Ptr(catRestoredByUser),
	}

	return item, nil
}

func generateAddonCategoryConfirmationToken(ids []int64) string {
	if len(ids) == 0 {
		return ""
	}
	copyIDs := append([]int64{}, ids...)
	sort.Slice(copyIDs, func(i, j int) bool { return copyIDs[i] < copyIDs[j] })
	return fmt.Sprintf("DELETE_ADDON_CATS_%d_ITEMS_%d_%d", len(copyIDs), copyIDs[0], copyIDs[len(copyIDs)-1])
}

func generateAddonItemConfirmationToken(ids []int64) string {
	if len(ids) == 0 {
		return ""
	}
	copyIDs := append([]int64{}, ids...)
	sort.Slice(copyIDs, func(i, j int) bool { return copyIDs[i] < copyIDs[j] })
	return fmt.Sprintf("DELETE_ADDON_ITEMS_%d_ITEMS_%d_%d", len(copyIDs), copyIDs[0], copyIDs[len(copyIDs)-1])
}

func parseCommaSeparatedInt64(value string) ([]int64, error) {
	parts := strings.Split(value, ",")
	ids := make([]int64, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		parsed, err := parseStringToInt64(trimmed)
		if err != nil {
			continue
		}
		ids = append(ids, parsed)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no valid ids")
	}
	return ids, nil
}

func parseAnyInt64(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return parseStringToInt64(strings.TrimSpace(v))
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("unsupported type")
	}
}

func parseAnyInt64Slice(values []any) ([]int64, error) {
	ids := make([]int64, 0, len(values))
	for _, value := range values {
		parsed, err := parseAnyInt64(value)
		if err != nil {
			return nil, err
		}
		ids = append(ids, parsed)
	}
	return ids, nil
}

func int64ToString(value int64) string {
	return fmt.Sprintf("%d", value)
}
