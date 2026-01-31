package handlers

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type categoryCount struct {
	Menus     int64 `json:"menus"`
	MenuItems int64 `json:"menuItems"`
}

type merchantCategory struct {
	ID               int64         `json:"id"`
	MerchantID       int64         `json:"merchantId"`
	Name             string        `json:"name"`
	Description      *string       `json:"description"`
	SortOrder        int32         `json:"sortOrder"`
	IsActive         bool          `json:"isActive"`
	CreatedAt        time.Time     `json:"createdAt"`
	UpdatedAt        time.Time     `json:"updatedAt"`
	CreatedByUserID  *int64        `json:"createdByUserId"`
	UpdatedByUserID  *int64        `json:"updatedByUserId"`
	DeletedAt        *time.Time    `json:"deletedAt"`
	DeletedByUserID  *int64        `json:"deletedByUserId"`
	RestoredAt       *time.Time    `json:"restoredAt"`
	RestoredByUserID *int64        `json:"restoredByUserId"`
	Count            categoryCount `json:"_count"`
}

type createCategoryPayload struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
	SortOrder   *int32  `json:"sortOrder"`
}

type updateCategoryPayload struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	SortOrder   *int32  `json:"sortOrder"`
}

type reorderCategoryPayload struct {
	Categories []struct {
		ID        string `json:"id"`
		SortOrder int    `json:"sortOrder"`
	} `json:"categories"`
}

type bulkDeletePayload struct {
	IDs []string `json:"ids"`
}

type bulkSoftDeletePayload struct {
	IDs               []int64 `json:"ids"`
	ConfirmationToken string  `json:"confirmationToken"`
}

func (h *Handler) MerchantCategoriesList(w http.ResponseWriter, r *http.Request) {
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
			mc.id, mc.merchant_id, mc.name, mc.description, mc.sort_order, mc.is_active,
			mc.created_at, mc.updated_at, mc.created_by_user_id, mc.updated_by_user_id,
			mc.deleted_at, mc.deleted_by_user_id, mc.restored_at, mc.restored_by_user_id,
			coalesce((select count(*) from menus m where m.category_id = mc.id and m.deleted_at is null), 0) as menus_count,
			coalesce((select count(*) from menu_category_items mci where mci.category_id = mc.id), 0) as menu_items_count
		from menu_categories mc
		where mc.merchant_id = $1 and mc.deleted_at is null
		order by mc.sort_order asc
	`

	rows, err := h.DB.Query(ctx, query, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve categories")
		return
	}
	defer rows.Close()

	items := make([]merchantCategory, 0)
	for rows.Next() {
		var (
			category         merchantCategory
			description      pgtype.Text
			createdByUserID  pgtype.Int8
			updatedByUserID  pgtype.Int8
			deletedAt        pgtype.Timestamptz
			deletedByUserID  pgtype.Int8
			restoredAt       pgtype.Timestamptz
			restoredByUserID pgtype.Int8
			menusCount       int64
			menuItemsCount   int64
		)

		if err := rows.Scan(
			&category.ID,
			&category.MerchantID,
			&category.Name,
			&description,
			&category.SortOrder,
			&category.IsActive,
			&category.CreatedAt,
			&category.UpdatedAt,
			&createdByUserID,
			&updatedByUserID,
			&deletedAt,
			&deletedByUserID,
			&restoredAt,
			&restoredByUserID,
			&menusCount,
			&menuItemsCount,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve categories")
			return
		}

		if description.Valid {
			category.Description = &description.String
		}
		if createdByUserID.Valid {
			category.CreatedByUserID = &createdByUserID.Int64
		}
		if updatedByUserID.Valid {
			category.UpdatedByUserID = &updatedByUserID.Int64
		}
		if deletedAt.Valid {
			category.DeletedAt = &deletedAt.Time
		}
		if deletedByUserID.Valid {
			category.DeletedByUserID = &deletedByUserID.Int64
		}
		if restoredAt.Valid {
			category.RestoredAt = &restoredAt.Time
		}
		if restoredByUserID.Valid {
			category.RestoredByUserID = &restoredByUserID.Int64
		}

		category.Count = categoryCount{Menus: menusCount, MenuItems: menuItemsCount}
		items = append(items, category)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       items,
		"message":    "Categories retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoriesCreate(w http.ResponseWriter, r *http.Request) {
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

	var payload createCategoryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name is required")
		return
	}

	sortOrder := int32(0)
	if payload.SortOrder != nil {
		sortOrder = *payload.SortOrder
	} else {
		var maxSort pgtype.Int4
		if err := h.DB.QueryRow(ctx, "select max(sort_order) from menu_categories where merchant_id = $1", *authCtx.MerchantID).Scan(&maxSort); err == nil && maxSort.Valid {
			sortOrder = maxSort.Int32 + 1
		}
	}

	query := `
		insert into menu_categories (merchant_id, name, description, sort_order, is_active, created_at, updated_at, created_by_user_id)
		values ($1, $2, $3, $4, true, now(), now(), $5)
		returning id, merchant_id, name, description, sort_order, is_active, created_at, updated_at, created_by_user_id
	`
	var (
		category        merchantCategory
		description     pgtype.Text
		createdByUserID pgtype.Int8
	)
	if err := h.DB.QueryRow(ctx, query, *authCtx.MerchantID, name, payload.Description, sortOrder, authCtx.UserID).
		Scan(
			&category.ID,
			&category.MerchantID,
			&category.Name,
			&description,
			&category.SortOrder,
			&category.IsActive,
			&category.CreatedAt,
			&category.UpdatedAt,
			&createdByUserID,
		); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create category")
		return
	}

	if description.Valid {
		category.Description = &description.String
	}
	if createdByUserID.Valid {
		category.CreatedByUserID = &createdByUserID.Int64
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       category,
		"message":    "Category created successfully",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantCategoriesUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var payload updateCategoryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Name != nil && strings.TrimSpace(*payload.Name) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category name is required")
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
	if payload.SortOrder != nil {
		setClauses = append(setClauses, "sort_order = $"+intToString(len(args)+1))
		args = append(args, *payload.SortOrder)
	}

	setClauses = append(setClauses, "updated_by_user_id = $"+intToString(len(args)+1))
	args = append(args, authCtx.UserID)

	args = append(args, categoryID)
	query := `
		update menu_categories
		set ` + strings.Join(setClauses, ", ") + `
		where id = $` + intToString(len(args)) + `
		returning id, merchant_id, name, description, sort_order, is_active, created_at, updated_at,
			created_by_user_id, updated_by_user_id, deleted_at, deleted_by_user_id, restored_at, restored_by_user_id
	`

	var (
		category         merchantCategory
		description      pgtype.Text
		createdByUserID  pgtype.Int8
		updatedByUserID  pgtype.Int8
		deletedAt        pgtype.Timestamptz
		deletedByUserID  pgtype.Int8
		restoredAt       pgtype.Timestamptz
		restoredByUserID pgtype.Int8
	)

	if err := h.DB.QueryRow(ctx, query, args...).Scan(
		&category.ID,
		&category.MerchantID,
		&category.Name,
		&description,
		&category.SortOrder,
		&category.IsActive,
		&category.CreatedAt,
		&category.UpdatedAt,
		&createdByUserID,
		&updatedByUserID,
		&deletedAt,
		&deletedByUserID,
		&restoredAt,
		&restoredByUserID,
	); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
		return
	}

	if description.Valid {
		category.Description = &description.String
	}
	if createdByUserID.Valid {
		category.CreatedByUserID = &createdByUserID.Int64
	}
	if updatedByUserID.Valid {
		category.UpdatedByUserID = &updatedByUserID.Int64
	}
	if deletedAt.Valid {
		category.DeletedAt = &deletedAt.Time
	}
	if deletedByUserID.Valid {
		category.DeletedByUserID = &deletedByUserID.Int64
	}
	if restoredAt.Valid {
		category.RestoredAt = &restoredAt.Time
	}
	if restoredByUserID.Valid {
		category.RestoredByUserID = &restoredByUserID.Int64
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       category,
		"message":    "Category updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoriesDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	// Ensure category exists
	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1)", categoryID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "delete from menu_category_items where category_id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	if _, err := tx.Exec(ctx, `
		update menu_categories
		set deleted_at = now(), deleted_by_user_id = $2
		where id = $1
	`, categoryID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Category deleted successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoriesDeletePreview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var (
		categoryName string
	)
	if err := h.DB.QueryRow(ctx, `
		select name
		from menu_categories
		where id = $1 and merchant_id = $2 and deleted_at is null
	`, categoryID, *authCtx.MerchantID).Scan(&categoryName); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
		return
	}

	menuRows, err := h.DB.Query(ctx, `
		select distinct m.id, m.name, m.image_url, m.is_active
		from menu_category_items mci
		join menus m on m.id = mci.menu_id
		where mci.category_id = $1
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
		var imagePtr *string
		if imageURL.Valid {
			imagePtr = &imageURL.String
		}
		menus = append(menus, menuPreview{ID: menuID, Name: menuName, ImageURL: imagePtr, IsActive: isActive})
	}

	previewMenus := menus
	if len(menus) > 10 {
		previewMenus = menus[:10]
	}

	menuNames := make([]string, 0, len(previewMenus))
	for _, m := range previewMenus {
		menuNames = append(menuNames, m.Name)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"category": map[string]any{
				"id":   categoryID,
				"name": categoryName,
			},
			"addonCategory": map[string]any{
				"id":   categoryID,
				"name": categoryName,
			},
			"menuItemsCount":     len(menus),
			"menuNames":          menuNames,
			"affectedMenusCount": len(menus),
			"affectedMenus":      previewMenus,
			"hasMoreMenus":       len(menus) > 10,
			"addonItemsCount":    0,
			"addonItems":         []map[string]any{},
			"hasMoreItems":       false,
			"message": func() string {
				if len(menus) > 0 {
					return "This category is assigned to " + intToString(len(menus)) + " menu item(s). Deleting will remove the category from these menus."
				}
				return "This category is not assigned to any menu items. It can be safely deleted."
			}(),
			"canDelete": true,
		},
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoryMenusList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&exists); err != nil || !exists {
		response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Category not found")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select m.*
		from menu_category_items mci
		join menus m on m.id = mci.menu_id
		where mci.category_id = $1
	`, categoryID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
		return
	}
	defer rows.Close()

	menus := make([]map[string]any, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
			return
		}
		cols := rows.FieldDescriptions()
		rowMap := make(map[string]any, len(cols))
		for i, col := range cols {
			rowMap[string(col.Name)] = values[i]
		}
		menus = append(menus, rowMap)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       menus,
		"message":    "Menus retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoryMenusAdd(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var payload struct {
		MenuID int64 `json:"menuId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.MenuID == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Menu ID is required")
		return
	}

	var categoryExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&categoryExists); err != nil || !categoryExists {
		response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Category not found")
		return
	}

	var menuExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menus where id = $1 and merchant_id = $2)", payload.MenuID, *authCtx.MerchantID).Scan(&menuExists); err != nil || !menuExists {
		response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu not found")
		return
	}

	var linkExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_category_items where menu_id = $1 and category_id = $2)", payload.MenuID, categoryID).Scan(&linkExists); err == nil && linkExists {
		response.Error(w, http.StatusBadRequest, "ALREADY_EXISTS", "Menu is already in this category")
		return
	}

	var linkID int64
	if err := h.DB.QueryRow(ctx, "insert into menu_category_items (menu_id, category_id, created_at) values ($1, $2, now()) returning id", payload.MenuID, categoryID).Scan(&linkID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to add menu to category")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       map[string]any{"id": linkID, "menuId": payload.MenuID, "categoryId": categoryID},
		"message":    "Menu added to category successfully",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantCategoryMenusRemove(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}
	menuID, err := readPathInt64(r, "menuId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var categoryExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1 and merchant_id = $2)", categoryID, *authCtx.MerchantID).Scan(&categoryExists); err != nil || !categoryExists {
		response.Error(w, http.StatusNotFound, "CATEGORY_NOT_FOUND", "Category not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from menu_category_items where menu_id = $1 and category_id = $2", menuID, categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to remove menu from category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Menu removed from category successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoriesPermanentDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var categoryName string
	if err := h.DB.QueryRow(ctx, `
		select name
		from menu_categories
		where id = $1 and merchant_id = $2 and deleted_at is not null
	`, categoryID, *authCtx.MerchantID).Scan(&categoryName); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Archived category not found")
		return
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "delete from menu_category_items where category_id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	if _, err := tx.Exec(ctx, "delete from menu_categories where id = $1", categoryID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete category")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Category permanently deleted",
		"data":    map[string]any{"id": categoryID, "name": categoryName},
	})
}

func (h *Handler) MerchantCategoriesRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, "select deleted_at from menu_categories where id = $1", categoryID).Scan(&deletedAt); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
		return
	}
	if !deletedAt.Valid {
		response.Error(w, http.StatusBadRequest, "NOT_FOUND", "Category is not deleted")
		return
	}

	query := `
		update menu_categories
		set deleted_at = null,
			deleted_by_user_id = null,
			restored_at = now(),
			restored_by_user_id = $2,
			is_active = true
		where id = $1
		returning id, merchant_id, name, description, sort_order, is_active, created_at, updated_at,
			created_by_user_id, updated_by_user_id, deleted_at, deleted_by_user_id, restored_at, restored_by_user_id
	`

	var (
		category         merchantCategory
		description      pgtype.Text
		createdByUserID  pgtype.Int8
		updatedByUserID  pgtype.Int8
		deletedAtOut     pgtype.Timestamptz
		deletedByUserID  pgtype.Int8
		restoredAt       pgtype.Timestamptz
		restoredByUserID pgtype.Int8
	)

	if err := h.DB.QueryRow(ctx, query, categoryID, authCtx.UserID).Scan(
		&category.ID,
		&category.MerchantID,
		&category.Name,
		&description,
		&category.SortOrder,
		&category.IsActive,
		&category.CreatedAt,
		&category.UpdatedAt,
		&createdByUserID,
		&updatedByUserID,
		&deletedAtOut,
		&deletedByUserID,
		&restoredAt,
		&restoredByUserID,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore category")
		return
	}

	if description.Valid {
		category.Description = &description.String
	}
	if createdByUserID.Valid {
		category.CreatedByUserID = &createdByUserID.Int64
	}
	if updatedByUserID.Valid {
		category.UpdatedByUserID = &updatedByUserID.Int64
	}
	if deletedAtOut.Valid {
		category.DeletedAt = &deletedAtOut.Time
	}
	if deletedByUserID.Valid {
		category.DeletedByUserID = &deletedByUserID.Int64
	}
	if restoredAt.Valid {
		category.RestoredAt = &restoredAt.Time
	}
	if restoredByUserID.Valid {
		category.RestoredByUserID = &restoredByUserID.Int64
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Menu category restored successfully",
		"data":    category,
	})
}

func (h *Handler) MerchantCategoriesToggleActive(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
		return
	}

	var (
		isActive   bool
		merchantID int64
	)
	if err := h.DB.QueryRow(ctx, "select is_active, merchant_id from menu_categories where id = $1", categoryID).Scan(&isActive, &merchantID); err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
		return
	}
	if merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Category does not belong to this merchant")
		return
	}

	var updated merchantCategory
	var (
		description      pgtype.Text
		createdByUserID  pgtype.Int8
		updatedByUserID  pgtype.Int8
		deletedAt        pgtype.Timestamptz
		deletedByUserID  pgtype.Int8
		restoredAt       pgtype.Timestamptz
		restoredByUserID pgtype.Int8
	)
	if err := h.DB.QueryRow(ctx, `
		update menu_categories
		set is_active = $2
		where id = $1
		returning id, merchant_id, name, description, sort_order, is_active, created_at, updated_at,
			created_by_user_id, updated_by_user_id, deleted_at, deleted_by_user_id, restored_at, restored_by_user_id
	`, categoryID, !isActive).Scan(
		&updated.ID,
		&updated.MerchantID,
		&updated.Name,
		&description,
		&updated.SortOrder,
		&updated.IsActive,
		&updated.CreatedAt,
		&updated.UpdatedAt,
		&createdByUserID,
		&updatedByUserID,
		&deletedAt,
		&deletedByUserID,
		&restoredAt,
		&restoredByUserID,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle category status")
		return
	}

	if description.Valid {
		updated.Description = &description.String
	}
	if createdByUserID.Valid {
		updated.CreatedByUserID = &createdByUserID.Int64
	}
	if updatedByUserID.Valid {
		updated.UpdatedByUserID = &updatedByUserID.Int64
	}
	if deletedAt.Valid {
		updated.DeletedAt = &deletedAt.Time
	}
	if deletedByUserID.Valid {
		updated.DeletedByUserID = &deletedByUserID.Int64
	}
	if restoredAt.Valid {
		updated.RestoredAt = &restoredAt.Time
	}
	if restoredByUserID.Valid {
		updated.RestoredByUserID = &restoredByUserID.Int64
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    "Category status updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantCategoriesBulkDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.IDs) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category IDs are required")
		return
	}

	categoryIDs := make([]int64, 0, len(payload.IDs))
	for _, raw := range payload.IDs {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		id, err := parseStringToInt64(raw)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category id")
			return
		}
		categoryIDs = append(categoryIDs, id)
	}
	if len(categoryIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Category IDs are required")
		return
	}

	var foundCount int64
	if err := h.DB.QueryRow(ctx, `
		select count(*)
		from menu_categories
		where id = any($1) and merchant_id = $2 and deleted_at is null
	`, categoryIDs, *authCtx.MerchantID).Scan(&foundCount); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete categories")
		return
	}

	if foundCount != int64(len(categoryIDs)) {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Some categories not found or already deleted")
		return
	}

	cmd, err := h.DB.Exec(ctx, `
		update menu_categories
		set deleted_at = now(), deleted_by_user_id = $3
		where id = any($1) and merchant_id = $2 and deleted_at is null
	`, categoryIDs, *authCtx.MerchantID, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete categories")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Successfully deleted " + intToString(int(cmd.RowsAffected())) + " category(ies)",
		"data":    map[string]any{"count": cmd.RowsAffected()},
	})
}

func (h *Handler) MerchantCategoriesBulkSoftDeleteToken(w http.ResponseWriter, r *http.Request) {
	idsParam := strings.TrimSpace(r.URL.Query().Get("ids"))
	if idsParam == "" {
		response.Error(w, http.StatusBadRequest, "MISSING_IDS", "Please provide ids as query parameter")
		return
	}

	parts := strings.Split(idsParam, ",")
	ids := make([]int, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		id, err := parseStringToInt(trimmed)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "No valid IDs provided")
		return
	}

	confirmationToken := generateCategoryConfirmationToken(ids)
	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"itemCount":         len(ids),
			"confirmationToken": confirmationToken,
			"message":           "This will delete " + intToString(len(ids)) + " menu categories. Use this token to confirm.",
		},
	})
}

func (h *Handler) MerchantCategoriesBulkSoftDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkSoftDeletePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of category IDs")
		return
	}

	if len(payload.IDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_IDS", "Please provide an array of category IDs")
		return
	}

	if len(payload.IDs) > 50 {
		response.Error(w, http.StatusBadRequest, "TOO_MANY_ITEMS", "Cannot delete more than 50 categories at once")
		return
	}

	idsInt := make([]int, 0, len(payload.IDs))
	for _, id := range payload.IDs {
		idsInt = append(idsInt, int(id))
	}

	expectedToken := generateCategoryConfirmationToken(idsInt)
	if payload.ConfirmationToken != expectedToken {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "CONFIRMATION_REQUIRED",
			"message":    "Please confirm this bulk delete operation",
			"statusCode": http.StatusBadRequest,
			"data": map[string]any{
				"itemCount":         len(payload.IDs),
				"confirmationToken": expectedToken,
			},
		})
		return
	}

	cmd, err := h.DB.Exec(ctx, `
		update menu_categories
		set deleted_at = now(), deleted_by_user_id = $2
		where id = any($1)
	`, payload.IDs, authCtx.UserID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "BULK_DELETE_FAILED", "Failed to bulk delete categories")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Successfully deleted " + intToString(int(cmd.RowsAffected())) + " menu categories",
		"data":    map[string]any{"deletedCount": cmd.RowsAffected()},
	})
}

func (h *Handler) MerchantCategoriesReorder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload reorderCategoryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Categories) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Categories array is required")
		return
	}

	categoryIDs := make([]int64, 0, len(payload.Categories))
	for _, cat := range payload.Categories {
		id, err := parseStringToInt64(cat.ID)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Some categories not found or do not belong to your merchant")
			return
		}
		categoryIDs = append(categoryIDs, id)
	}

	var existingCount int64
	if err := h.DB.QueryRow(ctx, `
		select count(*)
		from menu_categories
		where id = any($1) and merchant_id = $2 and deleted_at is null
	`, categoryIDs, *authCtx.MerchantID).Scan(&existingCount); err != nil || existingCount != int64(len(categoryIDs)) {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Some categories not found or do not belong to your merchant")
		return
	}

	sorted := make([]struct {
		ID        int64
		SortOrder int
	}, 0, len(payload.Categories))
	for _, cat := range payload.Categories {
		id, _ := parseStringToInt64(cat.ID)
		sorted = append(sorted, struct {
			ID        int64
			SortOrder int
		}{ID: id, SortOrder: cat.SortOrder})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].SortOrder < sorted[j].SortOrder
	})

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder categories")
		return
	}
	defer tx.Rollback(ctx)

	for idx, cat := range sorted {
		if _, err := tx.Exec(ctx, `
			update menu_categories
			set sort_order = $2, updated_by_user_id = $3
			where id = $1
		`, cat.ID, idx, authCtx.UserID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder categories")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder categories")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select
			mc.id, mc.merchant_id, mc.name, mc.description, mc.sort_order, mc.is_active,
			mc.created_at, mc.updated_at, mc.created_by_user_id, mc.updated_by_user_id,
			mc.deleted_at, mc.deleted_by_user_id, mc.restored_at, mc.restored_by_user_id,
			coalesce((select count(*) from menu_category_items mci where mci.category_id = mc.id), 0) as menu_items_count,
			coalesce((select count(*) from menus m where m.category_id = mc.id and m.deleted_at is null), 0) as menus_count
		from menu_categories mc
		where mc.merchant_id = $1 and mc.deleted_at is null
		order by mc.sort_order asc
	`, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder categories")
		return
	}
	defer rows.Close()

	items := make([]merchantCategory, 0)
	for rows.Next() {
		var (
			category         merchantCategory
			description      pgtype.Text
			createdByUserID  pgtype.Int8
			updatedByUserID  pgtype.Int8
			deletedAt        pgtype.Timestamptz
			deletedByUserID  pgtype.Int8
			restoredAt       pgtype.Timestamptz
			restoredByUserID pgtype.Int8
			menuItemsCount   int64
			menusCount       int64
		)

		if err := rows.Scan(
			&category.ID,
			&category.MerchantID,
			&category.Name,
			&description,
			&category.SortOrder,
			&category.IsActive,
			&category.CreatedAt,
			&category.UpdatedAt,
			&createdByUserID,
			&updatedByUserID,
			&deletedAt,
			&deletedByUserID,
			&restoredAt,
			&restoredByUserID,
			&menuItemsCount,
			&menusCount,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to reorder categories")
			return
		}

		if description.Valid {
			category.Description = &description.String
		}
		if createdByUserID.Valid {
			category.CreatedByUserID = &createdByUserID.Int64
		}
		if updatedByUserID.Valid {
			category.UpdatedByUserID = &updatedByUserID.Int64
		}
		if deletedAt.Valid {
			category.DeletedAt = &deletedAt.Time
		}
		if deletedByUserID.Valid {
			category.DeletedByUserID = &deletedByUserID.Int64
		}
		if restoredAt.Valid {
			category.RestoredAt = &restoredAt.Time
		}
		if restoredByUserID.Valid {
			category.RestoredByUserID = &restoredByUserID.Int64
		}
		category.Count = categoryCount{Menus: menusCount, MenuItems: menuItemsCount}
		items = append(items, category)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Categories reordered successfully",
		"data":    items,
	})
}

func generateCategoryConfirmationToken(ids []int) string {
	sorted := make([]int, len(ids))
	copy(sorted, ids)
	sort.Ints(sorted)
	return "DELETE_CATS_" + intToString(len(sorted)) + "_ITEMS_" + intToString(sorted[0]) + "_" + intToString(sorted[len(sorted)-1])
}

func (h *Handler) MerchantCategoriesBulkSoftDeleteLegacy(w http.ResponseWriter, r *http.Request) {
	// alias to keep compatibility if needed
	h.MerchantCategoriesBulkSoftDelete(w, r)
}
