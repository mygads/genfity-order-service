package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type menuCategoryRef struct {
	Category struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"category"`
}

type menuAddonItem struct {
	ID                 int64     `json:"id"`
	AddonCategoryID    int64     `json:"addonCategoryId"`
	Name               string    `json:"name"`
	Description        *string   `json:"description"`
	Price              float64   `json:"price"`
	IsActive           bool      `json:"isActive"`
	TrackStock         bool      `json:"trackStock"`
	StockQty           *int32    `json:"stockQty"`
	LowStockThreshold  *int32    `json:"lowStockThreshold"`
	InputType          string    `json:"inputType"`
	DisplayOrder       int32     `json:"displayOrder"`
	AutoResetStock     bool      `json:"autoResetStock"`
	DailyStockTemplate *int32    `json:"dailyStockTemplate"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type menuAddonCategory struct {
	MenuID          int64 `json:"menuId"`
	AddonCategoryID int64 `json:"addonCategoryId"`
	DisplayOrder    int32 `json:"displayOrder"`
	IsRequired      bool  `json:"isRequired"`
	AddonCategory   struct {
		ID           int64           `json:"id"`
		MerchantID   int64           `json:"merchantId"`
		Name         string          `json:"name"`
		Description  *string         `json:"description"`
		MinSelection int32           `json:"minSelection"`
		MaxSelection *int32          `json:"maxSelection"`
		IsActive     bool            `json:"isActive"`
		AddonItems   []menuAddonItem `json:"addonItems"`
	} `json:"addonCategory"`
}

type merchantMenu struct {
	ID                 int64               `json:"id"`
	MerchantID         int64               `json:"merchantId"`
	CategoryID         *int64              `json:"categoryId"`
	Name               string              `json:"name"`
	Description        *string             `json:"description"`
	Price              float64             `json:"price"`
	ImageURL           *string             `json:"imageUrl"`
	ImageThumbURL      *string             `json:"imageThumbUrl"`
	ImageThumbMeta     any                 `json:"imageThumbMeta"`
	StockPhotoID       *int64              `json:"stockPhotoId"`
	IsActive           bool                `json:"isActive"`
	TrackStock         bool                `json:"trackStock"`
	StockQty           *int32              `json:"stockQty"`
	CreatedAt          time.Time           `json:"createdAt"`
	UpdatedAt          time.Time           `json:"updatedAt"`
	AutoResetStock     bool                `json:"autoResetStock"`
	DailyStockTemplate *int32              `json:"dailyStockTemplate"`
	LastStockResetAt   *time.Time          `json:"lastStockResetAt"`
	CreatedByUserID    *int64              `json:"createdByUserId"`
	DeletedAt          *time.Time          `json:"deletedAt"`
	DeletedByUserID    *int64              `json:"deletedByUserId"`
	UpdatedByUserID    *int64              `json:"updatedByUserId"`
	RestoredAt         *time.Time          `json:"restoredAt"`
	RestoredByUserID   *int64              `json:"restoredByUserId"`
	IsSpicy            bool                `json:"isSpicy"`
	IsBestSeller       bool                `json:"isBestSeller"`
	IsSignature        bool                `json:"isSignature"`
	IsRecommended      bool                `json:"isRecommended"`
	ScheduleEnabled    bool                `json:"scheduleEnabled"`
	ScheduleStartTime  *string             `json:"scheduleStartTime"`
	ScheduleEndTime    *string             `json:"scheduleEndTime"`
	ScheduleDays       []int32             `json:"scheduleDays"`
	CostPrice          *float64            `json:"costPrice"`
	LowStockThreshold  *int32              `json:"lowStockThreshold"`
	Categories         []menuCategoryRef   `json:"categories"`
	AddonCategories    []menuAddonCategory `json:"addonCategories"`
	Category           *struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	} `json:"category,omitempty"`
}

type menuCreatePayload struct {
	CategoryID         *int64  `json:"categoryId"`
	Name               string  `json:"name"`
	Description        *string `json:"description"`
	Price              float64 `json:"price"`
	ImageURL           *string `json:"imageUrl"`
	ImageThumbURL      *string `json:"imageThumbUrl"`
	ImageThumbMeta     any     `json:"imageThumbMeta"`
	StockPhotoID       *int64  `json:"stockPhotoId"`
	IsActive           *bool   `json:"isActive"`
	IsSpicy            *bool   `json:"isSpicy"`
	IsBestSeller       *bool   `json:"isBestSeller"`
	IsSignature        *bool   `json:"isSignature"`
	IsRecommended      *bool   `json:"isRecommended"`
	TrackStock         *bool   `json:"trackStock"`
	StockQty           *int32  `json:"stockQty"`
	DailyStockTemplate *int32  `json:"dailyStockTemplate"`
	AutoResetStock     *bool   `json:"autoResetStock"`
}

type menuUpdatePayload struct {
	CategoryID         *int64   `json:"categoryId"`
	Name               *string  `json:"name"`
	Description        *string  `json:"description"`
	Price              *float64 `json:"price"`
	ImageURL           *string  `json:"imageUrl"`
	ImageThumbURL      *string  `json:"imageThumbUrl"`
	ImageThumbMeta     any      `json:"imageThumbMeta"`
	StockPhotoID       *int64   `json:"stockPhotoId"`
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

func (h *Handler) MerchantMenuList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	categoryIDParam := strings.TrimSpace(r.URL.Query().Get("categoryId"))
	var categoryID *int64
	if categoryIDParam != "" {
		parsed, err := parseStringToInt64(categoryIDParam)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid categoryId")
			return
		}
		categoryID = &parsed
	}

	var merchantExists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from merchants where id = $1)", *authCtx.MerchantID).Scan(&merchantExists); err != nil || !merchantExists {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, categoryID, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       menus,
		"message":    "Menus retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuCreate(w http.ResponseWriter, r *http.Request) {
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

	var payload menuCreatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Menu name is required")
		return
	}

	if payload.Price < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Price must be greater than or equal to 0")
		return
	}

	trackStock := payload.TrackStock != nil && *payload.TrackStock
	if trackStock && (payload.StockQty == nil || *payload.StockQty < 0) {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Stock quantity is required when trackStock is true")
		return
	}

	if payload.CategoryID != nil {
		var categoryExists bool
		if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1 and merchant_id = $2)", *payload.CategoryID, *authCtx.MerchantID).Scan(&categoryExists); err != nil || !categoryExists {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
			return
		}
	}

	// Resolve stock photo payload
	var (
		resolvedImageURL       = payload.ImageURL
		resolvedImageThumbURL  = payload.ImageThumbURL
		resolvedImageThumbMeta = payload.ImageThumbMeta
	)
	if payload.StockPhotoID != nil {
		var stockImageURL pgtype.Text
		var stockThumbURL pgtype.Text
		var stockThumbMeta []byte
		var stockActive bool
		if err := h.DB.QueryRow(ctx, `
			select image_url, thumbnail_url, thumbnail_meta, is_active
			from stock_photos
			where id = $1
		`, *payload.StockPhotoID).Scan(&stockImageURL, &stockThumbURL, &stockThumbMeta, &stockActive); err != nil || !stockActive {
			response.Error(w, http.StatusBadRequest, "NOT_FOUND", "Stock photo not found")
			return
		}
		if stockImageURL.Valid {
			resolvedImageURL = &stockImageURL.String
		}
		if stockThumbURL.Valid {
			resolvedImageThumbURL = &stockThumbURL.String
		}
		if len(stockThumbMeta) > 0 {
			resolvedImageThumbMeta = stockThumbMeta
		}
	}

	isActive := true
	if payload.IsActive != nil {
		isActive = *payload.IsActive
	}

	isSpicy := payload.IsSpicy != nil && *payload.IsSpicy
	isBestSeller := payload.IsBestSeller != nil && *payload.IsBestSeller
	isSignature := payload.IsSignature != nil && *payload.IsSignature
	isRecommended := payload.IsRecommended != nil && *payload.IsRecommended
	autoResetStock := payload.AutoResetStock != nil && *payload.AutoResetStock

	query := `
		insert into menus (
			merchant_id, category_id, name, description, price, image_url, image_thumb_url, image_thumb_meta,
			stock_photo_id, is_active, is_spicy, is_best_seller, is_signature, is_recommended, track_stock,
			stock_qty, daily_stock_template, auto_reset_stock, created_at, updated_at, created_by_user_id, updated_by_user_id
		) values (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15,
			$16, $17, $18, now(), now(), $19, $20
		)
		returning id
	`

	var imageThumbMeta []byte
	if resolvedImageThumbMeta != nil {
		switch v := resolvedImageThumbMeta.(type) {
		case []byte:
			imageThumbMeta = v
		default:
			if raw, err := json.Marshal(v); err == nil {
				imageThumbMeta = raw
			}
		}
	}

	var newID int64
	if err := h.DB.QueryRow(ctx, query,
		*authCtx.MerchantID,
		payload.CategoryID,
		name,
		payload.Description,
		payload.Price,
		resolvedImageURL,
		resolvedImageThumbURL,
		imageThumbMeta,
		payload.StockPhotoID,
		isActive,
		isSpicy,
		isBestSeller,
		isSignature,
		isRecommended,
		trackStock,
		payload.StockQty,
		payload.DailyStockTemplate,
		autoResetStock,
		authCtx.UserID,
		authCtx.UserID,
	).Scan(&newID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu")
		return
	}

	var created *merchantMenu
	for i := range menus {
		if menus[i].ID == newID {
			created = &menus[i]
			break
		}
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success":    true,
		"data":       created,
		"message":    "Menu created successfully",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantMenuDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	menuID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
		return
	}

	var found *merchantMenu
	for i := range menus {
		if menus[i].ID == menuID {
			found = &menus[i]
			break
		}
	}

	if found == nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       found,
		"message":    "Menu retrieved successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuUpdate(w http.ResponseWriter, r *http.Request) {
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

	var payload menuUpdatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if payload.Name != nil && strings.TrimSpace(*payload.Name) == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Menu name is required")
		return
	}

	if payload.Price != nil && *payload.Price < 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Price must be greater than or equal to 0")
		return
	}

	if payload.CategoryID != nil {
		var categoryExists bool
		if err := h.DB.QueryRow(ctx, "select exists(select 1 from menu_categories where id = $1 and merchant_id = $2)", *payload.CategoryID, *authCtx.MerchantID).Scan(&categoryExists); err != nil || !categoryExists {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Category not found")
			return
		}
	}

	setClauses := []string{"updated_at = now()"}
	args := []any{}

	if payload.CategoryID != nil {
		setClauses = append(setClauses, "category_id = $"+intToString(len(args)+1))
		args = append(args, payload.CategoryID)
	}
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
	if payload.ImageURL != nil {
		setClauses = append(setClauses, "image_url = $"+intToString(len(args)+1))
		args = append(args, payload.ImageURL)
	}
	if payload.ImageThumbURL != nil {
		setClauses = append(setClauses, "image_thumb_url = $"+intToString(len(args)+1))
		args = append(args, payload.ImageThumbURL)
	}
	if payload.ImageThumbMeta != nil {
		setClauses = append(setClauses, "image_thumb_meta = $"+intToString(len(args)+1))
		if raw, err := json.Marshal(payload.ImageThumbMeta); err == nil {
			args = append(args, raw)
		}
	}
	if payload.StockPhotoID != nil {
		setClauses = append(setClauses, "stock_photo_id = $"+intToString(len(args)+1))
		args = append(args, payload.StockPhotoID)
	}
	if payload.IsActive != nil {
		setClauses = append(setClauses, "is_active = $"+intToString(len(args)+1))
		args = append(args, *payload.IsActive)
	}
	if payload.IsSpicy != nil {
		setClauses = append(setClauses, "is_spicy = $"+intToString(len(args)+1))
		args = append(args, *payload.IsSpicy)
	}
	if payload.IsBestSeller != nil {
		setClauses = append(setClauses, "is_best_seller = $"+intToString(len(args)+1))
		args = append(args, *payload.IsBestSeller)
	}
	if payload.IsSignature != nil {
		setClauses = append(setClauses, "is_signature = $"+intToString(len(args)+1))
		args = append(args, *payload.IsSignature)
	}
	if payload.IsRecommended != nil {
		setClauses = append(setClauses, "is_recommended = $"+intToString(len(args)+1))
		args = append(args, *payload.IsRecommended)
	}
	if payload.TrackStock != nil {
		setClauses = append(setClauses, "track_stock = $"+intToString(len(args)+1))
		args = append(args, *payload.TrackStock)
	}
	if payload.StockQty != nil {
		setClauses = append(setClauses, "stock_qty = $"+intToString(len(args)+1))
		args = append(args, payload.StockQty)
	}
	if payload.DailyStockTemplate != nil {
		setClauses = append(setClauses, "daily_stock_template = $"+intToString(len(args)+1))
		args = append(args, payload.DailyStockTemplate)
	}
	if payload.AutoResetStock != nil {
		setClauses = append(setClauses, "auto_reset_stock = $"+intToString(len(args)+1))
		args = append(args, *payload.AutoResetStock)
	}

	setClauses = append(setClauses, "updated_by_user_id = $"+intToString(len(args)+1))
	args = append(args, authCtx.UserID)

	args = append(args, menuID)

	query := `update menus set ` + strings.Join(setClauses, ", ") + ` where id = $` + intToString(len(args))
	if _, err := h.DB.Exec(ctx, query, args...); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu")
		return
	}

	var updated *merchantMenu
	for i := range menus {
		if menus[i].ID == menuID {
			updated = &menus[i]
			break
		}
	}

	if updated == nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    "Menu updated successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuDelete(w http.ResponseWriter, r *http.Request) {
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

	var merchantID int64
	if err := h.DB.QueryRow(ctx, "select merchant_id from menus where id = $1", menuID).Scan(&merchantID); err != nil || merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu item not found or does not belong to your merchant")
		return
	}

	if _, err := h.DB.Exec(ctx, "update menus set deleted_at = now(), deleted_by_user_id = $2 where id = $1", menuID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete menu")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Menu deleted successfully",
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuToggleActive(w http.ResponseWriter, r *http.Request) {
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

	var merchantID int64
	var isActive bool
	if err := h.DB.QueryRow(ctx, "select merchant_id, is_active from menus where id = $1 and deleted_at is null", menuID).Scan(&merchantID, &isActive); err != nil || merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	if err := h.DB.QueryRow(ctx, `
		update menus
		set is_active = not is_active, updated_at = now(), updated_by_user_id = $2
		where id = $1
		returning is_active
	`, menuID, authCtx.UserID).Scan(&isActive); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle menu status")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle menu status")
		return
	}

	var updated *merchantMenu
	for i := range menus {
		if menus[i].ID == menuID {
			updated = &menus[i]
			break
		}
	}

	message := "Menu deactivated successfully"
	if isActive {
		message = "Menu activated successfully"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       updated,
		"message":    message,
		"statusCode": http.StatusOK,
	})
}

func (h *Handler) MerchantMenuRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	menuID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var merchantID int64
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, "select merchant_id, deleted_at from menus where id = $1", menuID).Scan(&merchantID, &deletedAt); err != nil || merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu not found")
		return
	}

	if !deletedAt.Valid {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Menu is not deleted")
		return
	}

	if _, err := h.DB.Exec(ctx, `
		update menus
		set deleted_at = null, deleted_by_user_id = null, restored_at = now(), restored_by_user_id = $2,
			is_active = true, updated_at = now(), updated_by_user_id = $2
		where id = $1
	`, menuID, authCtx.UserID); err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore menu")
		return
	}

	menus, err := h.fetchMenus(ctx, *authCtx.MerchantID, nil, true)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "RESTORE_FAILED", "Failed to restore menu")
		return
	}

	var restored *merchantMenu
	for i := range menus {
		if menus[i].ID == menuID {
			restored = &menus[i]
			break
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Menu restored successfully",
		"data":    restored,
	})
}

func (h *Handler) MerchantMenuPermanentDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	menuID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
		return
	}

	var merchantID int64
	var name string
	var deletedAt pgtype.Timestamptz
	if err := h.DB.QueryRow(ctx, `
		select merchant_id, name, deleted_at
		from menus
		where id = $1
	`, menuID).Scan(&merchantID, &name, &deletedAt); err != nil || merchantID != *authCtx.MerchantID || !deletedAt.Valid {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Archived menu not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from menu_category_items where menu_id = $1", menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete menu")
		return
	}
	if _, err := h.DB.Exec(ctx, "delete from menu_addon_categories where menu_id = $1", menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete menu")
		return
	}
	if _, err := h.DB.Exec(ctx, "delete from menus where id = $1", menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete menu")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Menu permanently deleted",
		"data": map[string]any{
			"id":   menuID,
			"name": name,
		},
	})
}

func (h *Handler) fetchMenus(ctx context.Context, merchantID int64, categoryID *int64, includeInactive bool) ([]merchantMenu, error) {

	args := []any{merchantID}
	where := "m.merchant_id = $1 and m.deleted_at is null"
	if !includeInactive {
		where += " and m.is_active = true"
	}
	if categoryID != nil {
		where += " and exists (select 1 from menu_category_items mci where mci.menu_id = m.id and mci.category_id = $2)"
		args = append(args, *categoryID)
	}

	query := `
		select
			m.id, m.merchant_id, m.category_id, m.name, m.description, m.price, m.image_url, m.image_thumb_url, m.image_thumb_meta,
			m.stock_photo_id, m.is_active, m.track_stock, m.stock_qty, m.created_at, m.updated_at,
			m.auto_reset_stock, m.daily_stock_template, m.last_stock_reset_at, m.created_by_user_id, m.deleted_at,
			m.deleted_by_user_id, m.updated_by_user_id, m.restored_at, m.restored_by_user_id,
			m.is_spicy, m.is_best_seller, m.is_signature, m.is_recommended,
			m.schedule_enabled, m.schedule_start_time, m.schedule_end_time, m.schedule_days, m.cost_price, m.low_stock_threshold
		from menus m
		where ` + where + `
		order by m.created_at desc
	`

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	menus := make([]merchantMenu, 0)
	menuIDs := make([]int64, 0)
	for rows.Next() {
		var (
			menu               merchantMenu
			categoryID         pgtype.Int8
			description        pgtype.Text
			imageURL           pgtype.Text
			imageThumbURL      pgtype.Text
			imageThumbMeta     []byte
			stockPhotoID       pgtype.Int8
			stockQty           pgtype.Int4
			dailyStockTemplate pgtype.Int4
			lastStockResetAt   pgtype.Timestamptz
			createdByUserID    pgtype.Int8
			deletedAt          pgtype.Timestamptz
			deletedByUserID    pgtype.Int8
			updatedByUserID    pgtype.Int8
			restoredAt         pgtype.Timestamptz
			restoredByUserID   pgtype.Int8
			scheduleStartTime  pgtype.Text
			scheduleEndTime    pgtype.Text
			scheduleDays       []int32
			costPrice          pgtype.Numeric
			lowStockThreshold  pgtype.Int4
			price              pgtype.Numeric
		)

		if err := rows.Scan(
			&menu.ID,
			&menu.MerchantID,
			&categoryID,
			&menu.Name,
			&description,
			&price,
			&imageURL,
			&imageThumbURL,
			&imageThumbMeta,
			&stockPhotoID,
			&menu.IsActive,
			&menu.TrackStock,
			&stockQty,
			&menu.CreatedAt,
			&menu.UpdatedAt,
			&menu.AutoResetStock,
			&dailyStockTemplate,
			&lastStockResetAt,
			&createdByUserID,
			&deletedAt,
			&deletedByUserID,
			&updatedByUserID,
			&restoredAt,
			&restoredByUserID,
			&menu.IsSpicy,
			&menu.IsBestSeller,
			&menu.IsSignature,
			&menu.IsRecommended,
			&menu.ScheduleEnabled,
			&scheduleStartTime,
			&scheduleEndTime,
			&scheduleDays,
			&costPrice,
			&lowStockThreshold,
		); err != nil {
			return nil, err
		}

		menu.Price = utils.NumericToFloat64(price)
		if description.Valid {
			menu.Description = &description.String
		}
		if imageURL.Valid {
			menu.ImageURL = &imageURL.String
		}
		if imageThumbURL.Valid {
			menu.ImageThumbURL = &imageThumbURL.String
		}
		if len(imageThumbMeta) > 0 {
			var decoded any
			if err := json.Unmarshal(imageThumbMeta, &decoded); err == nil {
				menu.ImageThumbMeta = decoded
			}
		}
		if stockPhotoID.Valid {
			menu.StockPhotoID = &stockPhotoID.Int64
		}
		if categoryID.Valid {
			menu.CategoryID = &categoryID.Int64
		}
		if stockQty.Valid {
			menu.StockQty = &stockQty.Int32
		}
		if dailyStockTemplate.Valid {
			menu.DailyStockTemplate = &dailyStockTemplate.Int32
		}
		if lastStockResetAt.Valid {
			menu.LastStockResetAt = &lastStockResetAt.Time
		}
		if createdByUserID.Valid {
			menu.CreatedByUserID = &createdByUserID.Int64
		}
		if deletedAt.Valid {
			menu.DeletedAt = &deletedAt.Time
		}
		if deletedByUserID.Valid {
			menu.DeletedByUserID = &deletedByUserID.Int64
		}
		if updatedByUserID.Valid {
			menu.UpdatedByUserID = &updatedByUserID.Int64
		}
		if restoredAt.Valid {
			menu.RestoredAt = &restoredAt.Time
		}
		if restoredByUserID.Valid {
			menu.RestoredByUserID = &restoredByUserID.Int64
		}
		if scheduleStartTime.Valid {
			menu.ScheduleStartTime = &scheduleStartTime.String
		}
		if scheduleEndTime.Valid {
			menu.ScheduleEndTime = &scheduleEndTime.String
		}
		if len(scheduleDays) > 0 {
			menu.ScheduleDays = scheduleDays
		}
		if costPrice.Valid {
			val := utils.NumericToFloat64(costPrice)
			menu.CostPrice = &val
		}
		if lowStockThreshold.Valid {
			menu.LowStockThreshold = &lowStockThreshold.Int32
		}

		menus = append(menus, menu)
		menuIDs = append(menuIDs, menu.ID)
	}

	if len(menuIDs) == 0 {
		return menus, nil
	}

	// Load categories (many-to-many)
	catRows, err := h.DB.Query(ctx, `
		select mci.menu_id, c.id, c.name
		from menu_category_items mci
		join menu_categories c on c.id = mci.category_id
		where mci.menu_id = any($1) and c.deleted_at is null
	`, menuIDs)
	if err != nil {
		return menus, nil
	}
	defer catRows.Close()

	categoryMap := make(map[int64][]menuCategoryRef)
	categoryNameMap := make(map[int64]string)
	for catRows.Next() {
		var menuID, categoryID int64
		var name string
		if err := catRows.Scan(&menuID, &categoryID, &name); err != nil {
			continue
		}
		ref := menuCategoryRef{}
		ref.Category.ID = categoryID
		ref.Category.Name = name
		categoryMap[menuID] = append(categoryMap[menuID], ref)
		if _, ok := categoryNameMap[categoryID]; !ok {
			categoryNameMap[categoryID] = name
		}
	}

	// Load addon categories
	addonRows, err := h.DB.Query(ctx, `
		select mac.menu_id, mac.addon_category_id, mac.display_order, mac.is_required,
			ac.id, ac.merchant_id, ac.name, ac.description, ac.min_selection, ac.max_selection, ac.is_active
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		where mac.menu_id = any($1) and ac.deleted_at is null
		order by mac.display_order asc
	`, menuIDs)
	if err != nil {
		return menus, nil
	}
	defer addonRows.Close()

	addonCategoryMap := make(map[int64][]menuAddonCategory)
	addonCategoryIDs := make([]int64, 0)
	addonCategoryByID := make(map[int64]*menuAddonCategory)

	for addonRows.Next() {
		var (
			menuID        int64
			addonCatID    int64
			displayOrder  int32
			isRequired    bool
			addonCatIDOut int64
			merchantID    int64
			name          string
			description   pgtype.Text
			minSelection  int32
			maxSelection  pgtype.Int4
			isActive      bool
		)

		if err := addonRows.Scan(
			&menuID,
			&addonCatIDOut,
			&displayOrder,
			&isRequired,
			&addonCatID,
			&merchantID,
			&name,
			&description,
			&minSelection,
			&maxSelection,
			&isActive,
		); err != nil {
			continue
		}

		if addonCatIDOut != addonCatID {
			addonCatID = addonCatIDOut
		}

		entry := menuAddonCategory{
			MenuID:          menuID,
			AddonCategoryID: addonCatID,
			DisplayOrder:    displayOrder,
			IsRequired:      isRequired,
		}
		entry.AddonCategory.ID = addonCatID
		entry.AddonCategory.MerchantID = merchantID
		entry.AddonCategory.Name = name
		if description.Valid {
			entry.AddonCategory.Description = &description.String
		}
		entry.AddonCategory.MinSelection = minSelection
		if maxSelection.Valid {
			val := maxSelection.Int32
			entry.AddonCategory.MaxSelection = &val
		}
		entry.AddonCategory.IsActive = isActive

		addonCategoryMap[menuID] = append(addonCategoryMap[menuID], entry)
		addonCategoryIDs = append(addonCategoryIDs, addonCatID)
		addonCategoryByID[addonCatID] = &addonCategoryMap[menuID][len(addonCategoryMap[menuID])-1]
	}

	// Load addon items (active only)
	if len(addonCategoryIDs) > 0 {
		itemRows, err := h.DB.Query(ctx, `
			select id, addon_category_id, name, description, price, is_active, track_stock, stock_qty,
				low_stock_threshold, input_type, display_order, auto_reset_stock, daily_stock_template, created_at, updated_at
			from addon_items
			where addon_category_id = any($1) and is_active = true and deleted_at is null
			order by display_order asc
		`, addonCategoryIDs)
		if err == nil {
			defer itemRows.Close()
			for itemRows.Next() {
				var (
					item          menuAddonItem
					description   pgtype.Text
					price         pgtype.Numeric
					stockQty      pgtype.Int4
					lowStock      pgtype.Int4
					dailyTemplate pgtype.Int4
				)
				if err := itemRows.Scan(
					&item.ID,
					&item.AddonCategoryID,
					&item.Name,
					&description,
					&price,
					&item.IsActive,
					&item.TrackStock,
					&stockQty,
					&lowStock,
					&item.InputType,
					&item.DisplayOrder,
					&item.AutoResetStock,
					&dailyTemplate,
					&item.CreatedAt,
					&item.UpdatedAt,
				); err != nil {
					continue
				}
				item.Price = utils.NumericToFloat64(price)
				if description.Valid {
					item.Description = &description.String
				}
				if stockQty.Valid {
					item.StockQty = &stockQty.Int32
				}
				if lowStock.Valid {
					item.LowStockThreshold = &lowStock.Int32
				}
				if dailyTemplate.Valid {
					item.DailyStockTemplate = &dailyTemplate.Int32
				}

				if target, ok := addonCategoryByID[item.AddonCategoryID]; ok {
					target.AddonCategory.AddonItems = append(target.AddonCategory.AddonItems, item)
				}
			}
		}
	}

	for i := range menus {
		menus[i].Categories = categoryMap[menus[i].ID]
		menus[i].AddonCategories = addonCategoryMap[menus[i].ID]
		if menus[i].CategoryID != nil {
			menus[i].Category = &struct {
				ID   int64  `json:"id"`
				Name string `json:"name"`
			}{}
			menus[i].Category.ID = *menus[i].CategoryID
			if name, ok := categoryNameMap[*menus[i].CategoryID]; ok {
				menus[i].Category.Name = name
			}
		}
	}

	return menus, nil
}
