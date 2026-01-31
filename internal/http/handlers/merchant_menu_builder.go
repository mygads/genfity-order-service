package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type menuBuilderPayload struct {
	ID                 *int64  `json:"id"`
	Name               string  `json:"name"`
	Description        *string `json:"description"`
	Price              float64 `json:"price"`
	ImageURL           *string `json:"imageUrl"`
	ImageThumbURL      *string `json:"imageThumbUrl"`
	ImageThumbMeta     any     `json:"imageThumbMeta"`
	StockPhotoID       *int64  `json:"stockPhotoId"`
	IsActive           *bool   `json:"isActive"`
	TrackStock         *bool   `json:"trackStock"`
	StockQty           *int32  `json:"stockQty"`
	DailyStockTemplate *int32  `json:"dailyStockTemplate"`
	AutoResetStock     *bool   `json:"autoResetStock"`
	IsSpicy            *bool   `json:"isSpicy"`
	IsBestSeller       *bool   `json:"isBestSeller"`
	IsSignature        *bool   `json:"isSignature"`
	IsRecommended      *bool   `json:"isRecommended"`
	CategoryIds        []int64 `json:"categoryIds"`
	AddonCategoryIds   []int64 `json:"addonCategoryIds"`
}

func (h *Handler) MerchantMenuBuilderCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Anda harus login terlebih dahulu")
		return
	}

	var payload menuBuilderPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Data tidak valid")
		return
	}

	if err := validateMenuBuilderPayload(payload); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "VALIDATION_ERROR",
			"message":    "Data tidak valid",
			"details":    []map[string]any{{"message": err.Error()}},
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	resolvedImageURL, resolvedImageThumbURL, resolvedImageThumbMeta, err := h.resolveMenuBuilderStockPhoto(ctx, payload.StockPhotoID, payload.ImageURL, payload.ImageThumbURL, payload.ImageThumbMeta)
	if err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	isActive := true
	if payload.IsActive != nil {
		isActive = *payload.IsActive
	}
	trackStock := payload.TrackStock != nil && *payload.TrackStock
	autoResetStock := payload.AutoResetStock != nil && *payload.AutoResetStock
	isSpicy := payload.IsSpicy != nil && *payload.IsSpicy
	isBestSeller := payload.IsBestSeller != nil && *payload.IsBestSeller
	isSignature := payload.IsSignature != nil && *payload.IsSignature
	isRecommended := payload.IsRecommended != nil && *payload.IsRecommended

	imageThumbMetaBytes := marshalJSONIfAny(resolvedImageThumbMeta)

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var menuID int64
	insertQuery := `
		insert into menus (
			merchant_id, name, description, price, image_url, image_thumb_url, image_thumb_meta,
			stock_photo_id, is_active, track_stock, stock_qty, daily_stock_template, auto_reset_stock,
			is_spicy, is_best_seller, is_signature, is_recommended, created_at, updated_at, created_by_user_id, updated_by_user_id
		) values (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, now(), now(), $18, $19
		)
		returning id
	`
	if err := tx.QueryRow(ctx, insertQuery,
		*authCtx.MerchantID,
		strings.TrimSpace(payload.Name),
		payload.Description,
		payload.Price,
		resolvedImageURL,
		resolvedImageThumbURL,
		imageThumbMetaBytes,
		payload.StockPhotoID,
		isActive,
		trackStock,
		payload.StockQty,
		payload.DailyStockTemplate,
		autoResetStock,
		isSpicy,
		isBestSeller,
		isSignature,
		isRecommended,
		authCtx.UserID,
		authCtx.UserID,
	).Scan(&menuID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	if payload.StockPhotoID != nil {
		if err := h.updateStockPhotoUsage(ctx, tx, *payload.StockPhotoID); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
			return
		}
	}

	if err := h.attachMenuBuilderCategories(ctx, tx, menuID, *authCtx.MerchantID, payload.CategoryIds); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	if err := h.attachMenuBuilderAddonCategories(ctx, tx, menuID, *authCtx.MerchantID, payload.AddonCategoryIds); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	menuSummary, err := h.fetchMenuBuilderSummary(ctx, menuID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data": map[string]any{
			"menu": menuSummary,
		},
		"message":    "Menu berhasil dibuat",
		"statusCode": http.StatusCreated,
	})
}

func (h *Handler) MerchantMenuBuilderUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Anda harus login terlebih dahulu")
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Data tidak valid")
		return
	}

	var raw map[string]any
	_ = json.Unmarshal(bodyBytes, &raw)

	var payload menuBuilderPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Data tidak valid")
		return
	}

	if payload.ID == nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "ID menu harus diisi")
		return
	}

	if err := validateMenuBuilderPayload(payload); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "VALIDATION_ERROR",
			"message":    "Data tidak valid",
			"details":    []map[string]any{{"message": err.Error()}},
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	menuID := *payload.ID
	var existingStockPhotoID pgtype.Int8
	if err := h.DB.QueryRow(ctx, `select stock_photo_id from menus where id = $1 and merchant_id = $2 and deleted_at is null`, menuID, *authCtx.MerchantID).Scan(&existingStockPhotoID); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    "Menu tidak ditemukan",
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	_, hasStockPhotoUpdate := raw["stockPhotoId"]
	resolvedImageURL, resolvedImageThumbURL, resolvedImageThumbMeta, err := h.resolveMenuBuilderStockPhoto(ctx, payload.StockPhotoID, payload.ImageURL, payload.ImageThumbURL, payload.ImageThumbMeta)
	if err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	isActive := true
	if payload.IsActive != nil {
		isActive = *payload.IsActive
	}
	trackStock := payload.TrackStock != nil && *payload.TrackStock
	autoResetStock := payload.AutoResetStock != nil && *payload.AutoResetStock
	isSpicy := payload.IsSpicy != nil && *payload.IsSpicy
	isBestSeller := payload.IsBestSeller != nil && *payload.IsBestSeller
	isSignature := payload.IsSignature != nil && *payload.IsSignature
	isRecommended := payload.IsRecommended != nil && *payload.IsRecommended

	imageThumbMetaBytes := marshalJSONIfAny(resolvedImageThumbMeta)

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	updateQuery := `
		update menus
		set name = $1,
			description = $2,
			price = $3,
			image_url = $4,
			image_thumb_url = $5,
			image_thumb_meta = $6,
			stock_photo_id = $7,
			is_active = $8,
			track_stock = $9,
			stock_qty = $10,
			daily_stock_template = $11,
			auto_reset_stock = $12,
			is_spicy = $13,
			is_best_seller = $14,
			is_signature = $15,
			is_recommended = $16,
			updated_at = now(),
			updated_by_user_id = $17
		where id = $18
	`

	stockPhotoValue := any(nil)
	if hasStockPhotoUpdate {
		stockPhotoValue = payload.StockPhotoID
	} else if existingStockPhotoID.Valid {
		stockPhotoValue = existingStockPhotoID.Int64
	}

	if _, err := tx.Exec(ctx, updateQuery,
		strings.TrimSpace(payload.Name),
		payload.Description,
		payload.Price,
		resolvedImageURL,
		resolvedImageThumbURL,
		imageThumbMetaBytes,
		stockPhotoValue,
		isActive,
		trackStock,
		payload.StockQty,
		payload.DailyStockTemplate,
		autoResetStock,
		isSpicy,
		isBestSeller,
		isSignature,
		isRecommended,
		authCtx.UserID,
		menuID,
	); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	if hasStockPhotoUpdate {
		if existingStockPhotoID.Valid {
			if err := h.updateStockPhotoUsage(ctx, tx, existingStockPhotoID.Int64); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
				return
			}
		}
		if payload.StockPhotoID != nil {
			if err := h.updateStockPhotoUsage(ctx, tx, *payload.StockPhotoID); err != nil {
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
				return
			}
		}
	}

	if err := h.attachMenuBuilderCategories(ctx, tx, menuID, *authCtx.MerchantID, payload.CategoryIds); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	if err := h.attachMenuBuilderAddonCategories(ctx, tx, menuID, *authCtx.MerchantID, payload.AddonCategoryIds); err != nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success":    false,
			"error":      "BUILDER_ERROR",
			"message":    err.Error(),
			"statusCode": http.StatusBadRequest,
		})
		return
	}

	if err := tx.Commit(ctx); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	menuSummary, err := h.fetchMenuBuilderSummary(ctx, menuID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Terjadi kesalahan internal")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"menu": menuSummary,
		},
		"message":    "Menu berhasil diupdate",
		"statusCode": http.StatusOK,
	})
}

func validateMenuBuilderPayload(payload menuBuilderPayload) error {
	if strings.TrimSpace(payload.Name) == "" {
		return fmt.Errorf("Nama menu harus diisi")
	}
	if payload.Price <= 0 {
		return fmt.Errorf("Harga harus lebih dari 0")
	}
	if payload.StockPhotoID != nil && *payload.StockPhotoID <= 0 {
		return fmt.Errorf("Foto stok tidak ditemukan")
	}
	if payload.StockQty != nil && *payload.StockQty < 0 {
		return fmt.Errorf("Stok tidak valid")
	}
	if payload.DailyStockTemplate != nil && *payload.DailyStockTemplate < 0 {
		return fmt.Errorf("Stok harian tidak valid")
	}
	for _, id := range payload.CategoryIds {
		if id <= 0 {
			return fmt.Errorf("Beberapa kategori tidak valid atau tidak ditemukan")
		}
	}
	for _, id := range payload.AddonCategoryIds {
		if id <= 0 {
			return fmt.Errorf("Beberapa kategori addon tidak valid atau tidak ditemukan")
		}
	}
	return nil
}

func (h *Handler) resolveMenuBuilderStockPhoto(ctx context.Context, stockPhotoID *int64, imageURL *string, imageThumbURL *string, imageThumbMeta any) (*string, *string, any, error) {
	resolvedImageURL := normalizeEmptyString(imageURL)
	resolvedImageThumbURL := normalizeEmptyString(imageThumbURL)
	resolvedImageThumbMeta := imageThumbMeta

	if stockPhotoID == nil {
		return resolvedImageURL, resolvedImageThumbURL, resolvedImageThumbMeta, nil
	}

	var stockImageURL pgtype.Text
	var stockThumbURL pgtype.Text
	var stockThumbMeta []byte
	var stockActive bool
	if err := h.DB.QueryRow(ctx, `
		select image_url, thumbnail_url, thumbnail_meta, is_active
		from stock_photos
		where id = $1
	`, *stockPhotoID).Scan(&stockImageURL, &stockThumbURL, &stockThumbMeta, &stockActive); err != nil || !stockActive {
		return nil, nil, nil, fmt.Errorf("Foto stok tidak ditemukan")
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

	return resolvedImageURL, resolvedImageThumbURL, resolvedImageThumbMeta, nil
}

func (h *Handler) updateStockPhotoUsage(ctx context.Context, tx pgx.Tx, stockPhotoID int64) error {
	var usageCount int
	if err := tx.QueryRow(ctx, `
		select count(*) from menus where stock_photo_id = $1 and deleted_at is null
	`, stockPhotoID).Scan(&usageCount); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `update stock_photos set usage_count = $1 where id = $2`, usageCount, stockPhotoID)
	return err
}

func (h *Handler) attachMenuBuilderCategories(ctx context.Context, tx pgx.Tx, menuID int64, merchantID int64, categoryIDs []int64) error {
	if _, err := tx.Exec(ctx, `delete from menu_category_items where menu_id = $1`, menuID); err != nil {
		return err
	}
	if len(categoryIDs) == 0 {
		return nil
	}
	var count int
	if err := tx.QueryRow(ctx, `
		select count(*) from menu_categories where id = any($1) and merchant_id = $2 and deleted_at is null
	`, categoryIDs, merchantID).Scan(&count); err != nil || count != len(categoryIDs) {
		return fmt.Errorf("Beberapa kategori tidak valid atau tidak ditemukan")
	}
	for _, categoryID := range categoryIDs {
		if _, err := tx.Exec(ctx, `
			insert into menu_category_items (menu_id, category_id, created_at)
			values ($1, $2, now())
		`, menuID, categoryID); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) attachMenuBuilderAddonCategories(ctx context.Context, tx pgx.Tx, menuID int64, merchantID int64, addonCategoryIDs []int64) error {
	if _, err := tx.Exec(ctx, `delete from menu_addon_categories where menu_id = $1`, menuID); err != nil {
		return err
	}
	if len(addonCategoryIDs) == 0 {
		return nil
	}
	var count int
	if err := tx.QueryRow(ctx, `
		select count(*) from addon_categories where id = any($1) and merchant_id = $2 and deleted_at is null
	`, addonCategoryIDs, merchantID).Scan(&count); err != nil || count != len(addonCategoryIDs) {
		return fmt.Errorf("Beberapa kategori addon tidak valid atau tidak ditemukan")
	}
	for idx, addonCategoryID := range addonCategoryIDs {
		if _, err := tx.Exec(ctx, `
			insert into menu_addon_categories (menu_id, addon_category_id, display_order, is_required, created_at, updated_at)
			values ($1, $2, $3, false, now(), now())
		`, menuID, addonCategoryID, idx); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) fetchMenuBuilderSummary(ctx context.Context, menuID int64) (map[string]any, error) {
	var (
		name  string
		price pgtype.Numeric
	)
	if err := h.DB.QueryRow(ctx, `select name, price from menus where id = $1`, menuID).Scan(&name, &price); err != nil {
		return nil, err
	}

	categories := make([]map[string]any, 0)
	catRows, err := h.DB.Query(ctx, `
		select mc.id, mc.name
		from menu_category_items mci
		join menu_categories mc on mc.id = mci.category_id
		where mci.menu_id = $1
		order by mc.id asc
	`, menuID)
	if err == nil {
		for catRows.Next() {
			var id int64
			var catName string
			if err := catRows.Scan(&id, &catName); err == nil {
				categories = append(categories, map[string]any{
					"id":   fmt.Sprint(id),
					"name": catName,
				})
			}
		}
		catRows.Close()
	}

	addonCategories := make([]map[string]any, 0)
	addonRows, err := h.DB.Query(ctx, `
		select ac.id, ac.name
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		where mac.menu_id = $1
		order by mac.display_order asc
	`, menuID)
	if err == nil {
		for addonRows.Next() {
			var id int64
			var addonName string
			if err := addonRows.Scan(&id, &addonName); err == nil {
				addonCategories = append(addonCategories, map[string]any{
					"id":   fmt.Sprint(id),
					"name": addonName,
				})
			}
		}
		addonRows.Close()
	}

	return map[string]any{
		"id":              fmt.Sprint(menuID),
		"name":            name,
		"price":           utils.NumericToFloat64(price),
		"categories":      categories,
		"addonCategories": addonCategories,
	}, nil
}

func normalizeEmptyString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func marshalJSONIfAny(value any) []byte {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return v
	default:
		if raw, err := json.Marshal(v); err == nil {
			return raw
		}
	}
	return nil
}
