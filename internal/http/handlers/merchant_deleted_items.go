package handlers

import (
	"context"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

const deletedItemsRetentionDays = 30

type deletedItemRow struct {
	ID        int64
	Name      string
	DeletedAt time.Time
	ImageURL  pgtype.Text
	Details   pgtype.Text
}

func (h *Handler) MerchantDeletedItemsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Merchant ID required")
		return
	}

	queryType := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
	if queryType == "" {
		queryType = "all"
	}

	data := map[string]any{}
	summary := map[string]int64{
		"menus":           0,
		"categories":      0,
		"addonCategories": 0,
		"addonItems":      0,
		"total":           0,
	}

	if queryType == "all" || queryType == "menus" {
		items, err := h.fetchDeletedMenus(ctx, *authCtx.MerchantID)
		if err != nil {
			h.Logger.Error("deleted menus fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch deleted items")
			return
		}
		data["menus"] = items
		summary["menus"] = int64(len(items))
	}

	if queryType == "all" || queryType == "categories" {
		items, err := h.fetchDeletedCategories(ctx, *authCtx.MerchantID)
		if err != nil {
			h.Logger.Error("deleted categories fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch deleted items")
			return
		}
		data["categories"] = items
		summary["categories"] = int64(len(items))
	}

	if queryType == "all" || queryType == "addon-categories" {
		items, err := h.fetchDeletedAddonCategories(ctx, *authCtx.MerchantID)
		if err != nil {
			h.Logger.Error("deleted addon categories fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch deleted items")
			return
		}
		data["addonCategories"] = items
		summary["addonCategories"] = int64(len(items))
	}

	if queryType == "all" || queryType == "addon-items" {
		items, err := h.fetchDeletedAddonItems(ctx, *authCtx.MerchantID)
		if err != nil {
			h.Logger.Error("deleted addon items fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch deleted items")
			return
		}
		data["addonItems"] = items
		summary["addonItems"] = int64(len(items))
	}

	summary["total"] = summary["menus"] + summary["categories"] + summary["addonCategories"] + summary["addonItems"]

	data["summary"] = summary
	data["retentionPolicy"] = map[string]any{
		"days":    deletedItemsRetentionDays,
		"message": "Items are permanently deleted after 30 days",
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    data,
	})
}

func (h *Handler) fetchDeletedMenus(ctx context.Context, merchantID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select id, name, deleted_at, image_url, description
        from menus
        where merchant_id = $1 and deleted_at is not null
        order by deleted_at desc
    `, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var row deletedItemRow
		if err := rows.Scan(&row.ID, &row.Name, &row.DeletedAt, &row.ImageURL, &row.Details); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":                       row.ID,
			"name":                     row.Name,
			"deletedAt":                row.DeletedAt,
			"daysUntilPermanentDelete": calculateDaysUntilPermanentDelete(row.DeletedAt),
			"type":                     "menus",
			"imageUrl":                 nullableText(row.ImageURL),
			"description":              nullableText(row.Details),
		})
	}
	return items, nil
}

func (h *Handler) fetchDeletedCategories(ctx context.Context, merchantID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select id, name, deleted_at
        from menu_categories
        where merchant_id = $1 and deleted_at is not null
        order by deleted_at desc
    `, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var name string
		var deletedAt time.Time
		if err := rows.Scan(&id, &name, &deletedAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":                       id,
			"name":                     name,
			"deletedAt":                deletedAt,
			"daysUntilPermanentDelete": calculateDaysUntilPermanentDelete(deletedAt),
			"type":                     "categories",
		})
	}
	return items, nil
}

func (h *Handler) fetchDeletedAddonCategories(ctx context.Context, merchantID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select id, name, deleted_at
        from addon_categories
        where merchant_id = $1 and deleted_at is not null
        order by deleted_at desc
    `, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var name string
		var deletedAt time.Time
		if err := rows.Scan(&id, &name, &deletedAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":                       id,
			"name":                     name,
			"deletedAt":                deletedAt,
			"daysUntilPermanentDelete": calculateDaysUntilPermanentDelete(deletedAt),
			"type":                     "addon-categories",
		})
	}
	return items, nil
}

func (h *Handler) fetchDeletedAddonItems(ctx context.Context, merchantID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select ai.id, ai.name, ai.deleted_at
        from addon_items ai
        join addon_categories ac on ac.id = ai.addon_category_id
        where ac.merchant_id = $1 and ai.deleted_at is not null
        order by ai.deleted_at desc
    `, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var name string
		var deletedAt time.Time
		if err := rows.Scan(&id, &name, &deletedAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":                       id,
			"name":                     name,
			"deletedAt":                deletedAt,
			"daysUntilPermanentDelete": calculateDaysUntilPermanentDelete(deletedAt),
			"type":                     "addon-items",
		})
	}
	return items, nil
}

func calculateDaysUntilPermanentDelete(deletedAt time.Time) int {
	daysSince := int(time.Since(deletedAt).Hours() / 24)
	remaining := deletedItemsRetentionDays - daysSince
	if remaining < 0 {
		return 0
	}
	return remaining
}
