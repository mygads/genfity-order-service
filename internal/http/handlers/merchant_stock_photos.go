package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type stockPhotoRow struct {
	ID            int64
	Category      string
	Name          string
	ImageURL      string
	ThumbnailURL  pgtype.Text
	ThumbnailMeta []byte
}

func (h *Handler) MerchantStockPhotosList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, ok := middleware.GetAuthContext(ctx)
	if !ok {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Unauthorized")
		return
	}

	query := r.URL.Query()
	category := strings.TrimSpace(query.Get("category"))
	search := strings.TrimSpace(query.Get("search"))
	page := parseQueryIntValue(query.Get("page"), 1)
	limit := parseQueryIntValue(query.Get("limit"), 24)
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 24
	}
	offset := (page - 1) * limit

	where := []string{"is_active = true"}
	args := make([]any, 0)

	if category != "" {
		args = append(args, category)
		where = append(where, "category = $"+intToString(len(args)))
	}

	if search != "" {
		args = append(args, "%"+search+"%")
		where = append(where, "(name ilike $"+intToString(len(args))+" or category ilike $"+intToString(len(args))+")")
	}

	whereClause := strings.Join(where, " and ")

	var total int64
	countQuery := "select count(*) from stock_photos where " + whereClause
	if err := h.DB.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		h.Logger.Error("stock photos count failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock photos")
		return
	}

	listArgs := append([]any{}, args...)
	listArgs = append(listArgs, limit, offset)
	listQuery := "select id, category, name, image_url, thumbnail_url, thumbnail_meta from stock_photos where " + whereClause +
		" order by category asc, usage_count desc, created_at desc, id asc limit $" + intToString(len(listArgs)-1) + " offset $" + intToString(len(listArgs))

	rows, err := h.DB.Query(ctx, listQuery, listArgs...)
	if err != nil {
		h.Logger.Error("stock photos query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock photos")
		return
	}
	defer rows.Close()

	photos := make([]map[string]any, 0)
	for rows.Next() {
		var row stockPhotoRow
		if err := rows.Scan(&row.ID, &row.Category, &row.Name, &row.ImageURL, &row.ThumbnailURL, &row.ThumbnailMeta); err != nil {
			h.Logger.Error("stock photos scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock photos")
			return
		}
		photos = append(photos, map[string]any{
			"id":            row.ID,
			"category":      row.Category,
			"name":          row.Name,
			"imageUrl":      row.ImageURL,
			"thumbnailUrl":  nullableText(row.ThumbnailURL),
			"thumbnailMeta": decodeJSONBMeta(row.ThumbnailMeta),
		})
	}

	categoryRows, err := h.DB.Query(ctx, `
        select category, count(*)
        from stock_photos
        where is_active = true
        group by category
        order by category asc
    `)
	if err != nil {
		h.Logger.Error("stock photo categories query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock photos")
		return
	}
	defer categoryRows.Close()

	categories := make([]map[string]any, 0)
	for categoryRows.Next() {
		var name string
		var count int64
		if err := categoryRows.Scan(&name, &count); err != nil {
			h.Logger.Error("stock photo categories scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve stock photos")
			return
		}
		categories = append(categories, map[string]any{"name": name, "count": count})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + int64(limit) - 1) / int64(limit)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"photos":     photos,
			"categories": categories,
			"pagination": map[string]any{
				"page":       page,
				"limit":      limit,
				"total":      total,
				"totalPages": totalPages,
			},
		},
		"message":    "Stock photos retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantStockPhotoUse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, ok := middleware.GetAuthContext(ctx)
	if !ok {
		response.Error(w, http.StatusUnauthorized, "UNAUTHORIZED", "Unauthorized")
		return
	}

	photoID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Photo id is required")
		return
	}

	var exists bool
	if err := h.DB.QueryRow(ctx, "select exists(select 1 from stock_photos where id = $1 and is_active = true)", photoID).Scan(&exists); err != nil {
		h.Logger.Error("stock photo lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to track usage")
		return
	}
	if !exists {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Stock photo not found")
		return
	}

	var usageCount int64
	if err := h.DB.QueryRow(ctx, "select count(*) from menus where stock_photo_id = $1 and deleted_at is null", photoID).Scan(&usageCount); err != nil {
		h.Logger.Error("stock photo usage count failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to track usage")
		return
	}

	if _, err := h.DB.Exec(ctx, "update stock_photos set usage_count = $1, updated_at = now() where id = $2", usageCount, photoID); err != nil {
		h.Logger.Error("stock photo usage update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to track usage")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Usage tracked successfully",
		"statusCode": 200,
	})
}

func parseQueryIntValue(value string, fallback int) int {
	parsed, err := parseStringToInt(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func decodeJSONBMeta(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(value, &out); err != nil {
		return nil
	}
	return out
}
