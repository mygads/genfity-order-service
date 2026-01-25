package handlers

import (
	"net/http"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type publicMerchantCategory struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
	SortOrder   int32   `json:"sortOrder"`
}

func (h *Handler) PublicMerchantCategories(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var merchantID int64
	var merchantActive bool
	if err := h.DB.QueryRow(ctx, `select id, is_active from merchants where code = $1`, merchantCode).Scan(&merchantID, &merchantActive); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}
	if !merchantActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}

	categories := make([]publicMerchantCategory, 0)
	rows, err := h.DB.Query(ctx, `
		select id, name, description, sort_order
		from menu_categories
		where merchant_id = $1 and is_active = true and deleted_at is null
		order by sort_order asc
	`, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve categories")
		return
	}
	for rows.Next() {
		var c publicMerchantCategory
		var desc pgtype.Text
		if err := rows.Scan(&c.ID, &c.Name, &desc, &c.SortOrder); err == nil {
			if desc.Valid {
				c.Description = &desc.String
			}
			categories = append(categories, c)
		}
	}
	rows.Close()

	// Check for uncategorized menus
	var uncategorizedCount int64
	_ = h.DB.QueryRow(ctx, `
		select count(*)
		from menus m
		where m.merchant_id = $1
		  and m.is_active = true
		  and m.deleted_at is null
		  and not exists (select 1 from menu_category_items mci where mci.menu_id = m.id)
	`, merchantID).Scan(&uncategorizedCount)

	final := make([]map[string]any, 0, len(categories)+1)
	maxSortOrder := int32(0)
	for _, c := range categories {
		if c.SortOrder > maxSortOrder {
			maxSortOrder = c.SortOrder
		}
		final = append(final, map[string]any{
			"id":          c.ID,
			"name":        c.Name,
			"description": c.Description,
			"sortOrder":   c.SortOrder,
		})
	}

	if uncategorizedCount > 0 {
		final = append(final, map[string]any{
			"id":          "uncategorized",
			"name":        "All Menu",
			"description": "Other menu items",
			"sortOrder":   maxSortOrder + 1,
			"isVirtual":   true,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    final,
		"message": "Categories retrieved successfully",
	})
}
