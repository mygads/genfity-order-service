package handlers

import (
	"net/http"
	"strconv"
	"strings"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicMerchantRecommendations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	menuIdsParam := strings.TrimSpace(r.URL.Query().Get("menuIds"))
	if menuIdsParam == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "menuIds parameter is required")
		return
	}

	menuIDs := make([]int64, 0)
	for _, raw := range strings.Split(menuIdsParam, ",") {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		id, ok := parseNumericID(value)
		if !ok || id <= 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "menuIds must be a comma-separated list of numeric IDs")
			return
		}
		menuIDs = append(menuIDs, id)
	}

	if len(menuIDs) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    []any{},
		})
		return
	}

	var merchantID int64
	if err := h.DB.QueryRow(ctx, `select id from merchants where code = $1`, merchantCode).Scan(&merchantID); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select oi2.menu_id, oi2.menu_name, m.image_url, m.price, count(distinct oi2.order_id) as frequency
		from order_items oi1
		join order_items oi2 on oi1.order_id = oi2.order_id
		join orders o on oi1.order_id = o.id
		join menus m on oi2.menu_id = m.id
		where o.merchant_id = $1
		  and o.status in ('ACCEPTED', 'COMPLETED', 'READY')
		  and oi1.menu_id = any($2)
		  and not (oi2.menu_id = any($2))
		  and m.is_active = true
		  and m.deleted_at is null
		  and (m.track_stock = false or m.stock_qty is null or m.stock_qty > 0)
		group by oi2.menu_id, oi2.menu_name, m.image_url, m.price
		order by frequency desc
		limit 5
	`, merchantID, menuIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch recommendations")
		return
	}
	defer rows.Close()

	results := make([]map[string]any, 0)
	for rows.Next() {
		var (
			menuID    int64
			menuName  string
			imageURL  pgtype.Text
			menuPrice pgtype.Numeric
			frequency int64
		)
		if err := rows.Scan(&menuID, &menuName, &imageURL, &menuPrice, &frequency); err != nil {
			continue
		}
		item := map[string]any{
			"id":        strconv.FormatInt(menuID, 10),
			"name":      menuName,
			"price":     utils.NumericToFloat64(menuPrice),
			"imageUrl":  nullIfEmptyText(imageURL),
			"frequency": frequency,
		}
		results = append(results, item)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    results,
		"meta": map[string]any{
			"source":        "co-purchase",
			"cartItemCount": len(menuIDs),
		},
	})
}
