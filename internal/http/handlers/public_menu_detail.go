package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicMerchantMenuDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	menuID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu id")
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

	var (
		menuName           string
		menuDescription    pgtype.Text
		menuPrice          pgtype.Numeric
		imageURL           pgtype.Text
		imageThumbURL      pgtype.Text
		imageThumbMeta     []byte
		stockQty           pgtype.Int4
		isActive           bool
		trackStock         bool
		dailyStockTemplate pgtype.Int4
		createdAt          time.Time
		updatedAt          time.Time
	)

	err = h.DB.QueryRow(ctx, `
		select name, description, price, image_url, image_thumb_url, image_thumb_meta,
		       stock_qty, is_active, track_stock, daily_stock_template, created_at, updated_at
		from menus
		where id = $1 and merchant_id = $2 and is_active = true and deleted_at is null
	`, menuID, merchantID).Scan(
		&menuName,
		&menuDescription,
		&menuPrice,
		&imageURL,
		&imageThumbURL,
		&imageThumbMeta,
		&stockQty,
		&isActive,
		&trackStock,
		&dailyStockTemplate,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu item not found")
		return
	}

	promoPrice := (*float64)(nil)
	var promo pgtype.Numeric
	if err := h.DB.QueryRow(ctx, `
		select spi.promo_price
		from special_price_items spi
		join special_prices sp on sp.id = spi.special_price_id
		where spi.menu_id = $1
		  and sp.is_active = true
		  and sp.start_date <= current_date
		  and sp.end_date >= current_date
		order by sp.start_date desc
		limit 1
	`, menuID).Scan(&promo); err == nil {
		value := utils.NumericToFloat64(promo)
		promoPrice = &value
	}

	payload := map[string]any{
		"id":             menuID,
		"name":           menuName,
		"description":    nullIfEmptyText(menuDescription),
		"price":          utils.NumericToFloat64(menuPrice),
		"imageUrl":       nullIfEmptyText(imageURL),
		"imageThumbUrl":  nullIfEmptyText(imageThumbURL),
		"imageThumbMeta": decodeJSONMeta(imageThumbMeta),
		"stockQty": func() any {
			if stockQty.Valid {
				return stockQty.Int32
			}
			return nil
		}(),
		"isActive":   isActive,
		"trackStock": trackStock,
		"dailyStockTemplate": func() any {
			if dailyStockTemplate.Valid {
				return dailyStockTemplate.Int32
			}
			return nil
		}(),
		"createdAt":  createdAt,
		"updatedAt":  updatedAt,
		"isPromo":    promoPrice != nil,
		"promoPrice": promoPrice,
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":   true,
		"data":      payload,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func decodeJSONMeta(data []byte) any {
	if len(data) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return out
}
