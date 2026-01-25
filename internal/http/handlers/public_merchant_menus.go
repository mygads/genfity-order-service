package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) PublicMerchantMenus(w http.ResponseWriter, r *http.Request) {
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

	categoryParam := strings.TrimSpace(r.URL.Query().Get("category"))
	var (
		categoryID    int64
		hasCategory   bool
		uncategorized bool
	)
	if categoryParam != "" {
		if categoryParam == "uncategorized" {
			uncategorized = true
		} else {
			parsed, err := strconv.ParseInt(categoryParam, 10, 64)
			if err != nil || parsed <= 0 {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category")
				return
			}
			categoryID = parsed
			hasCategory = true
		}
	}

	menuQuery := `
		select m.id, m.name, m.description, m.price, m.image_url, m.image_thumb_url, m.image_thumb_meta,
		       m.is_active, m.is_spicy, m.is_best_seller, m.is_signature, m.is_recommended,
		       m.track_stock, m.stock_qty
		from menus m
		where m.merchant_id = $1 and m.is_active = true and m.deleted_at is null`
	args := []any{merchantID}
	if uncategorized {
		menuQuery += ` and not exists (select 1 from menu_category_items mci where mci.menu_id = m.id)`
	} else if hasCategory {
		menuQuery += ` and exists (select 1 from menu_category_items mci where mci.menu_id = m.id and mci.category_id = $2)`
		args = append(args, categoryID)
	}

	rows, err := h.DB.Query(ctx, menuQuery, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
		return
	}
	defer rows.Close()

	menus := make([]publicMenuItem, 0)
	menuIDs := make([]int64, 0)
	for rows.Next() {
		var (
			m              publicMenuItem
			description    pgtype.Text
			price          pgtype.Numeric
			imageURL       pgtype.Text
			imageThumbURL  pgtype.Text
			imageThumbMeta []byte
			stockQty       pgtype.Int4
		)
		if err := rows.Scan(
			&m.ID,
			&m.Name,
			&description,
			&price,
			&imageURL,
			&imageThumbURL,
			&imageThumbMeta,
			&m.IsActive,
			&m.IsSpicy,
			&m.IsBestSeller,
			&m.IsSignature,
			&m.IsRecommended,
			&m.TrackStock,
			&stockQty,
		); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
			return
		}
		m.Price = utils.NumericToFloat64(price)
		if description.Valid {
			m.Description = &description.String
		}
		if imageURL.Valid {
			m.ImageURL = &imageURL.String
		}
		if imageThumbURL.Valid {
			m.ImageThumbURL = &imageThumbURL.String
		}
		if len(imageThumbMeta) > 0 {
			var meta any
			if err := json.Unmarshal(imageThumbMeta, &meta); err == nil {
				m.ImageThumbMeta = meta
			}
		}
		if stockQty.Valid {
			value := stockQty.Int32
			m.StockQty = &value
		}
		menus = append(menus, m)
		menuIDs = append(menuIDs, m.ID)
	}

	if len(menuIDs) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       []any{},
			"message":    "Menus retrieved successfully",
			"statusCode": 200,
		})
		return
	}

	// Categories for each menu
	menuCategories := make(map[int64][]map[string]any)
	catRows, err := h.DB.Query(ctx, `
		select mci.menu_id, c.id, c.name
		from menu_category_items mci
		join menu_categories c on c.id = mci.category_id
		where mci.menu_id = any($1)
	`, menuIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
		return
	}
	for catRows.Next() {
		var menuID int64
		var catID int64
		var catName string
		if err := catRows.Scan(&menuID, &catID, &catName); err == nil {
			menuCategories[menuID] = append(menuCategories[menuID], map[string]any{
				"id":   catID,
				"name": catName,
			})
		}
	}
	catRows.Close()

	// Addon categories per menu
	addonMap := make(map[int64][]publicAddonCategory)
	addonRows, err := h.DB.Query(ctx, `
		select
			mac.menu_id,
			mac.display_order,
			mac.is_required,
			ac.id, ac.name, ac.description, ac.min_selection, ac.max_selection,
			ai.id, ai.name, ai.description, ai.price, ai.input_type, ai.display_order, ai.track_stock, ai.stock_qty, ai.is_active
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		left join addon_items ai on ai.addon_category_id = ac.id and ai.is_active = true and ai.deleted_at is null
		where mac.menu_id = any($1) and ac.is_active = true and ac.deleted_at is null
		order by mac.display_order asc, ai.display_order asc
	`, menuIDs)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menus")
		return
	}
	for addonRows.Next() {
		var (
			menuID           int64
			macDisplay       int32
			macRequired      bool
			catID            int64
			catName          string
			catDescription   pgtype.Text
			minSelection     int32
			maxSelection     pgtype.Int4
			itemID           pgtype.Int8
			itemName         pgtype.Text
			itemDescription  pgtype.Text
			itemPrice        pgtype.Numeric
			itemInputType    pgtype.Text
			itemDisplayOrder pgtype.Int4
			itemTrackStock   pgtype.Bool
			itemStockQty     pgtype.Int4
			itemActive       pgtype.Bool
		)
		if err := addonRows.Scan(
			&menuID,
			&macDisplay,
			&macRequired,
			&catID,
			&catName,
			&catDescription,
			&minSelection,
			&maxSelection,
			&itemID,
			&itemName,
			&itemDescription,
			&itemPrice,
			&itemInputType,
			&itemDisplayOrder,
			&itemTrackStock,
			&itemStockQty,
			&itemActive,
		); err != nil {
			continue
		}

		categories := addonMap[menuID]
		var current *publicAddonCategory
		for i := range categories {
			if categories[i].ID == catID {
				current = &categories[i]
				break
			}
		}
		if current == nil {
			cat := publicAddonCategory{
				ID:           catID,
				Name:         catName,
				MinSelection: minSelection,
				IsRequired:   macRequired,
				DisplayOrder: macDisplay,
				AddonItems:   []publicAddonItem{},
			}
			if catDescription.Valid {
				cat.Description = &catDescription.String
			}
			if maxSelection.Valid {
				value := maxSelection.Int32
				cat.MaxSelection = &value
			}
			categories = append(categories, cat)
			current = &categories[len(categories)-1]
		}

		if itemID.Valid && (!itemActive.Valid || itemActive.Bool) {
			addon := publicAddonItem{
				ID:           itemID.Int64,
				Name:         itemName.String,
				Price:        utils.NumericToFloat64(itemPrice),
				InputType:    itemInputType.String,
				DisplayOrder: itemDisplayOrder.Int32,
				TrackStock:   itemTrackStock.Bool,
			}
			if itemDescription.Valid {
				addon.Description = &itemDescription.String
			}
			if itemStockQty.Valid {
				value := itemStockQty.Int32
				addon.StockQty = &value
			}
			current.AddonItems = append(current.AddonItems, addon)
		}
		addonMap[menuID] = categories
	}
	addonRows.Close()

	// Promo prices
	promoMap := make(map[int64]float64)
	if len(menuIDs) > 0 {
		promoRows, err := h.DB.Query(ctx, `
			select spi.menu_id, spi.promo_price
			from special_price_items spi
			join special_prices sp on sp.id = spi.special_price_id
			where spi.menu_id = any($1)
			  and sp.merchant_id = $2
			  and sp.is_active = true
			  and sp.start_date <= current_date
			  and sp.end_date >= current_date
		`, menuIDs, merchantID)
		if err == nil {
			for promoRows.Next() {
				var menuID int64
				var promo pgtype.Numeric
				if err := promoRows.Scan(&menuID, &promo); err == nil {
					promoMap[menuID] = utils.NumericToFloat64(promo)
				}
			}
			promoRows.Close()
		}
	}

	// Order counts (last 90 days)
	orderCount := make(map[int64]int64)
	if len(menuIDs) > 0 {
		ninetyDaysAgo := time.Now().AddDate(0, 0, -90)
		countRows, err := h.DB.Query(ctx, `
			select oi.menu_id, coalesce(sum(oi.quantity), 0)
			from order_items oi
			join orders o on o.id = oi.order_id
			where oi.menu_id = any($1)
			  and o.merchant_id = $2
			  and o.status in ('COMPLETED', 'READY', 'IN_PROGRESS', 'ACCEPTED')
			  and o.created_at >= $3
			group by oi.menu_id
		`, menuIDs, merchantID, ninetyDaysAgo)
		if err == nil {
			for countRows.Next() {
				var menuID int64
				var total int64
				if err := countRows.Scan(&menuID, &total); err == nil {
					orderCount[menuID] = total
				}
			}
			countRows.Close()
		}
	}

	formatted := make([]map[string]any, 0, len(menus))
	for _, menu := range menus {
		promo, promoOk := promoMap[menu.ID]
		addonCats := addonMap[menu.ID]
		addonPayloads := make([]map[string]any, 0, len(addonCats))
		for _, cat := range addonCats {
			items := make([]map[string]any, 0, len(cat.AddonItems))
			for _, item := range cat.AddonItems {
				items = append(items, map[string]any{
					"id":           item.ID,
					"name":         item.Name,
					"description":  item.Description,
					"price":        item.Price,
					"inputType":    item.InputType,
					"displayOrder": item.DisplayOrder,
					"trackStock":   item.TrackStock,
					"stockQty":     item.StockQty,
					"isActive":     true,
				})
			}
			addonPayloads = append(addonPayloads, map[string]any{
				"id":           cat.ID,
				"name":         cat.Name,
				"description":  cat.Description,
				"minSelection": cat.MinSelection,
				"maxSelection": cat.MaxSelection,
				"isRequired":   cat.IsRequired,
				"displayOrder": cat.DisplayOrder,
				"addonItems":   items,
			})
		}
		formatted = append(formatted, map[string]any{
			"id":             menu.ID,
			"name":           menu.Name,
			"description":    menu.Description,
			"price":          menu.Price,
			"imageUrl":       menu.ImageURL,
			"imageThumbUrl":  menu.ImageThumbURL,
			"imageThumbMeta": menu.ImageThumbMeta,
			"isActive":       menu.IsActive,
			"isPromo":        promoOk,
			"isSpicy":        menu.IsSpicy,
			"isBestSeller":   menu.IsBestSeller,
			"isSignature":    menu.IsSignature,
			"isRecommended":  menu.IsRecommended,
			"promoPrice": func() any {
				if promoOk {
					return promo
				}
				return nil
			}(),
			"orderCount":      orderCount[menu.ID],
			"trackStock":      menu.TrackStock,
			"stockQty":        menu.StockQty,
			"categories":      menuCategories[menu.ID],
			"addonCategories": addonPayloads,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       formatted,
		"message":    "Menus retrieved successfully",
		"statusCode": 200,
	})
}
