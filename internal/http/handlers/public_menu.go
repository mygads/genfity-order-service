package handlers

import (
	"encoding/json"
	"net/http"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type publicMenuCategory struct {
	ID          int64
	Name        string
	Description *string
	SortOrder   int32
}

type publicMenuItem struct {
	ID             int64
	Name           string
	Description    *string
	Price          float64
	ImageURL       *string
	ImageThumbURL  *string
	ImageThumbMeta any
	IsActive       bool
	IsSpicy        bool
	IsBestSeller   bool
	IsSignature    bool
	IsRecommended  bool
	TrackStock     bool
	StockQty       *int32
}

type publicAddonItem struct {
	ID           int64
	Name         string
	Description  *string
	Price        float64
	InputType    string
	DisplayOrder int32
	TrackStock   bool
	StockQty     *int32
}

type publicAddonCategory struct {
	ID           int64
	Name         string
	Description  *string
	MinSelection int32
	MaxSelection *int32
	IsRequired   bool
	DisplayOrder int32
	AddonItems   []publicAddonItem
}

func (h *Handler) PublicMenu(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "merchantCode")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var (
		merchantID          int64
		merchantName        string
		merchantDescription pgtype.Text
		merchantLogo        pgtype.Text
		merchantActive      bool
		merchantOpen        bool
	)

	if err := h.DB.QueryRow(ctx, `
		select id, name, description, logo_url, is_active, is_open
		from merchants
		where code = $1
	`, merchantCode).Scan(&merchantID, &merchantName, &merchantDescription, &merchantLogo, &merchantActive, &merchantOpen); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}

	if !merchantActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}

	// Categories
	categories := make([]publicMenuCategory, 0)
	catRows, err := h.DB.Query(ctx, `
		select id, name, description, sort_order
		from menu_categories
		where merchant_id = $1 and is_active = true and deleted_at is null
		order by sort_order asc
	`, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
		return
	}
	for catRows.Next() {
		var (
			c            publicMenuCategory
			cDescription pgtype.Text
		)
		if err := catRows.Scan(&c.ID, &c.Name, &cDescription, &c.SortOrder); err != nil {
			catRows.Close()
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
			return
		}
		if cDescription.Valid {
			c.Description = &cDescription.String
		}
		categories = append(categories, c)
	}
	catRows.Close()

	// Menus
	menus := make([]publicMenuItem, 0)
	menuIDs := make([]int64, 0)
	menuRows, err := h.DB.Query(ctx, `
		select id, name, description, price, image_url, image_thumb_url, image_thumb_meta,
		       is_active, is_spicy, is_best_seller, is_signature, is_recommended, track_stock, stock_qty
		from menus
		where merchant_id = $1 and is_active = true and deleted_at is null
	`, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
		return
	}
	for menuRows.Next() {
		var (
			m              publicMenuItem
			description    pgtype.Text
			price          pgtype.Numeric
			imageURL       pgtype.Text
			imageThumbURL  pgtype.Text
			imageThumbMeta []byte
			stockQty       pgtype.Int4
		)
		if err := menuRows.Scan(
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
			menuRows.Close()
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
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
	menuRows.Close()

	if len(menus) == 0 || len(categories) == 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data": map[string]any{
				"merchant": map[string]any{
					"code":        merchantCode,
					"name":        merchantName,
					"description": valueOrNil(merchantDescription),
					"logoUrl":     valueOrNil(merchantLogo),
					"isOpen":      merchantOpen,
				},
				"menusByCategory": []any{},
			},
			"message":    "Menu retrieved successfully",
			"statusCode": 200,
		})
		return
	}

	// Menu-category mapping
	menuCategories := make(map[int64][]int64)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select menu_id, category_id
			from menu_category_items
			where menu_id = any($1)
		`, menuIDs)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
			return
		}
		for rows.Next() {
			var menuID int64
			var categoryID int64
			if err := rows.Scan(&menuID, &categoryID); err == nil {
				menuCategories[menuID] = append(menuCategories[menuID], categoryID)
			}
		}
		rows.Close()
	}

	// Addon categories per menu
	addonMap := make(map[int64][]publicAddonCategory)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
			select
				mac.menu_id,
				mac.display_order,
				mac.is_required,
				ac.id, ac.name, ac.description, ac.min_selection, ac.max_selection,
				ai.id, ai.name, ai.description, ai.price, ai.input_type, ai.display_order, ai.track_stock, ai.stock_qty
			from menu_addon_categories mac
			join addon_categories ac on ac.id = mac.addon_category_id
			left join addon_items ai on ai.addon_category_id = ac.id and ai.is_active = true and ai.deleted_at is null
			where mac.menu_id = any($1) and ac.is_active = true and ac.deleted_at is null
			order by mac.display_order asc, ai.display_order asc
		`, menuIDs)
		if err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu")
			return
		}
		for rows.Next() {
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
			)
			if err := rows.Scan(
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

			if itemID.Valid {
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
		rows.Close()
	}

	// Promo prices
	promoMap := make(map[int64]float64)
	if len(menuIDs) > 0 {
		rows, err := h.DB.Query(ctx, `
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
			for rows.Next() {
				var menuID int64
				var promo pgtype.Numeric
				if err := rows.Scan(&menuID, &promo); err == nil {
					promoMap[menuID] = utils.NumericToFloat64(promo)
				}
			}
			rows.Close()
		}
	}

	menuMap := make(map[int64]publicMenuItem)
	for _, m := range menus {
		menuMap[m.ID] = m
	}

	menusByCategory := make([]map[string]any, 0, len(categories))
	for _, category := range categories {
		menuPayloads := make([]map[string]any, 0)
		for _, menuID := range menuIDs {
			catIDs := menuCategories[menuID]
			matched := false
			for _, catID := range catIDs {
				if catID == category.ID {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			menu := menuMap[menuID]
			promo, promoOk := promoMap[menuID]
			payload := map[string]any{
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
				"trackStock": menu.TrackStock,
				"stockQty":   menu.StockQty,
				"addonCategories": func() []map[string]any {
					addons := addonMap[menuID]
					result := make([]map[string]any, 0, len(addons))
					for _, cat := range addons {
						addonItems := make([]map[string]any, 0, len(cat.AddonItems))
						for _, item := range cat.AddonItems {
							addonItems = append(addonItems, map[string]any{
								"id":           item.ID,
								"name":         item.Name,
								"description":  item.Description,
								"price":        item.Price,
								"inputType":    item.InputType,
								"displayOrder": item.DisplayOrder,
								"trackStock":   item.TrackStock,
								"stockQty":     item.StockQty,
							})
						}
						result = append(result, map[string]any{
							"id":           cat.ID,
							"name":         cat.Name,
							"description":  cat.Description,
							"minSelection": cat.MinSelection,
							"maxSelection": cat.MaxSelection,
							"isRequired":   cat.IsRequired,
							"displayOrder": cat.DisplayOrder,
							"addonItems":   addonItems,
						})
					}
					return result
				}(),
			}
			menuPayloads = append(menuPayloads, payload)
		}

		menusByCategory = append(menusByCategory, map[string]any{
			"category": map[string]any{
				"id":          category.ID,
				"name":        category.Name,
				"description": category.Description,
				"sortOrder":   category.SortOrder,
			},
			"menus": menuPayloads,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchant": map[string]any{
				"code":        merchantCode,
				"name":        merchantName,
				"description": valueOrNil(merchantDescription),
				"logoUrl":     valueOrNil(merchantLogo),
				"isOpen":      merchantOpen,
			},
			"menusByCategory": menusByCategory,
		},
		"message":    "Menu retrieved successfully",
		"statusCode": 200,
	})
}
