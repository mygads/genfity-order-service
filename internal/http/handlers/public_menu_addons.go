package handlers

import (
	"net/http"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type addonCategoryResponse struct {
	ID            int64            `json:"id"`
	Name          string           `json:"name"`
	Description   *string          `json:"description"`
	Type          string           `json:"type"`
	MinSelections int32            `json:"minSelections"`
	MaxSelections *int32           `json:"maxSelections"`
	DisplayOrder  int32            `json:"displayOrder"`
	Addons        []map[string]any `json:"addons"`
}

func (h *Handler) PublicMerchantMenuAddons(w http.ResponseWriter, r *http.Request) {
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

	var menuExists bool
	if err := h.DB.QueryRow(ctx, `
		select exists(
			select 1 from menus
			where id = $1 and merchant_id = $2 and is_active = true and deleted_at is null
		)
	`, menuID, merchantID).Scan(&menuExists); err != nil || !menuExists {
		response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu not found or inactive")
		return
	}

	rows, err := h.DB.Query(ctx, `
		select
			mac.display_order,
			mac.is_required,
			ac.id, ac.name, ac.description, ac.min_selection, ac.max_selection,
			ai.id, ai.name, ai.description, ai.price, ai.input_type, ai.display_order, ai.track_stock, ai.stock_qty, ai.is_active
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		left join addon_items ai on ai.addon_category_id = ac.id and ai.is_active = true and ai.deleted_at is null
		where mac.menu_id = $1 and ac.is_active = true and ac.deleted_at is null
		order by mac.display_order asc, ai.display_order asc
	`, menuID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve addon categories")
		return
	}
	defer rows.Close()

	categories := make([]addonCategoryResponse, 0)
	for rows.Next() {
		var (
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

		if err := rows.Scan(
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

		var current *addonCategoryResponse
		for i := range categories {
			if categories[i].ID == catID {
				current = &categories[i]
				break
			}
		}
		if current == nil {
			cat := addonCategoryResponse{
				ID:            catID,
				Name:          catName,
				MinSelections: minSelection,
				DisplayOrder:  macDisplay,
				Type:          "optional",
				Addons:        make([]map[string]any, 0),
			}
			if macRequired {
				cat.Type = "required"
			}
			if catDescription.Valid {
				cat.Description = &catDescription.String
			}
			if maxSelection.Valid {
				value := maxSelection.Int32
				cat.MaxSelections = &value
			}
			categories = append(categories, cat)
			current = &categories[len(categories)-1]
		}

		if itemID.Valid {
			isAvailable := itemActive.Bool
			if itemTrackStock.Valid && itemTrackStock.Bool {
				isAvailable = itemStockQty.Valid && itemStockQty.Int32 > 0
			}
			addon := map[string]any{
				"id":          itemID.Int64,
				"categoryId":  catID,
				"name":        nullIfEmptyText(itemName),
				"description": nullIfEmptyText(itemDescription),
				"price":       utils.NumericToFloat64(itemPrice),
				"inputType":   nullIfEmptyText(itemInputType),
				"isAvailable": isAvailable,
				"trackStock":  itemTrackStock.Bool,
				"stockQty": func() any {
					if itemStockQty.Valid {
						return itemStockQty.Int32
					}
					return nil
				}(),
				"displayOrder": func() any {
					if itemDisplayOrder.Valid {
						return itemDisplayOrder.Int32
					}
					return nil
				}(),
			}
			current.Addons = append(current.Addons, addon)
		}
	}

	payload := make([]map[string]any, 0, len(categories))
	for _, cat := range categories {
		payload = append(payload, map[string]any{
			"id":            cat.ID,
			"name":          cat.Name,
			"description":   cat.Description,
			"type":          cat.Type,
			"minSelections": cat.MinSelections,
			"maxSelections": cat.MaxSelections,
			"displayOrder":  cat.DisplayOrder,
			"addons":        cat.Addons,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    payload,
		"message": "Addon categories retrieved successfully",
	})
}
