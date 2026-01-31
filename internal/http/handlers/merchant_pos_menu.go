package handlers

import (
	"context"
	"net/http"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type posMenuAddonItem struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	Price      float64 `json:"price"`
	IsActive   bool    `json:"isActive"`
	TrackStock bool    `json:"trackStock"`
	StockQty   *int32  `json:"stockQty"`
}

type posMenuAddonCategory struct {
	ID           int64              `json:"id"`
	Name         string             `json:"name"`
	Description  *string            `json:"description"`
	MinSelection int32              `json:"minSelection"`
	MaxSelection *int32             `json:"maxSelection"`
	IsRequired   bool               `json:"isRequired"`
	AddonItems   []posMenuAddonItem `json:"addonItems"`
}

type posMenuItem struct {
	ID              int64                  `json:"id"`
	Name            string                 `json:"name"`
	Description     *string                `json:"description"`
	Price           float64                `json:"price"`
	ImageURL        *string                `json:"imageUrl"`
	IsActive        bool                   `json:"isActive"`
	CategoryID      *int64                 `json:"categoryId"`
	TrackStock      bool                   `json:"trackStock"`
	StockQty        *int32                 `json:"stockQty"`
	IsSpicy         bool                   `json:"isSpicy"`
	IsBestSeller    bool                   `json:"isBestSeller"`
	IsSignature     bool                   `json:"isSignature"`
	IsRecommended   bool                   `json:"isRecommended"`
	PromoPrice      *float64               `json:"promoPrice"`
	HasAddons       bool                   `json:"hasAddons"`
	AddonCategories []posMenuAddonCategory `json:"addonCategories"`
}

func (h *Handler) MerchantPOSMenuGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_NOT_FOUND", "Merchant context not found")
		return
	}

	var (
		merchantID                  int64
		currency                    string
		enableTax                   bool
		taxPercent                  pgtype.Numeric
		enableServiceCharge         bool
		serviceChargePercent        pgtype.Numeric
		enablePackagingFee          bool
		packagingFeeAmount          pgtype.Numeric
		totalTables                 pgtype.Int4
		requireTableNumberForDineIn bool
		posPayImmediately           bool
		features                    []byte
	)

	if err := h.DB.QueryRow(ctx, `
		select id, currency, enable_tax, tax_percentage, enable_service_charge, service_charge_percent,
		       enable_packaging_fee, packaging_fee_amount, total_tables, require_table_number_for_dine_in,
		       pos_pay_immediately, features
		from merchants where id = $1
	`, *authCtx.MerchantID).Scan(
		&merchantID,
		&currency,
		&enableTax,
		&taxPercent,
		&enableServiceCharge,
		&serviceChargePercent,
		&enablePackagingFee,
		&packagingFeeAmount,
		&totalTables,
		&requireTableNumberForDineIn,
		&posPayImmediately,
		&features,
	); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	customSettings := parsePosCustomItemsSettings(features, currency)
	editSettings := parsePosEditOrderSettings(features)

	merchantData := map[string]any{
		"id":                          merchantID,
		"currency":                    currency,
		"enableTax":                   enableTax,
		"taxPercentage":               utils.NumericToFloat64(taxPercent),
		"enableServiceCharge":         enableServiceCharge,
		"serviceChargePercent":        utils.NumericToFloat64(serviceChargePercent),
		"enablePackagingFee":          enablePackagingFee,
		"packagingFeeAmount":          utils.NumericToFloat64(packagingFeeAmount),
		"totalTables":                 int4ToPtr(totalTables),
		"requireTableNumberForDineIn": requireTableNumberForDineIn,
		"posPayImmediately":           posPayImmediately,
		"posCustomItems":              buildPosCustomItemsResponse(customSettings),
		"posEditOrder":                buildPosEditOrderResponse(editSettings),
	}

	categories, err := h.fetchPOSMenuCategories(ctx, merchantID)
	if err != nil {
		h.Logger.Error("pos categories fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS menu data")
		return
	}

	menuItems, menuIDs, err := h.fetchPOSMenuItems(ctx, merchantID)
	if err != nil {
		h.Logger.Error("pos menus fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS menu data")
		return
	}

	promoPrices := h.fetchPOSPromoPrices(ctx, merchantID, menuIDs)

	menuAddons, addonCategoryIDs, err := h.fetchPOSMenuAddonCategories(ctx, menuIDs)
	if err != nil {
		h.Logger.Error("pos addon categories fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch POS menu data")
		return
	}

	addonItems := h.fetchPOSAddonItems(ctx, addonCategoryIDs)

	for idx := range menuItems {
		item := &menuItems[idx]
		if promo, ok := promoPrices[item.ID]; ok {
			item.PromoPrice = &promo
		}
		addons := menuAddons[item.ID]
		for addonIdx := range addons {
			addons[addonIdx].AddonItems = addonItems[addons[addonIdx].ID]
		}
		item.AddonCategories = addons
		item.HasAddons = len(addons) > 0
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"merchant":   merchantData,
			"categories": categories,
			"menuItems":  menuItems,
		},
		"statusCode": 200,
	})
}

func (h *Handler) fetchPOSMenuCategories(ctx context.Context, merchantID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
		select id, name, sort_order
		from menu_categories
		where merchant_id = $1 and deleted_at is null and is_active = true
		order by sort_order asc
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	categories := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id        int64
			name      string
			sortOrder int32
		)
		if err := rows.Scan(&id, &name, &sortOrder); err != nil {
			return nil, err
		}
		categories = append(categories, map[string]any{
			"id":        id,
			"name":      name,
			"sortOrder": sortOrder,
		})
	}
	return categories, nil
}

func (h *Handler) fetchPOSMenuItems(ctx context.Context, merchantID int64) ([]posMenuItem, []int64, error) {
	rows, err := h.DB.Query(ctx, `
		select id, name, description, price, image_url, is_active, category_id, track_stock, stock_qty,
		       is_spicy, is_best_seller, is_signature, is_recommended
		from menus
		where merchant_id = $1 and deleted_at is null
		order by name asc
	`, merchantID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := make([]posMenuItem, 0)
	menuIDs := make([]int64, 0)
	for rows.Next() {
		var (
			id            int64
			name          string
			description   pgtype.Text
			price         pgtype.Numeric
			imageURL      pgtype.Text
			isActive      bool
			categoryID    pgtype.Int8
			trackStock    bool
			stockQty      pgtype.Int4
			isSpicy       bool
			isBestSeller  bool
			isSignature   bool
			isRecommended bool
		)
		if err := rows.Scan(&id, &name, &description, &price, &imageURL, &isActive, &categoryID, &trackStock, &stockQty, &isSpicy, &isBestSeller, &isSignature, &isRecommended); err != nil {
			return nil, nil, err
		}
		items = append(items, posMenuItem{
			ID:              id,
			Name:            name,
			Description:     textToPtr(description),
			Price:           utils.NumericToFloat64(price),
			ImageURL:        textToPtr(imageURL),
			IsActive:        isActive,
			CategoryID:      int8ToPtr(categoryID),
			TrackStock:      trackStock,
			StockQty:        int4ToPtr(stockQty),
			IsSpicy:         isSpicy,
			IsBestSeller:    isBestSeller,
			IsSignature:     isSignature,
			IsRecommended:   isRecommended,
			PromoPrice:      nil,
			HasAddons:       false,
			AddonCategories: []posMenuAddonCategory{},
		})
		menuIDs = append(menuIDs, id)
	}
	return items, menuIDs, nil
}

func (h *Handler) fetchPOSPromoPrices(ctx context.Context, merchantID int64, menuIDs []int64) map[int64]float64 {
	promoPrices := make(map[int64]float64)
	if len(menuIDs) == 0 {
		return promoPrices
	}

	rows, err := h.DB.Query(ctx, `
		select spi.menu_id, spi.promo_price
		from special_price_items spi
		join special_prices sp on sp.id = spi.special_price_id
		where spi.menu_id = any($1)
		  and sp.merchant_id = $2
		  and sp.is_active = true
		  and sp.start_date <= now()
		  and sp.end_date >= now()
	`, menuIDs, merchantID)
	if err != nil {
		return promoPrices
	}
	defer rows.Close()

	for rows.Next() {
		var (
			menuID int64
			price  pgtype.Numeric
		)
		if err := rows.Scan(&menuID, &price); err != nil {
			continue
		}
		promoPrices[menuID] = utils.NumericToFloat64(price)
	}
	return promoPrices
}

func (h *Handler) fetchPOSMenuAddonCategories(ctx context.Context, menuIDs []int64) (map[int64][]posMenuAddonCategory, []int64, error) {
	menuAddons := make(map[int64][]posMenuAddonCategory)
	addonCategoryIDs := make([]int64, 0)
	if len(menuIDs) == 0 {
		return menuAddons, addonCategoryIDs, nil
	}

	rows, err := h.DB.Query(ctx, `
		select mac.menu_id, ac.id, ac.name, ac.description, ac.min_selection, ac.max_selection, mac.is_required
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		where mac.menu_id = any($1)
		order by mac.menu_id asc, mac.display_order asc
	`, menuIDs)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	seenCategories := make(map[int64]bool)
	for rows.Next() {
		var (
			menuID       int64
			categoryID   int64
			name         string
			description  pgtype.Text
			minSelection int32
			maxSelection pgtype.Int4
			isRequired   bool
		)
		if err := rows.Scan(&menuID, &categoryID, &name, &description, &minSelection, &maxSelection, &isRequired); err != nil {
			return nil, nil, err
		}
		menuAddons[menuID] = append(menuAddons[menuID], posMenuAddonCategory{
			ID:           categoryID,
			Name:         name,
			Description:  textToPtr(description),
			MinSelection: minSelection,
			MaxSelection: int4ToPtr(maxSelection),
			IsRequired:   isRequired,
			AddonItems:   []posMenuAddonItem{},
		})
		if !seenCategories[categoryID] {
			seenCategories[categoryID] = true
			addonCategoryIDs = append(addonCategoryIDs, categoryID)
		}
	}

	return menuAddons, addonCategoryIDs, nil
}

func (h *Handler) fetchPOSAddonItems(ctx context.Context, addonCategoryIDs []int64) map[int64][]posMenuAddonItem {
	itemsByCategory := make(map[int64][]posMenuAddonItem)
	if len(addonCategoryIDs) == 0 {
		return itemsByCategory
	}

	rows, err := h.DB.Query(ctx, `
		select addon_category_id, id, name, price, is_active, track_stock, stock_qty
		from addon_items
		where addon_category_id = any($1)
		  and is_active = true
		  and deleted_at is null
		order by display_order asc
	`, addonCategoryIDs)
	if err != nil {
		return itemsByCategory
	}
	defer rows.Close()

	for rows.Next() {
		var (
			categoryID int64
			id         int64
			name       string
			price      pgtype.Numeric
			isActive   bool
			trackStock bool
			stockQty   pgtype.Int4
		)
		if err := rows.Scan(&categoryID, &id, &name, &price, &isActive, &trackStock, &stockQty); err != nil {
			continue
		}
		itemsByCategory[categoryID] = append(itemsByCategory[categoryID], posMenuAddonItem{
			ID:         id,
			Name:       name,
			Price:      utils.NumericToFloat64(price),
			IsActive:   isActive,
			TrackStock: trackStock,
			StockQty:   int4ToPtr(stockQty),
		})
	}
	return itemsByCategory
}

func textToPtr(value pgtype.Text) *string {
	if value.Valid {
		return &value.String
	}
	return nil
}

func int4ToPtr(value pgtype.Int4) *int32 {
	if value.Valid {
		v := value.Int32
		return &v
	}
	return nil
}

func int8ToPtr(value pgtype.Int8) *int64 {
	if value.Valid {
		v := value.Int64
		return &v
	}
	return nil
}
