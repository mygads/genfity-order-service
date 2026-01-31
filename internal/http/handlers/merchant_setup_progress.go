package handlers

import (
	"context"
	"math"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type templateMarkers struct {
	CategoryFoodName  string `json:"CATEGORY_FOOD_NAME"`
	CategoryFoodDesc  string `json:"CATEGORY_FOOD_DESC"`
	CategoryDrinkName string `json:"CATEGORY_DRINK_NAME"`
	CategoryDrinkDesc string `json:"CATEGORY_DRINK_DESC"`
	MenuFoodName      string `json:"MENU_FOOD_NAME"`
	MenuFoodDesc      string `json:"MENU_FOOD_DESC"`
	MenuDrinkName     string `json:"MENU_DRINK_NAME"`
	MenuDrinkDesc     string `json:"MENU_DRINK_DESC"`
	AddonCategoryName string `json:"ADDON_CATEGORY_NAME"`
	AddonCategoryDesc string `json:"ADDON_CATEGORY_DESC"`
	AddonItem1Name    string `json:"ADDON_ITEM_1_NAME"`
	AddonItem2Name    string `json:"ADDON_ITEM_2_NAME"`
}

var templateMarkersAUD = templateMarkers{
	CategoryFoodName:  "Food",
	CategoryFoodDesc:  "Starter category for food items.",
	CategoryDrinkName: "Drinks",
	CategoryDrinkDesc: "Starter category for drink items.",
	MenuFoodName:      "Food Item",
	MenuFoodDesc:      "Starter food menu item. Edit the name, price, and add a photo.",
	MenuDrinkName:     "Drink Item",
	MenuDrinkDesc:     "Starter drink menu item. Edit the name, price, and add a photo.",
	AddonCategoryName: "Addons",
	AddonCategoryDesc: "Starter addon options. Edit or add more to customize orders.",
	AddonItem1Name:    "Extra 1",
	AddonItem2Name:    "Extra 2",
}

var templateMarkersIDR = templateMarkers{
	CategoryFoodName:  "Makanan",
	CategoryFoodDesc:  "Kategori awal untuk menu makanan.",
	CategoryDrinkName: "Minuman",
	CategoryDrinkDesc: "Kategori awal untuk menu minuman.",
	MenuFoodName:      "Menu Makanan",
	MenuFoodDesc:      "Menu contoh makanan. Ubah nama, harga, dan tambahkan foto.",
	MenuDrinkName:     "Menu Minuman",
	MenuDrinkDesc:     "Menu contoh minuman. Ubah nama, harga, dan tambahkan foto.",
	AddonCategoryName: "Tambahan",
	AddonCategoryDesc: "Opsi tambahan awal. Edit atau tambahkan untuk menyesuaikan pesanan.",
	AddonItem1Name:    "Ekstra 1",
	AddonItem2Name:    "Ekstra 2",
}

var templateCategoryNames = map[string]struct{}{
	"Food":    {},
	"Drinks":  {},
	"Makanan": {},
	"Minuman": {},
}

var templateMenuNames = map[string]struct{}{
	"Food Item":    {},
	"Drink Item":   {},
	"Menu Makanan": {},
	"Menu Minuman": {},
}

var templateAddonCategoryNames = map[string]struct{}{
	"Addons":   {},
	"Tambahan": {},
}

func (h *Handler) MerchantSetupProgress(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.JSON(w, http.StatusBadRequest, map[string]any{
			"success": false,
			"error":   "No merchant associated with this user",
		})
		return
	}

	var logoURL pgtype.Text
	if err := h.DB.QueryRow(ctx, `select logo_url from merchants where id = $1`, *authCtx.MerchantID).Scan(&logoURL); err != nil {
		response.JSON(w, http.StatusNotFound, map[string]any{
			"success": false,
			"error":   "Merchant not found",
		})
		return
	}

	storeComplete := logoURL.Valid && strings.TrimSpace(logoURL.String) != ""

	categoriesComplete := h.isCategoriesStepComplete(ctx, *authCtx.MerchantID)
	menuComplete := h.isMenuStepComplete(ctx, *authCtx.MerchantID)
	addonsComplete := h.isAddonStepComplete(ctx, *authCtx.MerchantID)
	hoursComplete := h.isHoursStepComplete(ctx, *authCtx.MerchantID)

	steps := map[string]bool{
		"merchant_info": storeComplete,
		"categories":    categoriesComplete,
		"menu_items":    menuComplete,
		"addons":        addonsComplete,
		"opening_hours": hoursComplete,
	}

	requiredSteps := []string{"merchant_info", "categories", "menu_items", "opening_hours"}
	completedRequired := 0
	for _, step := range requiredSteps {
		if steps[step] {
			completedRequired += 1
		}
	}
	totalRequired := len(requiredSteps)
	progressPercent := int(math.Round(float64(completedRequired) / float64(totalRequired) * 100))

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"steps":             steps,
			"completedRequired": completedRequired,
			"totalRequired":     totalRequired,
			"progressPercent":   progressPercent,
			"isComplete":        progressPercent == 100,
			"templateMarkers": map[string]templateMarkers{
				"AUD": templateMarkersAUD,
				"IDR": templateMarkersIDR,
			},
		},
	})
}

func (h *Handler) isCategoriesStepComplete(ctx context.Context, merchantID int64) bool {
	rows, err := h.DB.Query(ctx, `
		select name from menu_categories where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return false
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		names = append(names, name)
	}

	if len(names) == 0 {
		return false
	}
	if len(names) > 2 {
		return true
	}
	for _, name := range names {
		if _, ok := templateCategoryNames[name]; !ok {
			return true
		}
	}
	return false
}

func (h *Handler) isMenuStepComplete(ctx context.Context, merchantID int64) bool {
	rows, err := h.DB.Query(ctx, `
		select name from menus where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return false
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		names = append(names, name)
	}

	if len(names) == 0 {
		return false
	}
	if len(names) >= 3 {
		return true
	}
	for _, name := range names {
		if _, ok := templateMenuNames[name]; !ok {
			return true
		}
	}
	return false
}

func (h *Handler) isAddonStepComplete(ctx context.Context, merchantID int64) bool {
	rows, err := h.DB.Query(ctx, `
		select name from addon_categories where merchant_id = $1 and deleted_at is null
	`, merchantID)
	if err != nil {
		return false
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		names = append(names, name)
	}

	if len(names) == 0 {
		return false
	}
	if len(names) > 1 {
		return true
	}
	if len(names) == 1 {
		if _, ok := templateAddonCategoryNames[names[0]]; !ok {
			return true
		}
	}
	return false
}

func (h *Handler) isHoursStepComplete(ctx context.Context, merchantID int64) bool {
	rows, err := h.DB.Query(ctx, `
		select open_time, close_time, is_closed from merchant_opening_hours where merchant_id = $1
	`, merchantID)
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var openTime string
		var closeTime string
		var isClosed bool
		if err := rows.Scan(&openTime, &closeTime, &isClosed); err != nil {
			continue
		}
		if isClosed || openTime != "00:00" || closeTime != "23:59" {
			return true
		}
	}

	return false
}
