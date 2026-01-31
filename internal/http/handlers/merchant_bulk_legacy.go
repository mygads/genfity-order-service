package handlers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type bulkLegacyOptions struct {
	RoundTo  *float64 `json:"roundTo"`
	MinPrice *float64 `json:"minPrice"`
	MaxPrice *float64 `json:"maxPrice"`
}

type bulkMenuLegacyPayload struct {
	Operation string             `json:"operation"`
	MenuIDs   []any              `json:"menuIds"`
	Value     any                `json:"value"`
	Options   *bulkLegacyOptions `json:"options"`
}

type bulkAddonItemsLegacyPayload struct {
	Operation    string             `json:"operation"`
	AddonItemIDs []any              `json:"addonItemIds"`
	Value        any                `json:"value"`
	Options      *bulkLegacyOptions `json:"options"`
}

func (h *Handler) MerchantBulkMenuLegacy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkMenuLegacyPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	menuIDs, err := parseAnyIDList(payload.MenuIDs)
	if err != nil || len(menuIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Operation and menuIds are required")
		return
	}

	var menuCount int64
	if err := h.DB.QueryRow(ctx, `
		select count(*) from menus where id = any($1) and merchant_id = $2 and deleted_at is null
	`, menuIDs, *authCtx.MerchantID).Scan(&menuCount); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate menus")
		return
	}
	if menuCount != int64(len(menuIDs)) {
		response.Error(w, http.StatusBadRequest, "INVALID_MENUS", "Some menu items not found or access denied")
		return
	}

	operation := strings.ToUpper(strings.TrimSpace(payload.Operation))
	affected, err := h.applyMenuBulkLegacy(ctx, authCtx, operation, menuIDs, payload.Value, payload.Options)
	if err != nil {
		code := "INVALID_OPERATION"
		message := err.Error()
		if bulkErr, ok := err.(bulkLegacyError); ok {
			code = bulkErr.code
			message = bulkErr.message
		}
		response.Error(w, http.StatusBadRequest, code, message)
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"operation": operation,
			"affected":  affected,
			"menuIds":   int64SliceToStrings(menuIDs),
		},
		"message": "Successfully updated " + int64ToString(affected) + " menu items",
	})
}

func (h *Handler) MerchantBulkAddonItemsLegacy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var payload bulkAddonItemsLegacyPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	addonItemIDs, err := parseAnyIDList(payload.AddonItemIDs)
	if err != nil || len(addonItemIDs) == 0 {
		response.Error(w, http.StatusBadRequest, "INVALID_INPUT", "Operation and addonItemIds are required")
		return
	}

	var itemCount int64
	if err := h.DB.QueryRow(ctx, `
		select count(*)
		from addon_items ai
		join addon_categories ac on ac.id = ai.addon_category_id
		where ai.id = any($1) and ai.deleted_at is null and ac.merchant_id = $2
	`, addonItemIDs, *authCtx.MerchantID).Scan(&itemCount); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to validate addon items")
		return
	}
	if itemCount != int64(len(addonItemIDs)) {
		response.Error(w, http.StatusBadRequest, "INVALID_ITEMS", "Some addon items not found or access denied")
		return
	}

	operation := strings.ToUpper(strings.TrimSpace(payload.Operation))
	affected, err := h.applyAddonBulkLegacy(ctx, authCtx, operation, addonItemIDs, payload.Value, payload.Options)
	if err != nil {
		code := "INVALID_OPERATION"
		message := err.Error()
		if bulkErr, ok := err.(bulkLegacyError); ok {
			code = bulkErr.code
			message = bulkErr.message
		}
		response.Error(w, http.StatusBadRequest, code, message)
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"operation":    operation,
			"affected":     affected,
			"addonItemIds": int64SliceToStrings(addonItemIDs),
		},
		"message": "Successfully updated " + int64ToString(affected) + " addon items",
	})
}

func (h *Handler) applyMenuBulkLegacy(
	ctx context.Context,
	authCtx *middleware.AuthContext,
	operation string,
	menuIDs []int64,
	value any,
	options *bulkLegacyOptions,
) (int64, error) {
	switch operation {
	case "UPDATE_PRICE":
		price, ok := parseFloatValue(value)
		if !ok || price < 0 {
			return 0, errInvalidValue("Price must be a positive number")
		}
		result, err := h.DB.Exec(ctx, `
			update menus set price = $1, updated_at = now(), updated_by_user_id = $2
			where id = any($3) and merchant_id = $4
		`, price, authCtx.UserID, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "UPDATE_PRICE_PERCENT":
		percent, ok := parseFloatValue(value)
		if !ok {
			return 0, errInvalidValue("Percentage must be a number")
		}
		rows, err := h.DB.Query(ctx, `
			select id, price from menus where id = any($1) and merchant_id = $2 and deleted_at is null
		`, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer rows.Close()

		tx, err := h.DB.Begin(ctx)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		count := int64(0)
		for rows.Next() {
			var id int64
			var price pgtype.Numeric
			if err := rows.Scan(&id, &price); err != nil {
				continue
			}
			current := utils.NumericToFloat64(price)
			newPrice := current * (1 + percent/100)
			newPrice = applyPriceOptions(newPrice, options)
			if _, err := tx.Exec(ctx, `
				update menus set price = $1, updated_at = now(), updated_by_user_id = $2 where id = $3
			`, newPrice, authCtx.UserID, id); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			count++
		}

		if err = tx.Commit(ctx); err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return count, nil
	case "UPDATE_STOCK":
		adjustment, ok := parseFloatValue(value)
		if !ok {
			return 0, errInvalidValue("Stock adjustment must be a number")
		}
		rows, err := h.DB.Query(ctx, `
			select id, stock_qty from menus where id = any($1) and merchant_id = $2 and deleted_at is null
		`, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer rows.Close()

		tx, err := h.DB.Begin(ctx)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		count := int64(0)
		for rows.Next() {
			var id int64
			var stock pgtype.Int4
			if err := rows.Scan(&id, &stock); err != nil {
				continue
			}
			current := int32(0)
			if stock.Valid {
				current = stock.Int32
			}
			newStock := int32(math.Max(0, float64(current)+adjustment))
			if _, err := tx.Exec(ctx, `
				update menus set stock_qty = $1, track_stock = true, updated_at = now(), updated_by_user_id = $2 where id = $3
			`, newStock, authCtx.UserID, id); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			count++
		}

		if err = tx.Commit(ctx); err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return count, nil
	case "SET_STOCK":
		stockValue, ok := parseFloatValue(value)
		if !ok || stockValue < 0 {
			return 0, errInvalidValue("Stock must be a non-negative number")
		}
		result, err := h.DB.Exec(ctx, `
			update menus set stock_qty = $1, track_stock = true, updated_at = now(), updated_by_user_id = $2
			where id = any($3) and merchant_id = $4
		`, int32(stockValue), authCtx.UserID, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "UPDATE_STATUS":
		status, ok := parseBoolValue(value)
		if !ok {
			return 0, errInvalidValue("Status must be true or false")
		}
		result, err := h.DB.Exec(ctx, `
			update menus set is_active = $1, updated_at = now(), updated_by_user_id = $2
			where id = any($3) and merchant_id = $4
		`, status, authCtx.UserID, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "UPDATE_CATEGORIES":
		categoryIDs, ok := parseIDArrayValue(value)
		if !ok {
			return 0, errInvalidValue("Categories must be an array of IDs")
		}
		if len(categoryIDs) > 0 {
			var count int64
			if err := h.DB.QueryRow(ctx, `
				select count(*) from menu_categories where id = any($1) and merchant_id = $2 and deleted_at is null
			`, categoryIDs, *authCtx.MerchantID).Scan(&count); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			if count != int64(len(categoryIDs)) {
				return 0, errInvalidValue("Some categories not found")
			}
		}

		tx, err := h.DB.Begin(ctx)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		for _, menuID := range menuIDs {
			if _, err := tx.Exec(ctx, `delete from menu_category_items where menu_id = $1`, menuID); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			if len(categoryIDs) > 0 {
				for _, categoryID := range categoryIDs {
					if _, err := tx.Exec(ctx, `
						insert into menu_category_items (menu_id, category_id, created_at, updated_at)
						values ($1,$2,now(),now())
					`, menuID, categoryID); err != nil {
						return 0, errOperation("Failed to execute bulk operation")
					}
				}
			}
			if _, err := tx.Exec(ctx, `
				update menus set updated_at = now(), updated_by_user_id = $1 where id = $2
			`, authCtx.UserID, menuID); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
		}

		if err = tx.Commit(ctx); err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return int64(len(menuIDs)), nil
	case "DELETE":
		result, err := h.DB.Exec(ctx, `
			update menus set deleted_at = now(), deleted_by_user_id = $1, is_active = false, updated_at = now()
			where id = any($2) and merchant_id = $3
		`, authCtx.UserID, menuIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	default:
		return 0, errInvalidValue("Unknown operation")
	}
}

func (h *Handler) applyAddonBulkLegacy(
	ctx context.Context,
	authCtx *middleware.AuthContext,
	operation string,
	addonItemIDs []int64,
	value any,
	options *bulkLegacyOptions,
) (int64, error) {
	switch operation {
	case "UPDATE_PRICE":
		price, ok := parseFloatValue(value)
		if !ok || price < 0 {
			return 0, errInvalidValue("Price must be a positive number")
		}
		result, err := h.DB.Exec(ctx, `
			update addon_items set price = $1, updated_at = now(), updated_by_user_id = $2
			where id = any($3)
		`, price, authCtx.UserID, addonItemIDs)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "UPDATE_PRICE_PERCENT":
		percent, ok := parseFloatValue(value)
		if !ok {
			return 0, errInvalidValue("Percentage must be a number")
		}
		rows, err := h.DB.Query(ctx, `
			select ai.id, ai.price
			from addon_items ai
			join addon_categories ac on ac.id = ai.addon_category_id
			where ai.id = any($1) and ai.deleted_at is null and ac.merchant_id = $2
		`, addonItemIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer rows.Close()

		tx, err := h.DB.Begin(ctx)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		count := int64(0)
		for rows.Next() {
			var id int64
			var price pgtype.Numeric
			if err := rows.Scan(&id, &price); err != nil {
				continue
			}
			current := utils.NumericToFloat64(price)
			newPrice := current * (1 + percent/100)
			newPrice = applyPriceOptions(newPrice, options)
			if _, err := tx.Exec(ctx, `
				update addon_items set price = $1, updated_at = now(), updated_by_user_id = $2 where id = $3
			`, newPrice, authCtx.UserID, id); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			count++
		}

		if err = tx.Commit(ctx); err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return count, nil
	case "UPDATE_STOCK":
		adjustment, ok := parseFloatValue(value)
		if !ok {
			return 0, errInvalidValue("Stock adjustment must be a number")
		}
		rows, err := h.DB.Query(ctx, `
			select ai.id, ai.stock_qty
			from addon_items ai
			join addon_categories ac on ac.id = ai.addon_category_id
			where ai.id = any($1) and ai.deleted_at is null and ac.merchant_id = $2
		`, addonItemIDs, *authCtx.MerchantID)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer rows.Close()

		tx, err := h.DB.Begin(ctx)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		count := int64(0)
		for rows.Next() {
			var id int64
			var stock pgtype.Int4
			if err := rows.Scan(&id, &stock); err != nil {
				continue
			}
			current := int32(0)
			if stock.Valid {
				current = stock.Int32
			}
			newStock := int32(math.Max(0, float64(current)+adjustment))
			if _, err := tx.Exec(ctx, `
				update addon_items set stock_qty = $1, track_stock = true, updated_at = now(), updated_by_user_id = $2 where id = $3
			`, newStock, authCtx.UserID, id); err != nil {
				return 0, errOperation("Failed to execute bulk operation")
			}
			count++
		}

		if err = tx.Commit(ctx); err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return count, nil
	case "SET_STOCK":
		stockValue, ok := parseFloatValue(value)
		if !ok || stockValue < 0 {
			return 0, errInvalidValue("Stock must be a non-negative number")
		}
		result, err := h.DB.Exec(ctx, `
			update addon_items set stock_qty = $1, track_stock = true, updated_at = now(), updated_by_user_id = $2
			where id = any($3)
		`, int32(stockValue), authCtx.UserID, addonItemIDs)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "UPDATE_STATUS":
		status, ok := parseBoolValue(value)
		if !ok {
			return 0, errInvalidValue("Status must be true or false")
		}
		result, err := h.DB.Exec(ctx, `
			update addon_items set is_active = $1, updated_at = now(), updated_by_user_id = $2
			where id = any($3)
		`, status, authCtx.UserID, addonItemIDs)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	case "DELETE":
		result, err := h.DB.Exec(ctx, `
			update addon_items set deleted_at = now(), deleted_by_user_id = $1, is_active = false, updated_at = now()
			where id = any($2)
		`, authCtx.UserID, addonItemIDs)
		if err != nil {
			return 0, errOperation("Failed to execute bulk operation")
		}
		return result.RowsAffected(), nil
	default:
		return 0, errInvalidValue("Unknown operation")
	}
}

func int64SliceToStrings(values []int64) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, int64ToString(value))
	}
	return out
}
