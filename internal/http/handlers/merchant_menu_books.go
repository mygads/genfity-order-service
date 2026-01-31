package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type menuBookPayload struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	IsActive    *bool  `json:"isActive"`
	MenuIDs     []any  `json:"menuIds"`
}

func (h *Handler) MerchantMenuBooksList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select id, merchant_id, name, description, is_active, created_at, updated_at
        from menu_books
        where merchant_id = $1
        order by created_at desc
    `, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("menu books query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu books")
		return
	}
	defer rows.Close()

	books := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id          int64
			merchantID  int64
			name        string
			description pgtype.Text
			isActive    bool
			createdAt   time.Time
			updatedAt   time.Time
		)
		if err := rows.Scan(&id, &merchantID, &name, &description, &isActive, &createdAt, &updatedAt); err != nil {
			h.Logger.Error("menu books scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu books")
			return
		}

		items, err := h.fetchMenuBookItems(ctx, id)
		if err != nil {
			h.Logger.Error("menu book items fetch failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu books")
			return
		}
		itemCount, specialCount := h.fetchMenuBookCounts(ctx, id)

		books = append(books, map[string]any{
			"id":          id,
			"merchantId":  merchantID,
			"name":        name,
			"description": nullableText(description),
			"isActive":    isActive,
			"createdAt":   createdAt,
			"updatedAt":   updatedAt,
			"items":       items,
			"_count": map[string]any{
				"items":         itemCount,
				"specialPrices": specialCount,
			},
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    books,
	})
}

func (h *Handler) MerchantMenuBooksCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var body menuBookPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(body.Name)
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Name is required")
		return
	}
	description := strings.TrimSpace(body.Description)
	menuIDs := parseIDArray(body.MenuIDs)

	var created map[string]any
	tx, err := h.DB.Begin(ctx)
	if err != nil {
		h.Logger.Error("menu book create begin failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu book")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		id        int64
		createdAt time.Time
		updatedAt time.Time
	)
	if err := tx.QueryRow(ctx, `
		insert into menu_books (merchant_id, name, description, is_active, created_at, updated_at)
		values ($1, $2, $3, true, now(), now())
		returning id, created_at, updated_at
	`, *authCtx.MerchantID, name, nullableString(description)).Scan(&id, &createdAt, &updatedAt); err != nil {
		h.Logger.Error("menu book insert failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu book")
		return
	}

	if len(menuIDs) > 0 {
		if err := h.insertMenuBookItems(ctx, tx, id, menuIDs); err != nil {
			h.Logger.Error("menu book items insert failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu book")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("menu book create commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu book")
		return
	}

	items, err := h.fetchMenuBookItems(ctx, id)
	if err != nil {
		h.Logger.Error("menu book items fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create menu book")
		return
	}

	created = map[string]any{
		"id":          id,
		"merchantId":  *authCtx.MerchantID,
		"name":        name,
		"description": nullableString(description),
		"isActive":    true,
		"createdAt":   createdAt,
		"updatedAt":   updatedAt,
		"items":       items,
	}

	response.JSON(w, http.StatusCreated, map[string]any{
		"success": true,
		"data":    created,
		"message": "Menu book created successfully",
	})
}

func (h *Handler) MerchantMenuBooksGet(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	bookID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu book id")
		return
	}

	var (
		merchantID  int64
		name        string
		description pgtype.Text
		isActive    bool
		createdAt   time.Time
		updatedAt   time.Time
	)
	if err := h.DB.QueryRow(ctx, `
        select merchant_id, name, description, is_active, created_at, updated_at
        from menu_books
        where id = $1 and merchant_id = $2
    `, bookID, *authCtx.MerchantID).Scan(&merchantID, &name, &description, &isActive, &createdAt, &updatedAt); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu book not found")
			return
		}
		h.Logger.Error("menu book lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu book")
		return
	}

	items, err := h.fetchMenuBookItems(ctx, bookID)
	if err != nil {
		h.Logger.Error("menu book items fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu book")
		return
	}

	specialPrices, err := h.fetchMenuBookSpecialPrices(ctx, bookID)
	if err != nil {
		h.Logger.Error("menu book special prices fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve menu book")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":            bookID,
			"merchantId":    merchantID,
			"name":          name,
			"description":   nullableText(description),
			"isActive":      isActive,
			"createdAt":     createdAt,
			"updatedAt":     updatedAt,
			"items":         items,
			"specialPrices": specialPrices,
		},
	})
}

func (h *Handler) MerchantMenuBooksUpdate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	bookID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu book id")
		return
	}

	var body menuBookPayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	var (
		currentName        string
		currentDescription pgtype.Text
		currentActive      bool
	)
	if err := h.DB.QueryRow(ctx, `
        select name, description, is_active
        from menu_books
        where id = $1 and merchant_id = $2
    `, bookID, *authCtx.MerchantID).Scan(&currentName, &currentDescription, &currentActive); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Menu book not found")
			return
		}
		h.Logger.Error("menu book lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
		return
	}

	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = currentName
	}
	description := currentDescription.String
	if body.Description != "" || currentDescription.Valid {
		if body.Description != "" {
			description = strings.TrimSpace(body.Description)
		}
	}
	isActive := currentActive
	if body.IsActive != nil {
		isActive = *body.IsActive
	}

	menuIDs := parseIDArray(body.MenuIDs)
	menuIDsProvided := body.MenuIDs != nil

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		h.Logger.Error("menu book update begin failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
        update menu_books
        set name = $1, description = $2, is_active = $3, updated_at = now()
        where id = $4 and merchant_id = $5
    `, name, nullableString(description), isActive, bookID, *authCtx.MerchantID); err != nil {
		h.Logger.Error("menu book update failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
		return
	}

	if menuIDsProvided {
		if _, err := tx.Exec(ctx, "delete from menu_book_items where menu_book_id = $1", bookID); err != nil {
			h.Logger.Error("menu book items delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
			return
		}
		if len(menuIDs) > 0 {
			if err := h.insertMenuBookItems(ctx, tx, bookID, menuIDs); err != nil {
				h.Logger.Error("menu book items insert failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
				return
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("menu book update commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
		return
	}

	items, err := h.fetchMenuBookItems(ctx, bookID)
	if err != nil {
		h.Logger.Error("menu book items fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update menu book")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":          bookID,
			"merchantId":  *authCtx.MerchantID,
			"name":        name,
			"description": nullableString(description),
			"isActive":    isActive,
			"items":       items,
		},
		"message": "Menu book updated successfully",
	})
}

func (h *Handler) MerchantMenuBooksDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	bookID, err := readPathInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid menu book id")
		return
	}

	var specialCount int64
	if err := h.DB.QueryRow(ctx, `
        select count(*)
        from special_prices
        where menu_book_id = $1
    `, bookID).Scan(&specialCount); err != nil {
		h.Logger.Error("menu book special prices count failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete menu book")
		return
	}

	if specialCount > 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Cannot delete menu book with active special prices")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from menu_books where id = $1 and merchant_id = $2", bookID, *authCtx.MerchantID); err != nil {
		h.Logger.Error("menu book delete failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete menu book")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Menu book deleted successfully",
	})
}

func (h *Handler) fetchMenuBookItems(ctx context.Context, menuBookID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select mbi.menu_id, m.name, m.price, m.image_url
        from menu_book_items mbi
        join menus m on m.id = mbi.menu_id
        where mbi.menu_book_id = $1
    `, menuBookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			menuID   int64
			name     string
			price    pgtype.Numeric
			imageURL pgtype.Text
		)
		if err := rows.Scan(&menuID, &name, &price, &imageURL); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"menu": map[string]any{
				"id":       menuID,
				"name":     name,
				"price":    utils.NumericToFloat64(price),
				"imageUrl": nullableText(imageURL),
			},
		})
	}
	return items, nil
}

func (h *Handler) fetchMenuBookSpecialPrices(ctx context.Context, menuBookID int64) ([]map[string]any, error) {
	rows, err := h.DB.Query(ctx, `
        select id, name, is_active
        from special_prices
        where menu_book_id = $1
    `, menuBookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id       int64
			name     string
			isActive bool
		)
		if err := rows.Scan(&id, &name, &isActive); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{
			"id":       id,
			"name":     name,
			"isActive": isActive,
		})
	}
	return items, nil
}

func (h *Handler) fetchMenuBookCounts(ctx context.Context, menuBookID int64) (int64, int64) {
	var itemCount int64
	_ = h.DB.QueryRow(ctx, "select count(*) from menu_book_items where menu_book_id = $1", menuBookID).Scan(&itemCount)
	var specialCount int64
	_ = h.DB.QueryRow(ctx, "select count(*) from special_prices where menu_book_id = $1", menuBookID).Scan(&specialCount)
	return itemCount, specialCount
}

func (h *Handler) insertMenuBookItems(ctx context.Context, tx pgx.Tx, menuBookID int64, menuIDs []int64) error {
	for _, menuID := range menuIDs {
		if _, err := tx.Exec(ctx, `
            insert into menu_book_items (menu_book_id, menu_id)
            values ($1, $2)
            on conflict do nothing
        `, menuBookID, menuID); err != nil {
			return err
		}
	}
	return nil
}

func parseIDArray(values []any) []int64 {
	out := make([]int64, 0)
	seen := make(map[int64]struct{})
	for _, value := range values {
		var parsed int64
		switch v := value.(type) {
		case float64:
			parsed = int64(v)
		case int64:
			parsed = v
		case int:
			parsed = int64(v)
		case string:
			val := strings.TrimSpace(v)
			if val == "" {
				continue
			}
			intVal, err := parseStringToInt64(val)
			if err != nil {
				continue
			}
			parsed = intVal
		default:
			continue
		}
		if parsed == 0 {
			continue
		}
		if _, ok := seen[parsed]; ok {
			continue
		}
		seen[parsed] = struct{}{}
		out = append(out, parsed)
	}
	return out
}
