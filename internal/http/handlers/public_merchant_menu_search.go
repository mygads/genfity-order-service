package handlers

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func levenshteinDistance(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	cols := len(a) + 1
	prev := make([]int, cols)
	cur := make([]int, cols)
	for i := 0; i < cols; i++ {
		prev[i] = i
	}

	for i := 1; i <= len(b); i++ {
		cur[0] = i
		for j := 1; j < cols; j++ {
			cost := 0
			if b[i-1] != a[j-1] {
				cost = 1
			}
			deletion := prev[j] + 1
			insertion := cur[j-1] + 1
			substitution := prev[j-1] + cost
			cur[j] = minInt(deletion, minInt(insertion, substitution))
		}
		copy(prev, cur)
	}
	return prev[cols-1]
}

func calculateRelevance(query, name string, description *string) float64 {
	q := strings.ToLower(strings.TrimSpace(query))
	n := strings.ToLower(name)
	d := ""
	if description != nil {
		d = strings.ToLower(*description)
	}

	score := 0.0
	if n == q {
		score += 100
	} else if strings.HasPrefix(n, q) {
		score += 80
	} else if strings.Contains(n, q) {
		score += 60
	} else if d != "" && strings.Contains(d, q) {
		score += 40
	} else {
		distance := levenshteinDistance(q, n)
		maxLen := float64(maxInt(len(q), len(n)))
		similarity := (1 - float64(distance)/maxLen) * 100
		if similarity > 30 {
			score += similarity - 30
		}
	}

	qWords := strings.Fields(q)
	nWords := strings.Fields(n)
	for _, qWord := range qWords {
		for _, nWord := range nWords {
			if strings.HasPrefix(nWord, qWord) {
				score += 10
				break
			}
		}
	}

	if score > 100 {
		return 100
	}
	if score < 0 {
		return 0
	}
	return score
}

func (h *Handler) PublicMerchantMenuSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(query) < 2 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success": true,
			"data":    []any{},
			"message": "Query must be at least 2 characters",
		})
		return
	}

	categoryParam := strings.TrimSpace(r.URL.Query().Get("category"))
	if categoryParam == "" {
		categoryParam = strings.TrimSpace(r.URL.Query().Get("categoryId"))
	}
	minPriceParam := strings.TrimSpace(r.URL.Query().Get("minPrice"))
	maxPriceParam := strings.TrimSpace(r.URL.Query().Get("maxPrice"))
	sortParam := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sortParam == "" {
		sortParam = "relevance"
	}

	var minPrice *float64
	if minPriceParam != "" {
		parsed, err := strconv.ParseFloat(minPriceParam, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid minPrice")
			return
		}
		minPrice = &parsed
	}

	var maxPrice *float64
	if maxPriceParam != "" {
		parsed, err := strconv.ParseFloat(maxPriceParam, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid maxPrice")
			return
		}
		maxPrice = &parsed
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
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

	where := `m.merchant_id = $1 and m.is_active = true and m.deleted_at is null`
	args := []any{merchantID}
	if categoryParam != "" && categoryParam != "all" {
		parsed, err := strconv.ParseInt(categoryParam, 10, 64)
		if err != nil || parsed <= 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid category")
			return
		}
		where += " and exists (select 1 from menu_category_items mci where mci.menu_id = m.id and mci.category_id = $" + strconv.Itoa(len(args)+1) + ")"
		args = append(args, parsed)
	}
	if minPrice != nil {
		where += " and m.price >= $" + strconv.Itoa(len(args)+1)
		args = append(args, *minPrice)
	}
	if maxPrice != nil {
		where += " and m.price <= $" + strconv.Itoa(len(args)+1)
		args = append(args, *maxPrice)
	}

	rows, err := h.DB.Query(ctx, `
		select m.id, m.name, m.description, m.price, m.image_url, m.image_thumb_url, m.image_thumb_meta,
		       m.is_active, m.is_spicy, m.is_best_seller, m.is_signature, m.is_recommended,
		       m.track_stock, m.stock_qty
		from menus m
		where `+where, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "SEARCH_ERROR", "Failed to search menus")
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
			response.Error(w, http.StatusInternalServerError, "SEARCH_ERROR", "Failed to search menus")
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
			"success": true,
			"data":    []any{},
			"meta": map[string]any{
				"query": query,
				"total": 0,
			},
		})
		return
	}

	menuCategories := make(map[int64][]map[string]any)
	catRows, err := h.DB.Query(ctx, `
		select mci.menu_id, c.id, c.name
		from menu_category_items mci
		join menu_categories c on c.id = mci.category_id
		where mci.menu_id = any($1)
	`, menuIDs)
	if err == nil {
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
	}

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

	type scoredMenu struct {
		payload      map[string]any
		score        float64
		price        float64
		promoPrice   *float64
		name         string
		isBestSeller bool
		isSignature  bool
	}
	results := make([]scoredMenu, 0, len(menus))
	for _, menu := range menus {
		score := calculateRelevance(query, menu.Name, menu.Description)
		promo, promoOk := promoMap[menu.ID]
		var promoPtr *float64
		if promoOk {
			promoPtr = &promo
		}
		results = append(results, scoredMenu{
			score: score,
			payload: map[string]any{
				"id":             menu.ID,
				"name":           menu.Name,
				"description":    menu.Description,
				"price":          menu.Price,
				"imageUrl":       menu.ImageURL,
				"imageThumbUrl":  menu.ImageThumbURL,
				"imageThumbMeta": menu.ImageThumbMeta,
				"isActive":       menu.IsActive,
				"isPromo":        promoOk,
				"promoPrice": func() any {
					if promoOk {
						return promo
					}
					return nil
				}(),
				"isSpicy":        menu.IsSpicy,
				"isBestSeller":   menu.IsBestSeller,
				"isSignature":    menu.IsSignature,
				"isRecommended":  menu.IsRecommended,
				"trackStock":     menu.TrackStock,
				"stockQty":       menu.StockQty,
				"categories":     menuCategories[menu.ID],
				"relevanceScore": score,
			},
			price:        menu.Price,
			promoPrice:   promoPtr,
			name:         strings.ToLower(menu.Name),
			isBestSeller: menu.IsBestSeller,
			isSignature:  menu.IsSignature,
		})
	}

	filtered := make([]scoredMenu, 0, len(results))
	for _, item := range results {
		if item.score > 20 {
			filtered = append(filtered, item)
		}
	}

	switch sortParam {
	case "price_asc":
		sort.Slice(filtered, func(i, j int) bool {
			leftPrice := filtered[i].price
			rightPrice := filtered[j].price
			if filtered[i].promoPrice != nil {
				leftPrice = *filtered[i].promoPrice
			}
			if filtered[j].promoPrice != nil {
				rightPrice = *filtered[j].promoPrice
			}
			if leftPrice == rightPrice {
				return filtered[i].score > filtered[j].score
			}
			return leftPrice < rightPrice
		})
	case "price_desc":
		sort.Slice(filtered, func(i, j int) bool {
			leftPrice := filtered[i].price
			rightPrice := filtered[j].price
			if filtered[i].promoPrice != nil {
				leftPrice = *filtered[i].promoPrice
			}
			if filtered[j].promoPrice != nil {
				rightPrice = *filtered[j].promoPrice
			}
			if leftPrice == rightPrice {
				return filtered[i].score > filtered[j].score
			}
			return leftPrice > rightPrice
		})
	case "name":
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].name == filtered[j].name {
				return filtered[i].score > filtered[j].score
			}
			return filtered[i].name < filtered[j].name
		})
	case "popular":
		sort.Slice(filtered, func(i, j int) bool {
			leftPopular := 0
			if filtered[i].isBestSeller {
				leftPopular += 2
			}
			if filtered[i].isSignature {
				leftPopular++
			}
			rightPopular := 0
			if filtered[j].isBestSeller {
				rightPopular += 2
			}
			if filtered[j].isSignature {
				rightPopular++
			}
			if leftPopular == rightPopular {
				return filtered[i].name < filtered[j].name
			}
			return leftPopular > rightPopular
		})
	default:
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].score > filtered[j].score
		})
	}

	if len(filtered) > limit {
		filtered = filtered[:limit]
	}

	final := make([]map[string]any, 0, len(filtered))
	for _, item := range filtered {
		final = append(final, item.payload)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    final,
		"meta": map[string]any{
			"query": query,
			"total": len(final),
		},
	})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
