package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) MerchantReservationCount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID not found")
		return
	}

	var tz pgtype.Text
	if err := h.DB.QueryRow(ctx, "select timezone from merchants where id = $1", *authCtx.MerchantID).Scan(&tz); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservation counts")
		return
	}

	timezone := "Australia/Sydney"
	if tz.Valid {
		timezone = tz.String
	}

	today := utils.CurrentDateInTimezone(timezone)
	nowTime := utils.CurrentTimeInTimezone(timezone)

	var pending, acceptedUpcoming int
	pendingQuery := `select count(*) from reservations where merchant_id = $1 and status = 'PENDING'`
	acceptedQuery := `
		select count(*) from reservations
		where merchant_id = $1
		  and status = 'ACCEPTED'
		  and (
			reservation_date > $2
			or (reservation_date = $2 and reservation_time >= $3)
		  )
	`
	if err := h.DB.QueryRow(ctx, pendingQuery, *authCtx.MerchantID).Scan(&pending); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservation counts")
		return
	}
	if err := h.DB.QueryRow(ctx, acceptedQuery, *authCtx.MerchantID, today, nowTime).Scan(&acceptedUpcoming); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch reservation counts")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"pending": pending,
			"active":  pending + acceptedUpcoming,
		},
	})
}

func (h *Handler) MerchantCustomerSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant context missing")
		return
	}

	q := strings.TrimSpace(r.URL.Query().Get("q"))
	take, _ := strconv.Atoi(r.URL.Query().Get("take"))
	if take <= 0 {
		take = 20
	}
	if take > 50 {
		take = 50
	}
	cursorParam := strings.TrimSpace(r.URL.Query().Get("cursor"))

	var cursorID *int64
	if cursorParam != "" {
		var parsed int64
		if _, err := fmt.Sscan(cursorParam, &parsed); err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid cursor")
			return
		}
		cursorID = &parsed
	}

	whereClause := "where o.merchant_id = $1"
	args := []any{*authCtx.MerchantID}
	if q != "" {
		whereClause += " and (c.name ilike $2 or c.email ilike $2 or c.phone ilike $2)"
		args = append(args, "%"+q+"%")
	}
	if cursorID != nil {
		whereClause += fmt.Sprintf(" and c.id < $%d", len(args)+1)
		args = append(args, *cursorID)
	}

	query := `
		select distinct c.id, c.name, c.email, c.phone
		from customers c
		join orders o on o.customer_id = c.id
		` + whereClause + `
		order by c.id desc
		limit $` + strconv.Itoa(len(args)+1)
	args = append(args, take+1)

	rows, err := h.DB.Query(ctx, query, args...)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch customers")
		return
	}
	defer rows.Close()

	customers := make([]CustomerSummary, 0)
	ids := make([]int64, 0)
	for rows.Next() {
		var c CustomerSummary
		var email pgtype.Text
		var phone pgtype.Text
		if err := rows.Scan(&c.ID, &c.Name, &email, &phone); err != nil {
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch customers")
			return
		}
		if email.Valid {
			c.Email = &email.String
		}
		if phone.Valid {
			c.Phone = &phone.String
		}
		customers = append(customers, c)
		ids = append(ids, c.ID)
	}

	hasMore := len(customers) > take
	if hasMore {
		customers = customers[:take]
	}

	statsMap := make(map[int64]map[string]any)
	if len(ids) > 0 {
		statsQuery := `
			select customer_id, count(*) as order_count, sum(total_amount) as total_spent, max(placed_at) as last_order_at
			from orders
			where merchant_id = $1 and customer_id = any($2)
			group by customer_id
		`
		statRows, err := h.DB.Query(ctx, statsQuery, *authCtx.MerchantID, ids)
		if err == nil {
			for statRows.Next() {
				var customerID int64
				var orderCount int64
				var totalSpent pgtype.Numeric
				var lastOrderAt pgtype.Timestamptz
				if err := statRows.Scan(&customerID, &orderCount, &totalSpent, &lastOrderAt); err == nil {
					statsMap[customerID] = map[string]any{
						"orderCount": orderCount,
						"totalSpent": utils.NumericToFloat64(totalSpent),
						"lastOrderAt": func() any {
							if lastOrderAt.Valid {
								return lastOrderAt.Time
							}
							return nil
						}(),
					}
				}
			}
			statRows.Close()
		}
	}

	enriched := make([]map[string]any, 0, len(customers))
	for _, c := range customers {
		row := map[string]any{
			"id":          c.ID,
			"name":        c.Name,
			"email":       c.Email,
			"phone":       c.Phone,
			"orderCount":  0,
			"totalSpent":  0,
			"lastOrderAt": nil,
		}
		if stats, ok := statsMap[c.ID]; ok {
			row["orderCount"] = stats["orderCount"]
			row["totalSpent"] = stats["totalSpent"]
			row["lastOrderAt"] = stats["lastOrderAt"]
		}
		enriched = append(enriched, row)
	}

	var nextCursor *int64
	if hasMore && len(customers) > 0 {
		last := customers[len(customers)-1].ID
		nextCursor = &last
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data":    enriched,
		"pagination": map[string]any{
			"take":   take,
			"cursor": cursorParam,
			"nextCursor": func() any {
				if nextCursor == nil {
					return nil
				}
				return fmt.Sprint(*nextCursor)
			}(),
			"hasMore": hasMore,
		},
	})
}
