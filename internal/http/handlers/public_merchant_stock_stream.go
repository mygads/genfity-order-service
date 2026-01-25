package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type stockAddonItem struct {
	ID         string `json:"id"`
	StockQty   *int32 `json:"stockQty"`
	TrackStock bool   `json:"trackStock"`
}

type stockUpdate struct {
	MenuID     string           `json:"menuId"`
	StockQty   *int32           `json:"stockQty"`
	TrackStock bool             `json:"trackStock"`
	AddonItems []stockAddonItem `json:"addonItems,omitempty"`
}

const (
	stockKeepAliveInterval = 30 * time.Second
	stockCheckInterval     = 5 * time.Second
)

func (h *Handler) PublicMerchantStockStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var merchantID int64
	var merchantActive bool
	if err := h.DB.QueryRow(ctx, `select id, is_active from merchants where code = $1`, merchantCode).Scan(&merchantID, &merchantActive); err != nil || !merchantActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	previous := make(map[string]*int32)

	initialStock, err := h.fetchStockData(ctx, merchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch stock data")
		return
	}

	for _, item := range initialStock {
		previous[fmt.Sprintf("menu_%s", item.MenuID)] = item.StockQty
		for _, addon := range item.AddonItems {
			previous[fmt.Sprintf("addon_%s", addon.ID)] = addon.StockQty
		}
	}

	if payload, err := json.Marshal(initialStock); err == nil {
		_, _ = fmt.Fprintf(w, "event: initial\ndata: %s\n\n", payload)
		flusher.Flush()
	}

	keepAliveTicker := time.NewTicker(stockKeepAliveInterval)
	stockTicker := time.NewTicker(stockCheckInterval)
	defer keepAliveTicker.Stop()
	defer stockTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAliveTicker.C:
			_, _ = fmt.Fprintf(w, ": keep-alive\n\n")
			flusher.Flush()
		case <-stockTicker.C:
			changes, err := h.getStockChanges(ctx, merchantID, previous)
			if err != nil {
				continue
			}
			if len(changes) > 0 {
				if payload, err := json.Marshal(changes); err == nil {
					_, _ = fmt.Fprintf(w, "event: stock-update\ndata: %s\n\n", payload)
					flusher.Flush()
				}
			}
		}
	}
}

func (h *Handler) fetchStockData(ctx context.Context, merchantID int64) ([]stockUpdate, error) {
	rows, err := h.DB.Query(ctx, `
		select id, stock_qty, track_stock
		from menus
		where merchant_id = $1 and is_active = true and deleted_at is null and track_stock = true
	`, merchantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	updates := make([]stockUpdate, 0)
	menuIDs := make([]int64, 0)
	for rows.Next() {
		var id int64
		var stockQty pgtype.Int4
		var trackStock bool
		if err := rows.Scan(&id, &stockQty, &trackStock); err != nil {
			continue
		}
		var qty *int32
		if stockQty.Valid {
			value := stockQty.Int32
			qty = &value
		}
		updates = append(updates, stockUpdate{
			MenuID:     strconv.FormatInt(id, 10),
			StockQty:   qty,
			TrackStock: trackStock,
		})
		menuIDs = append(menuIDs, id)
	}

	if len(menuIDs) == 0 {
		return updates, nil
	}

	addonRows, err := h.DB.Query(ctx, `
		select mac.menu_id, ai.id, ai.stock_qty, ai.track_stock
		from menu_addon_categories mac
		join addon_categories ac on ac.id = mac.addon_category_id
		join addon_items ai on ai.addon_category_id = ac.id
		where mac.menu_id = any($1)
		  and ai.is_active = true
		  and ai.deleted_at is null
		  and ai.track_stock = true
	`, menuIDs)
	if err != nil {
		return updates, nil
	}
	defer addonRows.Close()

	menuIndex := make(map[int64]int)
	for i, item := range updates {
		menuID, _ := strconv.ParseInt(item.MenuID, 10, 64)
		menuIndex[menuID] = i
	}

	for addonRows.Next() {
		var menuID int64
		var addonID int64
		var stockQty pgtype.Int4
		var trackStock bool
		if err := addonRows.Scan(&menuID, &addonID, &stockQty, &trackStock); err != nil {
			continue
		}
		idx, ok := menuIndex[menuID]
		if !ok {
			continue
		}
		var qty *int32
		if stockQty.Valid {
			value := stockQty.Int32
			qty = &value
		}
		updates[idx].AddonItems = append(updates[idx].AddonItems, stockAddonItem{
			ID:         strconv.FormatInt(addonID, 10),
			StockQty:   qty,
			TrackStock: trackStock,
		})
	}

	return updates, nil
}

func (h *Handler) getStockChanges(ctx context.Context, merchantID int64, previous map[string]*int32) ([]stockUpdate, error) {
	current, err := h.fetchStockData(ctx, merchantID)
	if err != nil {
		return nil, err
	}

	changes := make([]stockUpdate, 0)
	for _, item := range current {
		menuKey := fmt.Sprintf("menu_%s", item.MenuID)
		if !stockQtyEqual(previous[menuKey], item.StockQty) {
			changes = append(changes, item)
			previous[menuKey] = item.StockQty
		}
		for _, addon := range item.AddonItems {
			addonKey := fmt.Sprintf("addon_%s", addon.ID)
			if !stockQtyEqual(previous[addonKey], addon.StockQty) {
				if !containsMenuChange(changes, item.MenuID) {
					changes = append(changes, item)
				}
				previous[addonKey] = addon.StockQty
			}
		}
	}

	return changes, nil
}

func stockQtyEqual(a, b *int32) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func containsMenuChange(changes []stockUpdate, menuID string) bool {
	for _, item := range changes {
		if item.MenuID == menuID {
			return true
		}
	}
	return false
}
