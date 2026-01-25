package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func (h *Handler) acceptOrderIfPendingAfterPayment(ctx context.Context, tx pgx.Tx, orderID int64, now time.Time) error {
	var (
		status        string
		isScheduled   bool
		stockDeducted pgtype.Timestamptz
	)
	if err := tx.QueryRow(ctx, `
		select status, is_scheduled, stock_deducted_at
		from orders
		where id = $1
	`, orderID).Scan(&status, &isScheduled, &stockDeducted); err != nil {
		return err
	}

	if status != "PENDING" {
		return nil
	}

	if isScheduled && !stockDeducted.Valid {
		if err := h.deductStockForScheduledOrder(ctx, tx, orderID, now); err != nil {
			return err
		}
	}

	_, err := tx.Exec(ctx, `update orders set status = 'ACCEPTED' where id = $1`, orderID)
	return err
}

func (h *Handler) deductStockForScheduledOrder(ctx context.Context, tx pgx.Tx, orderID int64, now time.Time) error {
	itemRows, err := tx.Query(ctx, `select menu_id, quantity from order_items where order_id = $1`, orderID)
	if err != nil {
		return err
	}
	defer itemRows.Close()

	menuRequired := make(map[int64]int)
	for itemRows.Next() {
		var menuID pgtype.Int8
		var quantity int32
		if err := itemRows.Scan(&menuID, &quantity); err != nil {
			return err
		}
		if menuID.Valid {
			menuRequired[menuID.Int64] += int(quantity)
		}
	}

	menuInfo := make(map[int64]struct {
		Name       string
		TrackStock bool
		StockQty   pgtype.Int4
	})
	if len(menuRequired) > 0 {
		ids := make([]int64, 0, len(menuRequired))
		for id := range menuRequired {
			ids = append(ids, id)
		}
		rows, err := tx.Query(ctx, `select id, name, track_stock, stock_qty from menus where id = any($1)`, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var name string
			var trackStock bool
			var stockQty pgtype.Int4
			if err := rows.Scan(&id, &name, &trackStock, &stockQty); err != nil {
				return err
			}
			menuInfo[id] = struct {
				Name       string
				TrackStock bool
				StockQty   pgtype.Int4
			}{Name: name, TrackStock: trackStock, StockQty: stockQty}
		}
	}

	for id, qty := range menuRequired {
		info, ok := menuInfo[id]
		if !ok {
			return fmt.Errorf("Menu item not found")
		}
		if info.TrackStock && info.StockQty.Valid && int(info.StockQty.Int32) < qty {
			return fmt.Errorf("Insufficient stock for %s", info.Name)
		}
		if info.TrackStock {
			res, err := tx.Exec(ctx, `
				update menus
				set stock_qty = stock_qty - $1
				where id = $2 and track_stock = true and stock_qty >= $1
			`, qty, id)
			if err != nil {
				return err
			}
			if res.RowsAffected() != 1 {
				return fmt.Errorf("Insufficient stock for %s", info.Name)
			}
		}
	}

	addonRows, err := tx.Query(ctx, `
		select oia.addon_item_id, oia.quantity
		from order_item_addons oia
		join order_items oi on oi.id = oia.order_item_id
		where oi.order_id = $1
	`, orderID)
	if err != nil {
		return err
	}
	defer addonRows.Close()

	addonRequired := make(map[int64]int)
	for addonRows.Next() {
		var addonID pgtype.Int8
		var quantity int32
		if err := addonRows.Scan(&addonID, &quantity); err != nil {
			return err
		}
		if addonID.Valid {
			addonRequired[addonID.Int64] += int(quantity)
		}
	}

	addonInfo := make(map[int64]struct {
		Name       string
		TrackStock bool
		StockQty   pgtype.Int4
	})
	if len(addonRequired) > 0 {
		ids := make([]int64, 0, len(addonRequired))
		for id := range addonRequired {
			ids = append(ids, id)
		}
		rows, err := tx.Query(ctx, `select id, name, track_stock, stock_qty from addon_items where id = any($1)`, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var name string
			var trackStock bool
			var stockQty pgtype.Int4
			if err := rows.Scan(&id, &name, &trackStock, &stockQty); err != nil {
				return err
			}
			addonInfo[id] = struct {
				Name       string
				TrackStock bool
				StockQty   pgtype.Int4
			}{Name: name, TrackStock: trackStock, StockQty: stockQty}
		}
	}

	for id, qty := range addonRequired {
		info, ok := addonInfo[id]
		if !ok {
			return fmt.Errorf("Add-on item not found")
		}
		if info.TrackStock && info.StockQty.Valid && int(info.StockQty.Int32) < qty {
			return fmt.Errorf("Insufficient stock for %s", info.Name)
		}
		if info.TrackStock {
			res, err := tx.Exec(ctx, `
				update addon_items
				set stock_qty = stock_qty - $1
				where id = $2 and track_stock = true and stock_qty >= $1
			`, qty, id)
			if err != nil {
				return err
			}
			if res.RowsAffected() != 1 {
				return fmt.Errorf("Insufficient stock for %s", info.Name)
			}
		}
	}

	for id := range menuRequired {
		var stockQty pgtype.Int4
		if err := tx.QueryRow(ctx, `select stock_qty from menus where id = $1`, id).Scan(&stockQty); err == nil {
			if stockQty.Valid {
				_, _ = tx.Exec(ctx, `update menus set is_active = $1 where id = $2`, stockQty.Int32 > 0, id)
			}
		}
	}
	for id := range addonRequired {
		var stockQty pgtype.Int4
		if err := tx.QueryRow(ctx, `select stock_qty from addon_items where id = $1`, id).Scan(&stockQty); err == nil {
			if stockQty.Valid {
				_, _ = tx.Exec(ctx, `update addon_items set is_active = $1 where id = $2`, stockQty.Int32 > 0, id)
			}
		}
	}

	_, err = tx.Exec(ctx, `update orders set stock_deducted_at = $1 where id = $2`, now, orderID)
	return err
}
