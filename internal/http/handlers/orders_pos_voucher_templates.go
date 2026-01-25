package handlers

import (
	"net/http"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type POSVoucherTemplate struct {
	ID                int64      `json:"id"`
	Name              string     `json:"name"`
	Description       *string    `json:"description,omitempty"`
	DiscountType      string     `json:"discountType"`
	DiscountValue     float64    `json:"discountValue"`
	MaxDiscountAmount *float64   `json:"maxDiscountAmount,omitempty"`
	MinOrderAmount    *float64   `json:"minOrderAmount,omitempty"`
	ValidFrom         *time.Time `json:"validFrom,omitempty"`
	ValidUntil        *time.Time `json:"validUntil,omitempty"`
	IncludeAllItems   bool       `json:"includeAllItems"`
	ReportCategory    *string    `json:"reportCategory,omitempty"`
}

func (h *Handler) MerchantPOSVoucherTemplates(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant context not found")
		return
	}

	now := time.Now()
	rows, err := h.DB.Query(ctx, `
		select id, name, description, discount_type, discount_value,
		       max_discount_amount, min_order_amount, valid_from, valid_until,
		       include_all_items, report_category
		from order_voucher_templates
		where merchant_id = $1
		  and is_active = true
		  and audience in ('POS', 'BOTH')
		  and (valid_from is null or valid_from <= $2)
		  and (valid_until is null or valid_until >= $2)
		order by created_at desc
	`, *authCtx.MerchantID, now)
	if err != nil {
		h.Logger.Error("pos voucher templates query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch voucher templates")
		return
	}
	defer rows.Close()

	templates := make([]POSVoucherTemplate, 0)
	for rows.Next() {
		var (
			id             int64
			name           string
			description    pgtype.Text
			discountType   string
			discountValue  pgtype.Numeric
			maxDiscount    pgtype.Numeric
			minOrder       pgtype.Numeric
			validFrom      pgtype.Timestamptz
			validUntil     pgtype.Timestamptz
			includeAll     bool
			reportCategory pgtype.Text
		)

		if err := rows.Scan(&id, &name, &description, &discountType, &discountValue, &maxDiscount, &minOrder, &validFrom, &validUntil, &includeAll, &reportCategory); err != nil {
			h.Logger.Error("pos voucher templates scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to fetch voucher templates")
			return
		}

		template := POSVoucherTemplate{
			ID:              id,
			Name:            name,
			DiscountType:    discountType,
			DiscountValue:   utils.NumericToFloat64(discountValue),
			IncludeAllItems: includeAll,
		}

		if description.Valid {
			template.Description = &description.String
		}
		if maxDiscount.Valid {
			value := utils.NumericToFloat64(maxDiscount)
			template.MaxDiscountAmount = &value
		}
		if minOrder.Valid {
			value := utils.NumericToFloat64(minOrder)
			template.MinOrderAmount = &value
		}
		if validFrom.Valid {
			value := validFrom.Time
			template.ValidFrom = &value
		}
		if validUntil.Valid {
			value := validUntil.Time
			template.ValidUntil = &value
		}
		if reportCategory.Valid {
			template.ReportCategory = &reportCategory.String
		}

		templates = append(templates, template)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       templates,
		"statusCode": 200,
	})
}
