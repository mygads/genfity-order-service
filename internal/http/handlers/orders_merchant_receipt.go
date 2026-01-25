package handlers

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/phpdave11/gofpdf"
)

type receiptAddon struct {
	Name     string
	Quantity int32
	Price    string
	Subtotal string
}

type receiptItem struct {
	Name     string
	Quantity int32
	Unit     string
	Subtotal string
	Notes    string
	Addons   []receiptAddon
}

type receiptTemplateData struct {
	MerchantName    string
	MerchantCode    string
	MerchantAddress string
	MerchantPhone   string
	MerchantEmail   string
	Currency        string
	OrderNumber     string
	OrderType       string
	TableNumber     string
	DeliveryAddress string
	CustomerName    string
	CustomerPhone   string
	CustomerEmail   string
	PlacedAt        string
	PaidAt          string
	Items           []receiptItem
	Subtotal        string
	TaxAmount       string
	ServiceCharge   string
	PackagingFee    string
	DeliveryFee     string
	DiscountAmount  string
	DiscountLabel   string
	TotalAmount     string
	PaymentMethod   string
	PaymentStatus   string
	CashierName     string
}

const receiptHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Receipt {{.OrderNumber}}</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Courier New', monospace; font-size: 12px; padding: 12px; color: #000; }
    .header { text-align: center; border-bottom: 1px dashed #000; padding-bottom: 8px; margin-bottom: 8px; }
    .merchant-name { font-size: 16px; font-weight: bold; }
    .meta { text-align: center; margin-bottom: 8px; }
    .section { border-top: 1px dashed #999; padding-top: 6px; margin-top: 6px; }
    .row { display: flex; justify-content: space-between; margin: 2px 0; }
    .items { margin-top: 8px; }
    .item-name { font-weight: 600; }
    .addon { margin-left: 12px; font-size: 11px; color: #333; }
    .notes { margin-left: 12px; font-size: 10px; font-style: italic; color: #555; }
    .total { font-weight: bold; font-size: 14px; }
  </style>
</head>
<body>
  <div class="header">
    <div class="merchant-name">{{.MerchantName}}</div>
    {{if .MerchantAddress}}<div>{{.MerchantAddress}}</div>{{end}}
    {{if .MerchantPhone}}<div>{{.MerchantPhone}}</div>{{end}}
    {{if .MerchantEmail}}<div>{{.MerchantEmail}}</div>{{end}}
  </div>
  <div class="meta">
    <div>Order: {{.OrderNumber}}</div>
    <div>{{.OrderType}}{{if .TableNumber}} · Table {{.TableNumber}}{{end}}</div>
    {{if .DeliveryAddress}}<div>{{.DeliveryAddress}}</div>{{end}}
    <div>Placed: {{.PlacedAt}}</div>
    {{if .PaidAt}}<div>Paid: {{.PaidAt}}</div>{{end}}
  </div>
  <div class="items">
    {{range .Items}}
      <div class="row">
        <div class="item-name">{{.Quantity}} x {{.Name}}</div>
        <div>{{.Subtotal}}</div>
      </div>
      {{if .Unit}}<div class="addon">Unit: {{.Unit}}</div>{{end}}
      {{if .Notes}}<div class="notes">{{.Notes}}</div>{{end}}
      {{range .Addons}}
        <div class="row addon">
          <div>{{.Quantity}} x {{.Name}}</div>
          <div>{{.Subtotal}}</div>
        </div>
      {{end}}
    {{end}}
  </div>
  <div class="section">
    <div class="row"><div>Subtotal</div><div>{{.Subtotal}}</div></div>
    {{if .TaxAmount}}<div class="row"><div>Tax</div><div>{{.TaxAmount}}</div></div>{{end}}
    {{if .ServiceCharge}}<div class="row"><div>Service</div><div>{{.ServiceCharge}}</div></div>{{end}}
    {{if .PackagingFee}}<div class="row"><div>Packaging</div><div>{{.PackagingFee}}</div></div>{{end}}
    {{if .DeliveryFee}}<div class="row"><div>Delivery</div><div>{{.DeliveryFee}}</div></div>{{end}}
    {{if .DiscountAmount}}<div class="row"><div>Discount{{if .DiscountLabel}} ({{.DiscountLabel}}){{end}}</div><div>-{{.DiscountAmount}}</div></div>{{end}}
    <div class="row total"><div>Total</div><div>{{.TotalAmount}}</div></div>
  </div>
  <div class="section">
    {{if .PaymentMethod}}<div class="row"><div>Payment</div><div>{{.PaymentMethod}}</div></div>{{end}}
    {{if .PaymentStatus}}<div class="row"><div>Status</div><div>{{.PaymentStatus}}</div></div>{{end}}
    {{if .CashierName}}<div class="row"><div>Cashier</div><div>{{.CashierName}}</div></div>{{end}}
  </div>
</body>
</html>`

func (h *Handler) MerchantOrderReceiptHTML(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	merchantInfo, err := h.fetchMerchantReceiptInfo(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load merchant")
		return
	}

	templateData := buildReceiptTemplateData(data, merchantInfo)
	if templateData.OrderNumber == "" {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	tmpl, err := template.New("receipt").Parse(receiptHTMLTemplate)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to render receipt")
		return
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to render receipt")
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func (h *Handler) MerchantOrderReceiptPDF(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	orderID, err := readPathInt64(r, "orderId")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order ID is required")
		return
	}

	data, err := h.fetchMerchantOrderDetail(ctx, *authCtx.MerchantID, orderID)
	if err != nil {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	merchantInfo, err := h.fetchMerchantReceiptInfo(ctx, *authCtx.MerchantID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load merchant")
		return
	}

	templateData := buildReceiptTemplateData(data, merchantInfo)
	if templateData.OrderNumber == "" {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Order not found")
		return
	}

	buf, err := renderReceiptPDF(templateData)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to generate receipt")
		return
	}

	filename := fmt.Sprintf("receipt_%s_%s.pdf", sanitizeFilename(templateData.MerchantCode), sanitizeFilename(templateData.OrderNumber))
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", filename))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

type merchantReceiptInfo struct {
	Code     string
	Name     string
	Address  string
	Phone    string
	Email    string
	Currency string
}

func (h *Handler) fetchMerchantReceiptInfo(ctx context.Context, merchantID int64) (merchantReceiptInfo, error) {
	var info merchantReceiptInfo
	var (
		address  pgtype.Text
		phone    pgtype.Text
		email    pgtype.Text
		currency pgtype.Text
	)
	query := `
		select code, name, address, phone, email, currency
		from merchants
		where id = $1
	`
	if err := h.DB.QueryRow(ctx, query, merchantID).Scan(&info.Code, &info.Name, &address, &phone, &email, &currency); err != nil {
		return info, err
	}
	info.Address = defaultStringPtr(textPtr(address))
	info.Phone = defaultStringPtr(textPtr(phone))
	info.Email = defaultStringPtr(textPtr(email))
	info.Currency = defaultStringPtr(textPtr(currency))
	if info.Currency == "" {
		info.Currency = "AUD"
	}
	return info, nil
}

func buildReceiptTemplateData(orderData map[string]any, merchant merchantReceiptInfo) receiptTemplateData {
	orderNumber := toString(orderData["orderNumber"])
	placedAt := formatTime(orderData["placedAt"])
	paidAt := formatTimeFromPayment(orderData["payment"])
	items := buildReceiptItems(orderData["orderItems"], merchant.Currency)

	discountLabel := buildDiscountLabel(orderData["orderDiscounts"])

	subtotal := formatCurrency(amountFromAny(orderData["subtotal"]), merchant.Currency)
	taxAmount := formatOptionalCurrency(orderData["taxAmount"], merchant.Currency)
	serviceCharge := formatOptionalCurrency(orderData["serviceChargeAmount"], merchant.Currency)
	packagingFee := formatOptionalCurrency(orderData["packagingFeeAmount"], merchant.Currency)
	deliveryFee := formatOptionalCurrency(orderData["deliveryFeeAmount"], merchant.Currency)
	discountAmount := formatOptionalCurrency(orderData["discountAmount"], merchant.Currency)
	totalAmount := formatCurrency(amountFromAny(orderData["totalAmount"]), merchant.Currency)

	paymentMethod, paymentStatus, cashier := extractPaymentSummary(orderData["payment"])

	return receiptTemplateData{
		MerchantName:    merchant.Name,
		MerchantCode:    merchant.Code,
		MerchantAddress: merchant.Address,
		MerchantPhone:   merchant.Phone,
		MerchantEmail:   merchant.Email,
		Currency:        merchant.Currency,
		OrderNumber:     orderNumber,
		OrderType:       toString(orderData["orderType"]),
		TableNumber:     toString(orderData["tableNumber"]),
		DeliveryAddress: toString(orderData["deliveryAddress"]),
		CustomerName:    toStringFromMap(orderData, "customer", "name"),
		CustomerPhone:   toStringFromMap(orderData, "customer", "phone"),
		CustomerEmail:   toStringFromMap(orderData, "customer", "email"),
		PlacedAt:        placedAt,
		PaidAt:          paidAt,
		Items:           items,
		Subtotal:        subtotal,
		TaxAmount:       taxAmount,
		ServiceCharge:   serviceCharge,
		PackagingFee:    packagingFee,
		DeliveryFee:     deliveryFee,
		DiscountAmount:  discountAmount,
		DiscountLabel:   discountLabel,
		TotalAmount:     totalAmount,
		PaymentMethod:   paymentMethod,
		PaymentStatus:   paymentStatus,
		CashierName:     cashier,
	}
}

func buildReceiptItems(raw any, currency string) []receiptItem {
	items := make([]receiptItem, 0)
	list, ok := raw.([]map[string]any)
	if !ok {
		if rawSlice, ok := raw.([]any); ok {
			for _, item := range rawSlice {
				if m, ok := item.(map[string]any); ok {
					items = append(items, buildReceiptItem(m, currency))
				}
			}
			return items
		}
		return items
	}

	for _, item := range list {
		items = append(items, buildReceiptItem(item, currency))
	}

	return items
}

func buildReceiptItem(item map[string]any, currency string) receiptItem {
	qty := int32FromAny(item["quantity"])
	addons := buildReceiptAddons(item["addons"], currency)
	return receiptItem{
		Name:     toString(item["menuName"]),
		Quantity: qty,
		Unit:     formatOptionalCurrency(item["menuPrice"], currency),
		Subtotal: formatCurrency(amountFromAny(item["subtotal"]), currency),
		Notes:    toString(item["notes"]),
		Addons:   addons,
	}
}

func buildReceiptAddons(raw any, currency string) []receiptAddon {
	addons := make([]receiptAddon, 0)
	if raw == nil {
		return addons
	}
	if list, ok := raw.([]map[string]any); ok {
		for _, addon := range list {
			addons = append(addons, buildReceiptAddon(addon, currency))
		}
		return addons
	}
	if list, ok := raw.([]any); ok {
		for _, addon := range list {
			if m, ok := addon.(map[string]any); ok {
				addons = append(addons, buildReceiptAddon(m, currency))
			}
		}
	}
	return addons
}

func buildReceiptAddon(addon map[string]any, currency string) receiptAddon {
	qty := int32FromAny(addon["quantity"])
	return receiptAddon{
		Name:     toString(addon["addonName"]),
		Quantity: qty,
		Price:    formatOptionalCurrency(addon["addonPrice"], currency),
		Subtotal: formatCurrency(amountFromAny(addon["subtotal"]), currency),
	}
}

func buildDiscountLabel(raw any) string {
	labels := make([]string, 0)
	if list, ok := raw.([]map[string]any); ok {
		for _, item := range list {
			label := strings.TrimSpace(toString(item["label"]))
			if label != "" {
				labels = append(labels, label)
			}
		}
	} else if list, ok := raw.([]any); ok {
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				label := strings.TrimSpace(toString(m["label"]))
				if label != "" {
					labels = append(labels, label)
				}
			}
		}
	}
	return strings.Join(labels, " + ")
}

func extractPaymentSummary(raw any) (string, string, string) {
	if raw == nil {
		return "", "", ""
	}
	if m, ok := raw.(map[string]any); ok {
		method := toString(m["paymentMethod"])
		status := toString(m["status"])
		cashier := ""
		if paidBy, ok := m["paidBy"].(map[string]any); ok {
			cashier = toString(paidBy["name"])
		}
		return method, status, cashier
	}
	return "", "", ""
}

func formatOptionalCurrency(value any, currency string) string {
	amount := amountFromAny(value)
	if amount <= 0 {
		return ""
	}
	return formatCurrency(amount, currency)
}

func formatCurrency(amount float64, currency string) string {
	if strings.EqualFold(currency, "IDR") {
		return fmt.Sprintf("Rp%.0f", amount)
	}
	return fmt.Sprintf("%s %.2f", currency, amount)
}

func amountFromAny(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int:
		return float64(v)
	default:
		return 0
	}
}

func int32FromAny(value any) int32 {
	switch v := value.(type) {
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	case float64:
		return int32(v)
	default:
		return 0
	}
}

func formatTime(value any) string {
	switch v := value.(type) {
	case time.Time:
		return v.Format("2006-01-02 15:04")
	case *time.Time:
		if v != nil {
			return v.Format("2006-01-02 15:04")
		}
	}
	return ""
}

func formatTimeFromPayment(raw any) string {
	if raw == nil {
		return ""
	}
	if m, ok := raw.(map[string]any); ok {
		return formatTime(m["paidAt"])
	}
	return ""
}

func toStringFromMap(parent map[string]any, key string, subKey string) string {
	child, ok := parent[key].(map[string]any)
	if !ok {
		return ""
	}
	return toString(child[subKey])
}

func sanitizeFilename(value string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9_-]+`)
	clean := re.ReplaceAllString(value, "_")
	return strings.Trim(clean, "_")
}

func renderReceiptPDF(data receiptTemplateData) (*bytes.Buffer, error) {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetMargins(12, 12, 12)
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 14)
	pdf.CellFormat(0, 8, data.MerchantName, "", 1, "C", false, 0, "")

	pdf.SetFont("Arial", "", 10)
	if data.MerchantAddress != "" {
		pdf.CellFormat(0, 5, data.MerchantAddress, "", 1, "C", false, 0, "")
	}
	if data.MerchantPhone != "" {
		pdf.CellFormat(0, 5, data.MerchantPhone, "", 1, "C", false, 0, "")
	}
	if data.MerchantEmail != "" {
		pdf.CellFormat(0, 5, data.MerchantEmail, "", 1, "C", false, 0, "")
	}

	pdf.Ln(2)
	pdf.SetFont("Arial", "B", 11)
	pdf.CellFormat(0, 6, fmt.Sprintf("Order %s", data.OrderNumber), "", 1, "C", false, 0, "")
	pdf.SetFont("Arial", "", 9)
	pdf.CellFormat(0, 5, fmt.Sprintf("%s", data.OrderType), "", 1, "C", false, 0, "")
	if data.TableNumber != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Table %s", data.TableNumber), "", 1, "C", false, 0, "")
	}
	if data.DeliveryAddress != "" {
		pdf.MultiCell(0, 4, data.DeliveryAddress, "", "C", false)
	}
	if data.PlacedAt != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Placed: %s", data.PlacedAt), "", 1, "C", false, 0, "")
	}
	if data.PaidAt != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Paid: %s", data.PaidAt), "", 1, "C", false, 0, "")
	}

	pdf.Ln(3)
	pdf.SetFont("Arial", "B", 10)
	pdf.CellFormat(0, 6, "Items", "B", 1, "L", false, 0, "")
	pdf.SetFont("Arial", "", 9)
	for _, item := range data.Items {
		pdf.CellFormat(0, 5, fmt.Sprintf("%dx %s", item.Quantity, item.Name), "", 1, "L", false, 0, "")
		pdf.CellFormat(0, 5, fmt.Sprintf("Subtotal: %s", item.Subtotal), "", 1, "L", false, 0, "")
		if item.Notes != "" {
			pdf.MultiCell(0, 4, fmt.Sprintf("Notes: %s", item.Notes), "", "L", false)
		}
		for _, addon := range item.Addons {
			pdf.CellFormat(0, 4, fmt.Sprintf("  %dx %s (%s)", addon.Quantity, addon.Name, addon.Subtotal), "", 1, "L", false, 0, "")
		}
		pdf.Ln(1)
	}

	pdf.Ln(2)
	pdf.SetFont("Arial", "B", 10)
	pdf.CellFormat(0, 6, "Totals", "B", 1, "L", false, 0, "")
	pdf.SetFont("Arial", "", 9)
	pdf.CellFormat(0, 5, fmt.Sprintf("Subtotal: %s", data.Subtotal), "", 1, "L", false, 0, "")
	if data.TaxAmount != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Tax: %s", data.TaxAmount), "", 1, "L", false, 0, "")
	}
	if data.ServiceCharge != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Service: %s", data.ServiceCharge), "", 1, "L", false, 0, "")
	}
	if data.PackagingFee != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Packaging: %s", data.PackagingFee), "", 1, "L", false, 0, "")
	}
	if data.DeliveryFee != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Delivery: %s", data.DeliveryFee), "", 1, "L", false, 0, "")
	}
	if data.DiscountAmount != "" {
		label := "Discount"
		if data.DiscountLabel != "" {
			label = fmt.Sprintf("Discount (%s)", data.DiscountLabel)
		}
		pdf.CellFormat(0, 5, fmt.Sprintf("%s: -%s", label, data.DiscountAmount), "", 1, "L", false, 0, "")
	}
	pdf.SetFont("Arial", "B", 11)
	pdf.CellFormat(0, 6, fmt.Sprintf("Total: %s", data.TotalAmount), "", 1, "L", false, 0, "")

	pdf.Ln(2)
	pdf.SetFont("Arial", "", 9)
	if data.PaymentMethod != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Payment: %s", data.PaymentMethod), "", 1, "L", false, 0, "")
	}
	if data.PaymentStatus != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Status: %s", data.PaymentStatus), "", 1, "L", false, 0, "")
	}
	if data.CashierName != "" {
		pdf.CellFormat(0, 5, fmt.Sprintf("Cashier: %s", data.CashierName), "", 1, "L", false, 0, "")
	}

	var out bytes.Buffer
	if err := pdf.Output(&out); err != nil {
		return nil, err
	}
	return &out, nil
}
