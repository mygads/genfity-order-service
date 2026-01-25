package auth

import "strings"

type StaffPermission string

const (
	PermOrders           StaffPermission = "orders"
	PermCustomerDisplay  StaffPermission = "customer_display"
	PermMenu             StaffPermission = "menu"
	PermMenuStock        StaffPermission = "menu_stock"
	PermMenuBuilder      StaffPermission = "menu_builder"
	PermCategories       StaffPermission = "categories"
	PermAddonCategories  StaffPermission = "addon_categories"
	PermAddonItems       StaffPermission = "addon_items"
	PermMenuBooks        StaffPermission = "menu_books"
	PermSpecialPrices    StaffPermission = "special_prices"
	PermOrderVouchers    StaffPermission = "order_vouchers"
	PermCustomerFeedback StaffPermission = "customer_feedback"
	PermReports          StaffPermission = "reports"
	PermRevenue          StaffPermission = "revenue"
	PermMerchantSettings StaffPermission = "merchant_settings"
	PermStoreToggleOpen  StaffPermission = "store_toggle_open"
	PermSubscription     StaffPermission = "subscription"
)

var apiPermissionMap = map[string]StaffPermission{
	"/api/merchant/orders":           PermOrders,
	"/api/merchant/reservations":     PermOrders,
	"/api/merchant/drivers":          PermOrders,
	"/api/merchant/pos":              PermOrders,
	"/api/merchant/customer-display": PermCustomerDisplay,
	"/api/merchant/menu":             PermMenu,
	"/api/merchant/menu/stock":       PermMenuStock,
	"/api/merchant/menu/builder":     PermMenuBuilder,
	"/api/merchant/menu/bulk":        PermMenu,
	"/api/merchant/deleted-items":    PermMenu,
	"/api/merchant/stock-photos":     PermMenu,
	"/api/merchant/categories":       PermCategories,
	"/api/merchant/addon-categories": PermAddonCategories,
	"/api/merchant/addon-items":      PermAddonItems,
	"/api/merchant/bulk/addon-items": PermAddonItems,
	"/api/merchant/bulk/menu":        PermMenu,
	"/api/merchant/menu-books":       PermMenuBooks,
	"/api/merchant/special-prices":   PermSpecialPrices,
	"/api/merchant/order-vouchers":   PermOrderVouchers,
	"/api/merchant/feedback":         PermCustomerFeedback,
	"/api/merchant/analytics":        PermRevenue,
	"/api/merchant/reports":          PermReports,
	"/api/merchant/revenue":          PermRevenue,
	"PUT /api/merchant/profile":      PermMerchantSettings,
	"/api/merchant/opening-hours":    PermMerchantSettings,
	"/api/merchant/special-hours":    PermMerchantSettings,
	"/api/merchant/mode-schedules":   PermMerchantSettings,
	"/api/merchant/toggle-open":      PermStoreToggleOpen,
	"/api/merchant/subscription":     PermSubscription,
}

func GetPermissionForAPI(path string, method string) *StaffPermission {
	method = strings.ToUpper(strings.TrimSpace(method))

	var bestPath string
	var bestPerm *StaffPermission
	var bestMethodSpecific bool

	for key, perm := range apiPermissionMap {
		keyMethod := ""
		keyPath := key
		methodSpecific := false
		if strings.Contains(key, " ") {
			parts := strings.SplitN(key, " ", 2)
			keyMethod = strings.ToUpper(strings.TrimSpace(parts[0]))
			keyPath = strings.TrimSpace(parts[1])
			methodSpecific = true
			if method == "" || method != keyMethod {
				continue
			}
		}

		if !strings.HasPrefix(path, keyPath) {
			continue
		}

		if bestPerm == nil || len(keyPath) > len(bestPath) || (len(keyPath) == len(bestPath) && methodSpecific && !bestMethodSpecific) {
			bestPath = keyPath
			bestMethodSpecific = methodSpecific
			permCopy := perm
			bestPerm = &permCopy
		}
	}

	return bestPerm
}
