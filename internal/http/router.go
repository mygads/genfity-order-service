package httpapi

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"time"

	"genfity-order-services/internal/config"
	"genfity-order-services/internal/http/handlers"
	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/queue"
	"genfity-order-services/internal/ws"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func NewRouter(db *pgxpool.Pool, logger *zap.Logger, cfg config.Config, queueClient *queue.Client, wsServer *ws.Server) http.Handler {
	r := chi.NewRouter()
	r.Use(requestLogger(logger))
	r.Use(middleware.RequestID())
	r.Use(middleware.Telemetry(logger))

	if cfg.Env == "development" || len(cfg.CorsAllowedOrigins) > 0 {
		options := cors.Options{
			AllowedMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowedHeaders: []string{
				"Accept",
				"Authorization",
				"Content-Type",
				"X-Requested-With",
				"X-Csrf-Token",
				"Api-Key",
				"Cache-Control",
				"Pragma",
				"Dnt",
				"Last-Event-ID",
			},
			AllowCredentials: true,
			MaxAge:           300,
		}

		if cfg.Env == "development" {
			options.AllowOriginFunc = func(_ *http.Request, origin string) bool {
				return true
			}
		} else {
			options.AllowedOrigins = cfg.CorsAllowedOrigins
		}

		r.Use(cors.Handler(options))
	}

	h := &handlers.Handler{DB: db, Logger: logger, Config: cfg, Queue: queueClient}

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Route("/api/public", func(r chi.Router) {
		r.Use(setResponseHeader("X-Order-Service-Origin", "native"))
		r.Post("/orders", h.PublicOrderCreate)
		r.Get("/orders/{orderNumber}", h.PublicOrderDetail)
		r.Post("/orders/{orderNumber}/upload-proof", h.PublicOrderUploadProof)
		r.Post("/orders/{orderNumber}/confirm-payment", h.PublicOrderConfirmPayment)
		r.Get("/orders/{orderNumber}/wait-time", h.PublicOrderWaitTime)
		r.Get("/orders/{orderNumber}/group-details", h.PublicOrderGroupDetails)
		r.Get("/orders/{orderNumber}/feedback", h.PublicOrderFeedbackGet)
		r.Post("/orders/{orderNumber}/feedback", h.PublicOrderFeedbackCreate)
		r.Get("/geocode/forward", h.PublicGeocodeForward)
		r.Get("/geocode/reverse", h.PublicGeocodeReverse)
		r.Post("/vouchers/validate", h.PublicVoucherValidate)
		r.Post("/reservations", h.PublicReservationsCreate)
		r.Post("/group-order", h.PublicGroupOrderCreate)
		r.Get("/group-order/{code}", h.PublicGroupOrderSession)
		r.Delete("/group-order/{code}", h.PublicGroupOrderCancel)
		r.Post("/group-order/{code}/join", h.PublicGroupOrderJoin)
		r.Delete("/group-order/{code}/leave", h.PublicGroupOrderLeave)
		r.Put("/group-order/{code}/cart", h.PublicGroupOrderUpdateCart)
		r.Delete("/group-order/{code}/kick", h.PublicGroupOrderKick)
		r.Post("/group-order/{code}/transfer-host", h.PublicGroupOrderTransferHost)
		r.Post("/group-order/{code}/submit", h.PublicGroupOrderSubmit)
		r.Get("/menu/{merchantCode}", h.PublicMenu)
		r.Get("/merchants/{code}", h.PublicMerchant)
		r.Get("/merchants/{code}/categories", h.PublicMerchantCategories)
		r.Get("/merchants/{code}/status", h.PublicMerchantStatus)
		r.Get("/merchants/{code}/stock-stream", h.PublicMerchantStockStream)
		r.Get("/merchants/{code}/available-times", h.PublicMerchantAvailableTimes)
		r.Post("/merchants/{code}/delivery/quote", h.PublicDeliveryQuote)
		r.Get("/merchants/{code}/menus", h.PublicMerchantMenus)
		r.Get("/merchants/{code}/menus/{id}", h.PublicMerchantMenuDetail)
		r.Get("/merchants/{code}/menus/{id}/addons", h.PublicMerchantMenuAddons)
		r.Get("/merchants/{code}/menus/search", h.PublicMerchantMenuSearch)
		r.Get("/merchants/{code}/recommendations", h.PublicMerchantRecommendations)
		// Push notifications
		r.Get("/push/subscribe", h.PublicPushGetVAPIDKey)
		r.Post("/push/subscribe", h.PublicPushSubscribe)
		r.Patch("/push/subscribe", h.PublicPushAddOrder)
		r.Delete("/push/subscribe", h.PublicPushUnsubscribe)
	})

	r.Route("/api/merchant", func(r chi.Router) {
		r.Use(setResponseHeader("X-Order-Service-Origin", "native"))
		r.Use(middleware.MerchantAuth(db, cfg.JWTSecret))

		r.Get("/orders", h.MerchantOrdersList)
		r.Get("/orders/active", h.MerchantActiveOrders)
		r.Get("/orders/analytics", h.MerchantOrderAnalytics)
		r.Get("/orders/stats", h.MerchantOrderStats)
		r.Get("/orders/{orderId}", h.MerchantOrderDetailGet)
		r.Patch("/orders/{orderId}", h.MerchantOrderDetailPatch)
		r.Delete("/orders/{orderId}", h.MerchantOrderDelete)
		r.Put("/orders/{orderId}/admin-note", h.MerchantOrderAdminNote)
		r.Post("/orders/{orderId}/cancel", h.MerchantOrderCancel)
		r.Put("/orders/{orderId}/delivery/assign", h.MerchantOrderDeliveryAssign)
		r.Post("/orders/{orderId}/cod/confirm", h.MerchantOrderCashOnDeliveryConfirm)
		r.Post("/orders/{orderId}/payment", h.MerchantOrderPayment)
		r.Get("/orders/{orderId}/receipt-html", h.MerchantOrderReceiptHTML)
		r.Get("/orders/{orderId}/receipt", h.MerchantOrderReceiptPDF)
		r.Get("/orders/{orderId}/tracking-token", h.MerchantOrderTrackingToken)
		r.Get("/orders/pos/history", h.MerchantPOSOrderHistory)
		r.Get("/orders/pos/voucher-templates", h.MerchantPOSVoucherTemplates)
		r.Post("/orders/pos/validate-voucher", h.MerchantPOSValidateVoucher)
		r.Post("/orders/pos/validate-voucher-template", h.MerchantPOSValidateVoucherTemplate)
		r.Post("/orders/pos/refund", h.MerchantPOSRefund)
		r.Post("/orders/pos/payment", h.MerchantPOSPayment)
		r.Post("/orders/pos", h.MerchantPOSCreateOrder)
		r.Get("/orders/pos/{orderId}", h.MerchantPOSOrderGet)
		r.Put("/orders/pos/{orderId}", h.MerchantPOSOrderUpdate)
		r.Get("/pos/menu", h.MerchantPOSMenuGet)
		r.Get("/pos-settings", h.MerchantPOSSettingsGet)
		r.Put("/pos-settings", h.MerchantPOSSettingsPut)
		r.Put("/orders/{orderId}/status", h.MerchantUpdateOrderStatus)
		r.Get("/orders/resolve", h.MerchantResolveOrder)
		r.Get("/menu", h.MerchantMenuList)
		r.Post("/menu", h.MerchantMenuCreate)
		r.Get("/menu/{id}", h.MerchantMenuDetail)
		r.Put("/menu/{id}", h.MerchantMenuUpdate)
		r.Delete("/menu/{id}", h.MerchantMenuDelete)
		r.Post("/menu/builder", h.MerchantMenuBuilderCreate)
		r.Put("/menu/builder", h.MerchantMenuBuilderUpdate)
		r.Post("/menu/{id}/addon-categories", h.MerchantMenuAddAddonCategory)
		r.Delete("/menu/{id}/addon-categories/{categoryId}", h.MerchantMenuRemoveAddonCategory)
		r.Put("/menu/{id}/categories", h.MerchantMenuUpdateCategories)
		r.Post("/menu/{id}/duplicate", h.MerchantMenuDuplicate)
		r.Post("/menu/{id}/add-stock", h.MerchantMenuAddStock)
		r.Patch("/menu/{id}/toggle-active", h.MerchantMenuToggleActive)
		r.Post("/menu/{id}/restore", h.MerchantMenuRestore)
		r.Delete("/menu/{id}/permanent-delete", h.MerchantMenuPermanentDelete)
		r.Post("/menu/bulk", h.MerchantMenuBulk)
		r.Post("/menu/bulk-delete", h.MerchantMenuBulkDelete)
		r.Post("/menu/bulk-restore", h.MerchantMenuBulkRestore)
		r.Get("/menu/bulk-soft-delete", h.MerchantMenuBulkSoftDeleteToken)
		r.Post("/menu/bulk-soft-delete", h.MerchantMenuBulkSoftDelete)
		r.Post("/menu/bulk-update-status", h.MerchantMenuBulkUpdateStatus)
		r.Post("/menu/bulk-upload", h.MerchantMenuBulkUpload)
		r.Post("/menu/rebuild-thumbnails", h.MerchantMenuRebuildThumbnails)
		r.Post("/menu/reset-stock", h.MerchantMenuResetStock)
		r.Get("/menu/stock/overview", h.MerchantMenuStockOverview)
		r.Post("/menu/stock/bulk-update", h.MerchantMenuStockBulkUpdate)
		r.Post("/bulk/menu", h.MerchantBulkMenuLegacy)
		r.Get("/addon-categories", h.MerchantAddonCategoriesList)
		r.Post("/addon-categories", h.MerchantAddonCategoriesCreate)
		r.Get("/addon-categories/{id}", h.MerchantAddonCategoriesDetail)
		r.Put("/addon-categories/{id}", h.MerchantAddonCategoriesUpdate)
		r.Delete("/addon-categories/{id}", h.MerchantAddonCategoriesDelete)
		r.Patch("/addon-categories/{id}/toggle-active", h.MerchantAddonCategoriesToggleActive)
		r.Post("/addon-categories/{id}/restore", h.MerchantAddonCategoriesRestore)
		r.Delete("/addon-categories/{id}/permanent-delete", h.MerchantAddonCategoriesPermanentDelete)
		r.Get("/addon-categories/{id}/delete-preview", h.MerchantAddonCategoriesDeletePreview)
		r.Get("/addon-categories/{id}/items", h.MerchantAddonCategoriesItemsList)
		r.Post("/addon-categories/{id}/reorder-items", h.MerchantAddonCategoriesReorderItems)
		r.Get("/addon-categories/{id}/relationships", h.MerchantAddonCategoriesRelationships)
		r.Post("/addon-categories/bulk-delete", h.MerchantAddonCategoriesBulkDelete)
		r.Get("/addon-categories/bulk-soft-delete", h.MerchantAddonCategoriesBulkSoftDeleteToken)
		r.Post("/addon-categories/bulk-soft-delete", h.MerchantAddonCategoriesBulkSoftDelete)
		r.Get("/addon-items", h.MerchantAddonItemsList)
		r.Post("/addon-items", h.MerchantAddonItemsCreate)
		r.Get("/addon-items/{id}", h.MerchantAddonItemsDetail)
		r.Put("/addon-items/{id}", h.MerchantAddonItemsUpdate)
		r.Delete("/addon-items/{id}", h.MerchantAddonItemsDelete)
		r.Patch("/addon-items/{id}/toggle-active", h.MerchantAddonItemsToggleActive)
		r.Post("/addon-items/{id}/restore", h.MerchantAddonItemsRestore)
		r.Delete("/addon-items/{id}/permanent-delete", h.MerchantAddonItemsPermanentDelete)
		r.Get("/addon-items/bulk-soft-delete", h.MerchantAddonItemsBulkSoftDeleteToken)
		r.Post("/addon-items/bulk-soft-delete", h.MerchantAddonItemsBulkSoftDelete)
		r.Post("/addon-items/bulk-upload", h.MerchantAddonItemsBulkUpload)
		r.Post("/bulk/addon-items", h.MerchantBulkAddonItemsLegacy)
		r.Get("/categories", h.MerchantCategoriesList)
		r.Post("/categories", h.MerchantCategoriesCreate)
		r.Put("/categories/{id}", h.MerchantCategoriesUpdate)
		r.Delete("/categories/{id}", h.MerchantCategoriesDelete)
		r.Get("/categories/{id}/delete-preview", h.MerchantCategoriesDeletePreview)
		r.Get("/categories/{id}/menus", h.MerchantCategoryMenusList)
		r.Post("/categories/{id}/menus", h.MerchantCategoryMenusAdd)
		r.Delete("/categories/{id}/menus/{menuId}", h.MerchantCategoryMenusRemove)
		r.Delete("/categories/{id}/permanent-delete", h.MerchantCategoriesPermanentDelete)
		r.Post("/categories/{id}/restore", h.MerchantCategoriesRestore)
		r.Patch("/categories/{id}/toggle-active", h.MerchantCategoriesToggleActive)
		r.Post("/categories/bulk-delete", h.MerchantCategoriesBulkDelete)
		r.Get("/categories/bulk-soft-delete", h.MerchantCategoriesBulkSoftDeleteToken)
		r.Post("/categories/bulk-soft-delete", h.MerchantCategoriesBulkSoftDelete)
		r.Post("/categories/reorder", h.MerchantCategoriesReorder)
		r.Get("/reservations", h.MerchantReservationsList)
		r.Get("/reservations/active", h.MerchantReservationsActive)
		r.Get("/reservations/count", h.MerchantReservationCount)
		r.Get("/reservations/{reservationId}/preorder", h.MerchantReservationPreorder)
		r.Put("/reservations/{reservationId}/accept", h.MerchantReservationAccept)
		r.Put("/reservations/{reservationId}/cancel", h.MerchantReservationCancel)
		r.Get("/customers/search", h.MerchantCustomerSearch)
		r.Get("/staff", h.MerchantStaffList)
		r.Post("/staff", h.MerchantStaffCreate)
		r.Delete("/staff", h.MerchantStaffDelete)
		r.Post("/staff/invite", h.MerchantStaffInvite)
		r.Post("/staff/leave", h.MerchantStaffLeave)
		r.Put("/staff/{id}", h.MerchantStaffUpdate)
		r.Delete("/staff/{id}", h.MerchantStaffDelete)
		r.Get("/staff/{id}/permissions", h.MerchantStaffPermissionsGet)
		r.Put("/staff/{id}/permissions", h.MerchantStaffPermissionsUpdate)
		r.Patch("/staff/{id}/permissions", h.MerchantStaffPermissionsToggle)
		r.Get("/drivers", h.MerchantDriversList)
		r.Post("/drivers", h.MerchantDriversCreate)
		r.Patch("/drivers/{userId}", h.MerchantDriversUpdate)
		r.Delete("/drivers/{userId}", h.MerchantDriversDelete)
		r.Get("/delivery/zones", h.MerchantDeliveryZonesList)
		r.Post("/delivery/zones", h.MerchantDeliveryZonesUpsert)
		r.Delete("/delivery/zones", h.MerchantDeliveryZonesDelete)
		r.Post("/delivery/zones/bulk-import", h.MerchantDeliveryZonesBulkImport)
		r.Get("/profile", h.MerchantProfileGet)
		r.Put("/profile", h.MerchantProfilePut)
		r.Put("/opening-hours", h.MerchantOpeningHoursPut)
		r.Get("/special-hours", h.MerchantSpecialHoursGet)
		r.Post("/special-hours", h.MerchantSpecialHoursPost)
		r.Get("/special-hours/{id}", h.MerchantSpecialHoursDetailGet)
		r.Put("/special-hours/{id}", h.MerchantSpecialHoursDetailPut)
		r.Delete("/special-hours/{id}", h.MerchantSpecialHoursDetailDelete)
		r.Get("/mode-schedules", h.MerchantModeSchedulesGet)
		r.Post("/mode-schedules", h.MerchantModeSchedulesPost)
		r.Delete("/mode-schedules", h.MerchantModeSchedulesDelete)
		r.Put("/toggle-open", h.MerchantToggleOpen)
		r.Get("/subscription", h.MerchantSubscriptionGet)
		r.Get("/subscription/can-switch", h.MerchantSubscriptionCanSwitch)
		r.Get("/subscription/history", h.MerchantSubscriptionHistory)
		r.Post("/subscription/switch-type", h.MerchantSubscriptionSwitchType)
		r.Get("/balance", h.MerchantBalanceGet)
		r.Get("/balance/usage-summary", h.MerchantBalanceUsageSummary)
		r.Get("/balance/transactions", h.MerchantBalanceTransactions)
		r.Get("/balance/transactions/export", h.MerchantBalanceTransactionsExport)
		r.Post("/balance/transfer", h.MerchantBalanceTransfer)
		r.Get("/balance/group", h.MerchantBalanceGroup)
		r.Get("/branches", h.MerchantBranchesList)
		r.Post("/branches", h.MerchantBranchesCreate)
		r.Post("/branches/move", h.MerchantBranchesMove)
		r.Post("/branches/set-main", h.MerchantBranchesSetMain)
		r.Get("/payment-settings", h.MerchantPaymentSettingsGet)
		r.Put("/payment-settings", h.MerchantPaymentSettingsPut)
		r.Get("/payment-request", h.MerchantPaymentRequestList)
		r.Post("/payment-request", h.MerchantPaymentRequestCreate)
		r.Get("/payment-request/active", h.MerchantPaymentRequestActive)
		r.Post("/payment-request/{id}/confirm", h.MerchantPaymentRequestConfirm)
		r.Post("/payment-request/{id}/cancel", h.MerchantPaymentRequestCancel)
		r.Get("/setup-progress", h.MerchantSetupProgress)
		r.Post("/main", h.MerchantMainCreate)
		r.Get("/analytics/customers", h.MerchantCustomerAnalytics)
		r.Get("/analytics/menu-performance", h.MerchantMenuPerformanceAnalytics)
		r.Get("/analytics/sales", h.MerchantSalesAnalytics)
		r.Get("/revenue", h.MerchantRevenue)
		r.Get("/reports", h.MerchantReports)
		r.Get("/reports/sales-dashboard", h.MerchantReportsSalesDashboard)
		r.Get("/feedback", h.MerchantFeedbackList)
		r.Get("/feedback/analytics", h.MerchantFeedbackAnalytics)
		r.Get("/order-vouchers/analytics", h.MerchantOrderVoucherAnalytics)
		r.Get("/order-vouchers/settings", h.MerchantOrderVoucherSettingsGet)
		r.Put("/order-vouchers/settings", h.MerchantOrderVoucherSettingsUpdate)
		r.Get("/order-vouchers/templates", h.MerchantOrderVoucherTemplatesList)
		r.Post("/order-vouchers/templates", h.MerchantOrderVoucherTemplatesCreate)
		r.Get("/order-vouchers/templates/{id}", h.MerchantOrderVoucherTemplateGet)
		r.Put("/order-vouchers/templates/{id}", h.MerchantOrderVoucherTemplateUpdate)
		r.Get("/order-vouchers/templates/{id}/usage", h.MerchantOrderVoucherTemplateUsage)
		r.Get("/order-vouchers/templates/{id}/codes", h.MerchantOrderVoucherTemplateCodesList)
		r.Post("/order-vouchers/templates/{id}/codes", h.MerchantOrderVoucherTemplateCodesCreate)
		r.Put("/order-vouchers/templates/{id}/codes/{codeId}", h.MerchantOrderVoucherTemplateCodeUpdate)
		r.Delete("/order-vouchers/templates/{id}/codes/{codeId}", h.MerchantOrderVoucherTemplateCodeDelete)
		r.Get("/users", h.MerchantUsersList)
		r.Get("/deleted-items", h.MerchantDeletedItemsList)
		r.Get("/stock-photos", h.MerchantStockPhotosList)
		r.Post("/stock-photos/{id}/use", h.MerchantStockPhotoUse)
		r.Post("/vouchers/redeem", h.MerchantVouchersRedeem)
		r.Get("/menu-books", h.MerchantMenuBooksList)
		r.Post("/menu-books", h.MerchantMenuBooksCreate)
		r.Get("/menu-books/{id}", h.MerchantMenuBooksGet)
		r.Put("/menu-books/{id}", h.MerchantMenuBooksUpdate)
		r.Delete("/menu-books/{id}", h.MerchantMenuBooksDelete)
		r.Get("/special-prices", h.MerchantSpecialPricesList)
		r.Post("/special-prices", h.MerchantSpecialPricesCreate)
		r.Get("/special-prices/{id}", h.MerchantSpecialPricesDetail)
		r.Put("/special-prices/{id}", h.MerchantSpecialPricesUpdate)
		r.Delete("/special-prices/{id}", h.MerchantSpecialPricesDelete)
		r.Get("/payment/verify", h.MerchantPaymentVerify)
		r.Post("/receipt/preview", h.MerchantReceiptPreviewPDF)
		r.Post("/receipt/preview-html", h.MerchantReceiptPreviewHTML)
		r.Get("/receipt-settings", h.MerchantReceiptSettingsGet)
		r.Put("/receipt-settings", h.MerchantReceiptSettingsPut)
		r.Get("/lock-status", h.MerchantLockStatus)
		r.Put("/delete-pin", h.MerchantDeletePinSet)
		r.Delete("/delete-pin", h.MerchantDeletePinRemove)
		r.Get("/customer-display/state", h.MerchantCustomerDisplayStateGet)
		r.Put("/customer-display/state", h.MerchantCustomerDisplayStatePut)
		r.Get("/customer-display/sessions", h.MerchantCustomerDisplaySessions)
		r.Post("/upload-logo", h.MerchantUploadLogo)
		r.Post("/upload/qris", h.MerchantUploadQris)
		r.Post("/upload/merchant-image", h.MerchantUploadMerchantImage)
		r.Post("/upload/promo-banner", h.MerchantUploadPromoBanner)
		r.Post("/upload/menu-image", h.MerchantUploadMenuImage)
		r.Post("/upload/menu-image/confirm", h.MerchantMenuImageConfirm)
		r.Post("/upload/delete-image", h.MerchantDeleteImage)
		r.Post("/upload/presign", h.MerchantUploadPresign)
		r.Post("/upload/confirm", h.MerchantUploadConfirm)

		r.NotFound(h.MerchantProxy)
		r.MethodNotAllowed(h.MerchantProxy)
	})

	if wsServer != nil {
		r.Get("/ws/merchant/orders", wsServer.MerchantOrdersWS)
		r.Get("/ws/merchant/customer-display", wsServer.MerchantCustomerDisplayWS)
		r.Get("/ws/public/order", wsServer.PublicOrderWS)
		r.Get("/ws/public/group-order", wsServer.PublicGroupOrderWS)
	}

	return r
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hj.Hijack()
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func requestLogger(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			logger.Info("",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", rec.status),
				zap.Duration("duration", time.Since(start)),
				// zap.String("origin", r.Header.Get("Origin")),
				// zap.String("userAgent", r.UserAgent()),
			)
		})
	}
}

func setResponseHeader(name string, value string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(name, value)
			next.ServeHTTP(w, r)
		})
	}
}
