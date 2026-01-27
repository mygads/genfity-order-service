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

	if cfg.Env == "development" || len(cfg.CorsAllowedOrigins) > 0 {
		options := cors.Options{
			AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
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
	})

	r.Route("/api/merchant", func(r chi.Router) {
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
		r.Put("/orders/{orderId}/status", h.MerchantUpdateOrderStatus)
		r.Get("/orders/resolve", h.MerchantResolveOrder)
		r.Get("/reservations", h.MerchantReservationsList)
		r.Get("/reservations/active", h.MerchantReservationsActive)
		r.Get("/reservations/count", h.MerchantReservationCount)
		r.Get("/reservations/{reservationId}/preorder", h.MerchantReservationPreorder)
		r.Put("/reservations/{reservationId}/accept", h.MerchantReservationAccept)
		r.Put("/reservations/{reservationId}/cancel", h.MerchantReservationCancel)
		r.Get("/customers/search", h.MerchantCustomerSearch)
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
