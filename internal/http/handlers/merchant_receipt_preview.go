package handlers

import "net/http"

func (h *Handler) MerchantReceiptPreviewPDF(w http.ResponseWriter, r *http.Request) {
	h.MerchantProxy(w, r)
}

func (h *Handler) MerchantReceiptPreviewHTML(w http.ResponseWriter, r *http.Request) {
	h.MerchantProxy(w, r)
}
