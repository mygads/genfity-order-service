package handlers

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"genfity-order-services/pkg/response"
)

func (h *Handler) MerchantProxy(w http.ResponseWriter, r *http.Request) {
	baseURL := strings.TrimSpace(h.Config.NextApiBaseURL)
	if baseURL == "" {
		response.Error(w, http.StatusBadRequest, "NEXT_API_BASE_URL_REQUIRED", "NEXT_API_BASE_URL is not configured")
		return
	}

	target, err := url.Parse(baseURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		response.Error(w, http.StatusBadRequest, "NEXT_API_BASE_URL_INVALID", "NEXT_API_BASE_URL is invalid")
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		response.Error(rw, http.StatusBadGateway, "UPSTREAM_ERROR", "Failed to reach Next.js API")
	}
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.Path != "" && target.Path != "/" {
			trimmedBase := strings.TrimSuffix(target.Path, "/")
			req.URL.Path = trimmedBase + req.URL.Path
		}
		req.Host = target.Host
	}

	w.Header().Set("X-Order-Service-Origin", "proxy")

	proxy.ServeHTTP(w, r)
}
