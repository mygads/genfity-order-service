package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"genfity-order-services/pkg/response"
)

type geocodeCachedValue struct {
	Data      any
	ExpiresAt time.Time
}

type geocodeRateLimitBucket struct {
	Count   int
	ResetAt time.Time
}

var (
	geocodeCacheMu   sync.Mutex
	geocodeCache     = map[string]geocodeCachedValue{}
	geocodeRateMu    sync.Mutex
	geocodeRateLimit = map[string]geocodeRateLimitBucket{}
)

const (
	geocodeCacheTTL     = 24 * time.Hour
	geocodeRateWindow   = 60 * time.Second
	geocodeRateLimitMax = 30
)

func (h *Handler) PublicGeocodeReverse(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if limited, retryAfter := geocodeIsRateLimited(clientIP(r)); limited {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		response.JSON(w, http.StatusTooManyRequests, map[string]any{
			"success":    false,
			"error":      "RATE_LIMITED",
			"message":    "Too many requests. Please try again shortly.",
			"statusCode": 429,
		})
		return
	}

	lat, okLat := parseFloatQuery(r, "lat")
	lng, okLng := parseFloatQuery(r, "lng")
	if !okLat || !okLng {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "lat and lng are required")
		return
	}

	cacheKey := roundCoord(lat) + "," + roundCoord(lng)
	if data, ok := geocodeGetCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       data,
			"cached":     true,
			"statusCode": 200,
		})
		return
	}

	endpoint := "https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=" + urlQueryFloat(lat) + "&lon=" + urlQueryFloat(lng) + "&zoom=18&addressdetails=1"
	resBody, err := fetchJSON(ctx, endpoint)
	if err != nil {
		response.Error(w, http.StatusBadGateway, "UPSTREAM_ERROR", "Failed to resolve address")
		return
	}

	address := getMap(resBody, "address")
	parts := buildAddressParts(address)
	formatted := buildFormattedAddress(parts)
	displayName := strings.TrimSpace(getString(resBody, "display_name"))

	data := map[string]any{
		"displayName":      displayName,
		"address":          address,
		"formattedAddress": defaultString(formatted, displayName),
		"parts":            parts,
	}

	geocodeSetCache(cacheKey, data)

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       data,
		"cached":     false,
		"statusCode": 200,
	})
}

func (h *Handler) PublicGeocodeForward(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if limited, retryAfter := geocodeIsRateLimited(clientIP(r)); limited {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		response.JSON(w, http.StatusTooManyRequests, map[string]any{
			"success":    false,
			"error":      "RATE_LIMITED",
			"message":    "Too many requests. Please try again shortly.",
			"statusCode": 429,
		})
		return
	}

	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "q is required")
		return
	}

	cacheKey := strings.ToLower(q)
	if data, ok := geocodeGetCache(cacheKey); ok {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"data":       data,
			"cached":     true,
			"statusCode": 200,
		})
		return
	}

	endpoint := "https://nominatim.openstreetmap.org/search?format=jsonv2&addressdetails=1&limit=5&q=" + urlQueryEscape(q)
	resBody, err := fetchJSONArray(ctx, endpoint)
	if err != nil {
		response.Error(w, http.StatusBadGateway, "UPSTREAM_ERROR", "Failed to search address")
		return
	}

	results := make([]map[string]any, 0)
	for _, row := range resBody {
		rowMap, ok := row.(map[string]any)
		if !ok {
			continue
		}
		lat, okLat := parseFloat(rowMap["lat"])
		lng, okLng := parseFloat(rowMap["lon"])
		if !okLat || !okLng {
			continue
		}

		address := getMap(rowMap, "address")
		parts := buildAddressParts(address)
		formatted := buildFormattedAddress(parts)
		displayName := strings.TrimSpace(getString(rowMap, "display_name"))

		results = append(results, map[string]any{
			"lat":              lat,
			"lng":              lng,
			"displayName":      displayName,
			"formattedAddress": defaultString(formatted, displayName),
			"parts":            parts,
			"raw":              address,
		})
	}

	data := map[string]any{
		"query":   q,
		"results": results,
	}
	geocodeSetCache(cacheKey, data)

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       data,
		"cached":     false,
		"statusCode": 200,
	})
}

func geocodeGetCache(key string) (any, bool) {
	geocodeCacheMu.Lock()
	defer geocodeCacheMu.Unlock()
	if cached, ok := geocodeCache[key]; ok {
		if cached.ExpiresAt.After(time.Now()) {
			return cached.Data, true
		}
		delete(geocodeCache, key)
	}
	return nil, false
}

func geocodeSetCache(key string, data any) {
	geocodeCacheMu.Lock()
	defer geocodeCacheMu.Unlock()
	geocodeCache[key] = geocodeCachedValue{Data: data, ExpiresAt: time.Now().Add(geocodeCacheTTL)}
}

func geocodeIsRateLimited(ip string) (bool, string) {
	geocodeRateMu.Lock()
	defer geocodeRateMu.Unlock()
	if ip == "" {
		ip = "unknown"
	}

	now := time.Now()
	bucket, ok := geocodeRateLimit[ip]
	if !ok || now.After(bucket.ResetAt) {
		geocodeRateLimit[ip] = geocodeRateLimitBucket{Count: 1, ResetAt: now.Add(geocodeRateWindow)}
		return false, ""
	}

	if bucket.Count >= geocodeRateLimitMax {
		retry := int(math.Max(1, math.Ceil(bucket.ResetAt.Sub(now).Seconds())))
		return true, strconv.Itoa(retry)
	}

	bucket.Count++
	geocodeRateLimit[ip] = bucket
	return false, ""
}

func clientIP(r *http.Request) string {
	forwarded := r.Header.Get("x-forwarded-for")
	if forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if realIP := strings.TrimSpace(r.Header.Get("x-real-ip")); realIP != "" {
		return realIP
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func parseFloatQuery(r *http.Request, key string) (float64, bool) {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || !isFinite(parsed) {
		return 0, false
	}
	return parsed, true
}

func parseFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, isFinite(v)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, false
		}
		return parsed, isFinite(parsed)
	default:
		return 0, false
	}
}

func roundCoord(value float64) string {
	return strconv.FormatFloat(value, 'f', 5, 64)
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

func urlQueryFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func fetchJSON(ctx context.Context, url string) (map[string]any, error) {
	client := &http.Client{Timeout: 8 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Genfity Online Ordering (https://order.genfity.com)")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("upstream_error")
	}

	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func fetchJSONArray(ctx context.Context, url string) ([]any, error) {
	client := &http.Client{Timeout: 8 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Genfity Online Ordering (https://order.genfity.com)")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("upstream_error")
	}

	var payload []any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getMap(m map[string]any, key string) map[string]any {
	if v, ok := m[key]; ok {
		if obj, ok := v.(map[string]any); ok {
			return obj
		}
	}
	return nil
}

type addressParts struct {
	StreetLine    *string `json:"streetLine"`
	Neighbourhood *string `json:"neighbourhood"`
	Suburb        *string `json:"suburb"`
	City          *string `json:"city"`
	State         *string `json:"state"`
	Postcode      *string `json:"postcode"`
	Country       *string `json:"country"`
}

func buildAddressParts(address map[string]any) addressParts {
	if address == nil {
		return addressParts{}
	}

	streetLine := buildStreetLine(address)
	neighbourhood := pickFirstString(address, "neighbourhood")
	suburb := pickFirstString(address, "suburb", "city_district", "district")
	city := pickFirstString(address, "city", "town", "village", "municipality", "county")
	state := pickFirstString(address, "state", "region")
	postcode := pickFirstString(address, "postcode")
	country := pickFirstString(address, "country")

	return addressParts{
		StreetLine:    stringPtr(streetLine),
		Neighbourhood: stringPtr(neighbourhood),
		Suburb:        stringPtr(suburb),
		City:          stringPtr(city),
		State:         stringPtr(state),
		Postcode:      stringPtr(postcode),
		Country:       stringPtr(country),
	}
}

func buildStreetLine(address map[string]any) string {
	houseNumber := pickFirstString(address, "house_number")
	road := pickFirstString(address, "road", "pedestrian", "footway", "path", "cycleway")
	if houseNumber != "" && road != "" {
		return houseNumber + " " + road
	}
	return road
}

func buildFormattedAddress(parts addressParts) string {
	segments := make([]string, 0)
	if parts.StreetLine != nil {
		segments = append(segments, *parts.StreetLine)
	}
	if parts.Suburb != nil {
		segments = append(segments, *parts.Suburb)
	} else if parts.City != nil {
		segments = append(segments, *parts.City)
	}
	if parts.State != nil && parts.Postcode != nil {
		segments = append(segments, *parts.State+" "+*parts.Postcode)
	} else if parts.State != nil {
		segments = append(segments, *parts.State)
	} else if parts.Postcode != nil {
		segments = append(segments, *parts.Postcode)
	}
	if parts.Country != nil {
		segments = append(segments, *parts.Country)
	}
	return strings.Join(segments, ", ")
}

func pickFirstString(address map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := address[key]; ok {
			if s, ok := v.(string); ok {
				trimmed := strings.TrimSpace(s)
				if trimmed != "" {
					return trimmed
				}
			}
		}
	}
	return ""
}

func stringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}
