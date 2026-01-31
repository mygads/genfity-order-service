package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type deliveryZonePoint struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type deliveryZonePayload struct {
	ID       any    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	RadiusKm any    `json:"radiusKm"`
	Polygon  any    `json:"polygon"`
	IsActive *bool  `json:"isActive"`
}

type geoJSONFeatureCollection struct {
	Type     string           `json:"type"`
	Features []geoJSONFeature `json:"features"`
}

type geoJSONFeature struct {
	Type       string         `json:"type"`
	Geometry   map[string]any `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

func (h *Handler) MerchantDeliveryZonesList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select id, merchant_id, name, type, radius_km, polygon, is_active, created_at, updated_at
        from merchant_delivery_zones
        where merchant_id = $1
        order by created_at desc
    `, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("delivery zones list query failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve delivery zones")
		return
	}
	defer rows.Close()

	zones := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id         int64
			merchantID int64
			name       string
			zoneType   string
			radius     pgtype.Numeric
			polygon    []byte
			isActive   bool
			createdAt  time.Time
			updatedAt  time.Time
		)
		if err := rows.Scan(&id, &merchantID, &name, &zoneType, &radius, &polygon, &isActive, &createdAt, &updatedAt); err != nil {
			h.Logger.Error("delivery zones list scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve delivery zones")
			return
		}

		zones = append(zones, map[string]any{
			"id":         id,
			"merchantId": merchantID,
			"name":       name,
			"type":       zoneType,
			"radiusKm":   numericToNullableFloat(radius),
			"polygon":    decodeJSONB(polygon),
			"isActive":   isActive,
			"createdAt":  createdAt,
			"updatedAt":  updatedAt,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       zones,
		"message":    "Delivery zones retrieved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDeliveryZonesUpsert(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	var body deliveryZonePayload
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	name := strings.TrimSpace(body.Name)
	zoneType := strings.ToUpper(strings.TrimSpace(body.Type))
	if name == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Zone name is required")
		return
	}
	if zoneType != "RADIUS" && zoneType != "POLYGON" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Zone type must be RADIUS or POLYGON")
		return
	}

	radiusKm, _ := parseOptionalFloat(body.RadiusKm)
	polygonPoints, polygonErr := parsePolygon(body.Polygon)

	if zoneType == "RADIUS" {
		if radiusKm == nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "radiusKm is required for RADIUS zones")
			return
		}
		if *radiusKm <= 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "radiusKm must be a number greater than 0")
			return
		}

		var lat, lng pgtype.Numeric
		if err := h.DB.QueryRow(ctx, "select latitude, longitude from merchants where id = $1", *authCtx.MerchantID).Scan(&lat, &lng); err != nil {
			if err == pgx.ErrNoRows {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant latitude/longitude must be set before using radius zones")
				return
			}
			h.Logger.Error("delivery zone merchant lookup failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save delivery zone")
			return
		}
		if !lat.Valid || !lng.Valid {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant latitude/longitude must be set before using radius zones")
			return
		}
	}

	if zoneType == "POLYGON" {
		if polygonErr != nil || polygonPoints == nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "polygon is required for POLYGON zones")
			return
		}
		if len(polygonPoints) < 3 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "polygon must have at least 3 points")
			return
		}
		if len(polygonPoints) > 250 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "polygon has too many points (max 250)")
			return
		}
	}

	zoneID, err := parseOptionalInt64Value(body.ID)
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid zone id")
		return
	}

	isActive := true
	if body.IsActive != nil {
		isActive = *body.IsActive
	}

	var polygonJSON []byte
	if zoneType == "POLYGON" {
		data, err := json.Marshal(polygonPoints)
		if err != nil {
			h.Logger.Error("delivery zone polygon marshal failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save delivery zone")
			return
		}
		polygonJSON = data
	}

	var (
		id            int64
		merchantID    int64
		storedName    string
		storedType    string
		storedRadius  pgtype.Numeric
		storedPolygon []byte
		storedActive  bool
		createdAt     time.Time
		updatedAt     time.Time
	)

	if zoneID != nil {
		err = h.DB.QueryRow(ctx, `
            update merchant_delivery_zones
            set name = $1, type = $2, radius_km = $3, polygon = $4, is_active = $5, updated_at = now()
            where id = $6 and merchant_id = $7
            returning id, merchant_id, name, type, radius_km, polygon, is_active, created_at, updated_at
        `, name, zoneType, radiusKm, nullableJSONB(polygonJSON), isActive, *zoneID, *authCtx.MerchantID).Scan(&id, &merchantID, &storedName, &storedType, &storedRadius, &storedPolygon, &storedActive, &createdAt, &updatedAt)
		if err != nil {
			if err == pgx.ErrNoRows {
				response.Error(w, http.StatusNotFound, "NOT_FOUND", "Zone not found")
				return
			}
			h.Logger.Error("delivery zone update failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save delivery zone")
			return
		}
	} else {
		err = h.DB.QueryRow(ctx, `
            insert into merchant_delivery_zones (merchant_id, name, type, radius_km, polygon, is_active)
            values ($1, $2, $3, $4, $5, $6)
            returning id, merchant_id, name, type, radius_km, polygon, is_active, created_at, updated_at
        `, *authCtx.MerchantID, name, zoneType, radiusKm, nullableJSONB(polygonJSON), isActive).Scan(&id, &merchantID, &storedName, &storedType, &storedRadius, &storedPolygon, &storedActive, &createdAt, &updatedAt)
		if err != nil {
			h.Logger.Error("delivery zone create failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save delivery zone")
			return
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"id":         id,
			"merchantId": merchantID,
			"name":       storedName,
			"type":       storedType,
			"radiusKm":   numericToNullableFloat(storedRadius),
			"polygon":    decodeJSONB(storedPolygon),
			"isActive":   storedActive,
			"createdAt":  createdAt,
			"updatedAt":  updatedAt,
		},
		"message":    "Delivery zone saved successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDeliveryZonesDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	zoneID, err := readQueryInt64(r, "id")
	if err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Zone id is required")
		return
	}

	var merchantID int64
	if err := h.DB.QueryRow(ctx, "select merchant_id from merchant_delivery_zones where id = $1", zoneID).Scan(&merchantID); err != nil {
		if err == pgx.ErrNoRows {
			response.Error(w, http.StatusNotFound, "NOT_FOUND", "Zone not found")
			return
		}
		h.Logger.Error("delivery zone lookup failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete delivery zone")
		return
	}

	if merchantID != *authCtx.MerchantID {
		response.Error(w, http.StatusNotFound, "NOT_FOUND", "Zone not found")
		return
	}

	if _, err := h.DB.Exec(ctx, "delete from merchant_delivery_zones where id = $1", zoneID); err != nil {
		h.Logger.Error("delivery zone delete failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete delivery zone")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"data":       nil,
		"message":    "Delivery zone deleted successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDeliveryZonesBulkImport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	authCtx, ok := middleware.GetAuthContext(ctx)
	if !ok || authCtx.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}
	if !authCtx.IsOwner {
		response.Error(w, http.StatusForbidden, "FORBIDDEN", "Owner access required")
		return
	}

	var body struct {
		GeoJSON         geoJSONFeatureCollection `json:"geojson"`
		ReplaceExisting bool                     `json:"replaceExisting"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}

	if body.GeoJSON.Type != "FeatureCollection" || len(body.GeoJSON.Features) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid GeoJSON: expected FeatureCollection")
		return
	}

	hasRadius := false
	zones := make([]deliveryZonePayload, 0)
	nameIndex := 1

	for _, feature := range body.GeoJSON.Features {
		props := feature.Properties
		name := strings.TrimSpace(toStringValue(props["name"]))
		if name == "" {
			name = "Imported Zone " + fmtInt(nameIndex)
			nameIndex++
		}
		if name == "" {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Zone name is required")
			return
		}

		zoneTypeRaw := strings.ToUpper(strings.TrimSpace(toStringValue(props["zoneType"])))
		if zoneTypeRaw == "" {
			zoneTypeRaw = strings.ToUpper(strings.TrimSpace(toStringValue(props["type"])))
		}
		isPolygon := strings.EqualFold(toStringValue(feature.Geometry["type"]), "Polygon") || zoneTypeRaw == "POLYGON"

		isActive := true
		if val, ok := props["isActive"].(bool); ok {
			isActive = val
		}

		if isPolygon {
			polygon, err := parsePolygonFromGeoJSON(feature.Geometry)
			if err != nil {
				response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid polygon for zone \""+name+"\" (must be a Polygon with 3-250 points)")
				return
			}
			zones = append(zones, deliveryZonePayload{
				Name:     name,
				Type:     "POLYGON",
				Polygon:  polygon,
				IsActive: &isActive,
			})
			continue
		}

		hasRadius = true
		radiusKm, err := parseOptionalFloat(props["radiusKm"])
		if err != nil || radiusKm == nil || *radiusKm <= 0 {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid radiusKm for zone \""+name+"\" (must be > 0)")
			return
		}
		zones = append(zones, deliveryZonePayload{
			Name:     name,
			Type:     "RADIUS",
			RadiusKm: radiusKm,
			IsActive: &isActive,
		})
	}

	if len(zones) == 0 {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "GeoJSON contains no features to import")
		return
	}

	if hasRadius {
		var lat, lng pgtype.Numeric
		if err := h.DB.QueryRow(ctx, "select latitude, longitude from merchants where id = $1", *authCtx.MerchantID).Scan(&lat, &lng); err != nil || !lat.Valid || !lng.Valid {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant latitude/longitude must be set before importing radius zones")
			return
		}
	}

	tx, err := h.DB.Begin(ctx)
	if err != nil {
		h.Logger.Error("delivery zone bulk begin failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if body.ReplaceExisting {
		if _, err := tx.Exec(ctx, "delete from merchant_delivery_zones where merchant_id = $1", *authCtx.MerchantID); err != nil {
			h.Logger.Error("delivery zone bulk delete failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
			return
		}
	}

	for _, zone := range zones {
		var polygonJSON []byte
		if zone.Type == "POLYGON" {
			data, err := json.Marshal(zone.Polygon)
			if err != nil {
				h.Logger.Error("delivery zone bulk polygon marshal failed", zapError(err))
				response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
				return
			}
			polygonJSON = data
		}

		if _, err := tx.Exec(ctx, `
            insert into merchant_delivery_zones (merchant_id, name, type, radius_km, polygon, is_active)
            values ($1, $2, $3, $4, $5, $6)
        `, *authCtx.MerchantID, zone.Name, zone.Type, zone.RadiusKm, nullableJSONB(polygonJSON), *zone.IsActive); err != nil {
			h.Logger.Error("delivery zone bulk insert failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		h.Logger.Error("delivery zone bulk commit failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
		return
	}

	rows, err := h.DB.Query(ctx, `
        select id, merchant_id, name, type, radius_km, polygon, is_active, created_at, updated_at
        from merchant_delivery_zones
        where merchant_id = $1
        order by created_at desc
    `, *authCtx.MerchantID)
	if err != nil {
		h.Logger.Error("delivery zone bulk fetch failed", zapError(err))
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
		return
	}
	defer rows.Close()

	result := make([]map[string]any, 0)
	for rows.Next() {
		var (
			id         int64
			merchantID int64
			name       string
			zoneType   string
			radius     pgtype.Numeric
			polygon    []byte
			isActive   bool
			createdAt  time.Time
			updatedAt  time.Time
		)
		if err := rows.Scan(&id, &merchantID, &name, &zoneType, &radius, &polygon, &isActive, &createdAt, &updatedAt); err != nil {
			h.Logger.Error("delivery zone bulk scan failed", zapError(err))
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to bulk import delivery zones")
			return
		}
		result = append(result, map[string]any{
			"id":         id,
			"merchantId": merchantID,
			"name":       name,
			"type":       zoneType,
			"radiusKm":   numericToNullableFloat(radius),
			"polygon":    decodeJSONB(polygon),
			"isActive":   isActive,
			"createdAt":  createdAt,
			"updatedAt":  updatedAt,
		})
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"importedCount": len(zones),
			"zones":         result,
		},
		"message":    "Delivery zones imported successfully",
		"statusCode": 200,
	})
}

func parseOptionalFloat(value any) (*float64, error) {
	if value == nil || value == "" {
		return nil, nil
	}
	if num, ok := value.(float64); ok {
		return &num, nil
	}
	if num, ok := value.(int); ok {
		f := float64(num)
		return &f, nil
	}
	if num, ok := value.(int64); ok {
		f := float64(num)
		return &f, nil
	}
	if str, ok := value.(string); ok {
		str = strings.TrimSpace(str)
		if str == "" {
			return nil, nil
		}
		var f float64
		if _, err := fmt.Sscan(str, &f); err != nil {
			return nil, err
		}
		return &f, nil
	}
	return nil, errors.New("invalid number")
}

func parseOptionalInt64Value(value any) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case float64:
		val := int64(v)
		return &val, nil
	case int64:
		return &v, nil
	case int:
		val := int64(v)
		return &val, nil
	case string:
		if strings.TrimSpace(v) == "" {
			return nil, nil
		}
		parsed, err := parseInt64Value(v)
		if err != nil {
			return nil, err
		}
		return &parsed, nil
	default:
		return nil, errors.New("invalid id")
	}
}

func parsePolygon(value any) ([]deliveryZonePoint, error) {
	if value == nil {
		return nil, errors.New("polygon required")
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("polygon invalid")
	}
	points := make([]deliveryZonePoint, 0, len(items))
	for _, raw := range items {
		pointMap, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("polygon invalid")
		}
		lat, err := parseOptionalFloat(pointMap["lat"])
		if err != nil || lat == nil {
			return nil, errors.New("polygon invalid")
		}
		lng, err := parseOptionalFloat(pointMap["lng"])
		if err != nil || lng == nil {
			return nil, errors.New("polygon invalid")
		}
		if *lat < -90 || *lat > 90 || *lng < -180 || *lng > 180 {
			return nil, errors.New("polygon invalid")
		}
		points = append(points, deliveryZonePoint{Lat: *lat, Lng: *lng})
	}
	return points, nil
}

func parsePolygonFromGeoJSON(geometry map[string]any) ([]deliveryZonePoint, error) {
	if geometry == nil || !strings.EqualFold(toStringValue(geometry["type"]), "Polygon") {
		return nil, errors.New("invalid geometry")
	}
	coords, ok := geometry["coordinates"].([]any)
	if !ok || len(coords) == 0 {
		return nil, errors.New("invalid polygon")
	}
	ring, ok := coords[0].([]any)
	if !ok || len(ring) < 4 {
		return nil, errors.New("invalid polygon")
	}

	points := make([]deliveryZonePoint, 0, len(ring))
	for _, raw := range ring {
		pair, ok := raw.([]any)
		if !ok || len(pair) < 2 {
			return nil, errors.New("invalid polygon")
		}
		lng, err := parseOptionalFloat(pair[0])
		if err != nil || lng == nil {
			return nil, errors.New("invalid polygon")
		}
		lat, err := parseOptionalFloat(pair[1])
		if err != nil || lat == nil {
			return nil, errors.New("invalid polygon")
		}
		if *lat < -90 || *lat > 90 || *lng < -180 || *lng > 180 {
			return nil, errors.New("invalid polygon")
		}
		points = append(points, deliveryZonePoint{Lat: *lat, Lng: *lng})
	}

	if len(points) >= 2 {
		first := points[0]
		last := points[len(points)-1]
		if first.Lat == last.Lat && first.Lng == last.Lng {
			points = points[:len(points)-1]
		}
	}

	if len(points) < 3 || len(points) > 250 {
		return nil, errors.New("invalid polygon")
	}

	return points, nil
}

func numericToNullableFloat(value pgtype.Numeric) any {
	if !value.Valid {
		return nil
	}
	return utils.NumericToFloat64(value)
}

func decodeJSONB(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	var out any
	if err := json.Unmarshal(value, &out); err != nil {
		return nil
	}
	return out
}

func nullableJSONB(value []byte) any {
	if len(value) > 0 {
		return value
	}
	return nil
}

func toStringValue(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(value)
	}
}

func fmtInt(value int) string {
	return fmt.Sprint(value)
}
