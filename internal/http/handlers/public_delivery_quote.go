package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"

	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

type deliveryQuoteRequest struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type deliveryZonePoint struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

func (h *Handler) PublicDeliveryQuote(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	merchantCode := readPathString(r, "code")
	if merchantCode == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Merchant code is required")
		return
	}

	var body deliveryQuoteRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_COORDS", "Valid latitude and longitude are required")
		return
	}

	if !deliveryIsFinite(body.Latitude) || !deliveryIsFinite(body.Longitude) {
		response.Error(w, http.StatusBadRequest, "INVALID_COORDS", "Valid latitude and longitude are required")
		return
	}

	var (
		merchantID           int64
		isActive             bool
		isDeliveryEnabled    bool
		enforceDeliveryZones bool
		latitude             pgtype.Numeric
		longitude            pgtype.Numeric
		deliveryMaxDistance  pgtype.Numeric
		deliveryFeeBase      pgtype.Numeric
		deliveryFeePerKm     pgtype.Numeric
		deliveryFeeMin       pgtype.Numeric
		deliveryFeeMax       pgtype.Numeric
	)

	err := h.DB.QueryRow(ctx, `
		select id, is_active, is_delivery_enabled, enforce_delivery_zones,
		       latitude, longitude, delivery_max_distance_km,
		       delivery_fee_base, delivery_fee_per_km, delivery_fee_min, delivery_fee_max
		from merchants
		where code = $1
	`, merchantCode).Scan(
		&merchantID,
		&isActive,
		&isDeliveryEnabled,
		&enforceDeliveryZones,
		&latitude,
		&longitude,
		&deliveryMaxDistance,
		&deliveryFeeBase,
		&deliveryFeePerKm,
		&deliveryFeeMin,
		&deliveryFeeMax,
	)
	if err != nil || !isActive {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found or inactive")
		return
	}

	if !isDeliveryEnabled {
		response.Error(w, http.StatusBadRequest, "DELIVERY_NOT_ENABLED", "Delivery is not available for this merchant")
		return
	}

	if !latitude.Valid || !longitude.Valid {
		response.Error(w, http.StatusBadRequest, "MERCHANT_LOCATION_NOT_SET", "Merchant location is not configured for delivery")
		return
	}

	merchantLat := utils.NumericToFloat64(latitude)
	merchantLng := utils.NumericToFloat64(longitude)

	distanceKm := deliveryRound3(haversineDistanceKm(merchantLat, merchantLng, body.Latitude, body.Longitude))

	if deliveryMaxDistance.Valid {
		maxDistance := utils.NumericToFloat64(deliveryMaxDistance)
		if distanceKm > maxDistance {
			response.Error(w, http.StatusBadRequest, "OUT_OF_RANGE", "Delivery is only available within "+formatFloat(maxDistance)+" km")
			return
		}
	}

	if enforceDeliveryZones {
		ok, err := h.validateDeliveryZones(ctx, merchantID, merchantLat, merchantLng, body.Latitude, body.Longitude)
		if errors.Is(err, errNoZonesConfigured) {
			response.Error(w, http.StatusBadRequest, "NO_ZONES_CONFIGURED", "Delivery zones are not properly configured")
			return
		}
		if err != nil {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Delivery validation failed")
			return
		}
		if !ok {
			response.Error(w, http.StatusBadRequest, "OUT_OF_ZONE", "Delivery is not available for this location")
			return
		}
	}

	feeBase := 0.0
	if deliveryFeeBase.Valid {
		feeBase = utils.NumericToFloat64(deliveryFeeBase)
	}
	feePerKm := 0.0
	if deliveryFeePerKm.Valid {
		feePerKm = utils.NumericToFloat64(deliveryFeePerKm)
	}

	fee := deliveryRound2(feeBase + feePerKm*distanceKm)
	if deliveryFeeMin.Valid {
		minFee := utils.NumericToFloat64(deliveryFeeMin)
		if fee < minFee {
			fee = minFee
		}
	}
	if deliveryFeeMax.Valid {
		maxFee := utils.NumericToFloat64(deliveryFeeMax)
		if fee > maxFee {
			fee = maxFee
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"distanceKm": distanceKm,
			"feeAmount":  deliveryRound2(fee),
		},
		"message": "Delivery fee calculated successfully",
	})
}

var errNoZonesConfigured = errors.New("no zones configured")

func (h *Handler) validateDeliveryZones(ctx context.Context, merchantID int64, merchantLat, merchantLng, deliveryLat, deliveryLng float64) (bool, error) {
	type zoneRow struct {
		Type     string
		RadiusKm pgtype.Numeric
		Polygon  []byte
	}

	rows, err := h.DB.Query(ctx, `
		select type, radius_km, polygon
		from merchant_delivery_zones
		where merchant_id = $1 and is_active = true
	`, merchantID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	zones := make([]zoneRow, 0)
	for rows.Next() {
		var z zoneRow
		if err := rows.Scan(&z.Type, &z.RadiusKm, &z.Polygon); err == nil {
			zones = append(zones, z)
		}
	}

	if len(zones) == 0 {
		return false, errNoZonesConfigured
	}

	for _, zone := range zones {
		switch zone.Type {
		case "RADIUS":
			if zone.RadiusKm.Valid {
				radius := utils.NumericToFloat64(zone.RadiusKm)
				d := haversineDistanceKm(merchantLat, merchantLng, deliveryLat, deliveryLng)
				if d <= radius {
					return true, nil
				}
			}
		case "POLYGON":
			if len(zone.Polygon) == 0 {
				continue
			}
			var polygon []deliveryZonePoint
			if err := json.Unmarshal(zone.Polygon, &polygon); err != nil {
				continue
			}
			if len(polygon) < 3 {
				continue
			}
			if pointInPolygon(deliveryLat, deliveryLng, polygon) {
				return true, nil
			}
		}
	}

	return false, nil
}

func haversineDistanceKm(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadius = 6371.0
	toRad := func(deg float64) float64 {
		return deg * math.Pi / 180
	}

	dLat := toRad(lat2 - lat1)
	dLng := toRad(lng2 - lng1)

	lat1Rad := toRad(lat1)
	lat2Rad := toRad(lat2)

	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Sin(dLng/2)*math.Sin(dLng/2)*math.Cos(lat1Rad)*math.Cos(lat2Rad)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadius * c
}

func pointInPolygon(lat, lng float64, polygon []deliveryZonePoint) bool {
	inside := false
	j := len(polygon) - 1
	for i := 0; i < len(polygon); i++ {
		xi := polygon[i].Lng
		yi := polygon[i].Lat
		xj := polygon[j].Lng
		yj := polygon[j].Lat

		intersect := ((yi > lat) != (yj > lat)) &&
			(lng < (xj-xi)*(lat-yi)/(yj-yi)+xi)
		if intersect {
			inside = !inside
		}
		j = i
	}
	return inside
}

func deliveryRound2(value float64) float64 {
	return math.Round(value*100) / 100
}

func deliveryRound3(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func deliveryIsFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
