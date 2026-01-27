package handlers

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"genfity-order-services/internal/middleware"
	"genfity-order-services/internal/storage"
	"genfity-order-services/internal/utils"
	"genfity-order-services/pkg/response"

	"github.com/jackc/pgx/v5/pgtype"
)

const (
	maxSideProof    = 1400
	qrisSize        = 900
	menuThumbSize   = 300
	menuThumb2xSize = 600
)

type imageMetaPayload struct {
	Format string                `json:"format"`
	Source utils.ImageSourceMeta `json:"source"`
}

type menuThumbVariant struct {
	Dpr    int    `json:"dpr"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	URL    string `json:"url"`
}

type menuThumbMeta struct {
	Format   string                `json:"format"`
	Source   utils.ImageSourceMeta `json:"source"`
	Variants []menuThumbVariant    `json:"variants"`
}

func (h *Handler) makeStore(r *http.Request) (*storage.ObjectStore, error) {
	ctx := r.Context()
	return storage.NewObjectStore(ctx, storage.Config{
		Endpoint:        h.Config.ObjectStoreEndpoint,
		Region:          h.Config.ObjectStoreRegion,
		AccessKeyID:     h.Config.ObjectStoreAccessKeyID,
		SecretAccessKey: h.Config.ObjectStoreSecretAccessKey,
		Bucket:          h.Config.ObjectStoreBucket,
		PublicBaseURL:   h.Config.ObjectStorePublicBaseURL,
		StorageClass:    h.Config.ObjectStoreStorageClass,
	})
}

type fileReadErrorKind string

const (
	fileReadErrMissing     fileReadErrorKind = "missing"
	fileReadErrReadFailed  fileReadErrorKind = "read_failed"
	fileReadErrTooLarge    fileReadErrorKind = "too_large"
	fileReadErrInvalidType fileReadErrorKind = "invalid_type"
)

type fileReadError struct {
	Kind    fileReadErrorKind
	Message string
	Err     error
}

func readFileBytes(r *http.Request, field string, validateType bool, maxBytes int64) ([]byte, string, *string, *fileReadError) {
	file, header, err := r.FormFile(field)
	if err != nil {
		return nil, "", nil, &fileReadError{Kind: fileReadErrMissing, Message: "File is required", Err: err}
	}
	defer file.Close()

	if maxBytes <= 0 {
		maxBytes = 5 * 1024 * 1024
	}
	maxSizeMB := maxBytes / (1024 * 1024)
	if maxSizeMB <= 0 {
		maxSizeMB = 5
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if readErr != nil {
		return nil, "", nil, &fileReadError{Kind: fileReadErrReadFailed, Message: "Failed to read file", Err: readErr}
	}
	if int64(len(data)) > maxBytes {
		return nil, "", nil, &fileReadError{Kind: fileReadErrTooLarge, Message: fmt.Sprintf("File size must be less than %dMB.", maxSizeMB)}
	}

	ct := strings.TrimSpace(header.Header.Get("Content-Type"))
	if ct == "" {
		ct = utils.DetectContentType(data)
	}
	ctLower := strings.ToLower(ct)
	if validateType && !utils.ValidateImageContentType(ctLower) {
		msg := "Invalid file type. Please upload an image file."
		return nil, ctLower, nil, &fileReadError{Kind: fileReadErrInvalidType, Message: msg}
	}

	var filename *string
	if header.Filename != "" {
		v := header.Filename
		filename = &v
	}

	return data, ctLower, filename, nil
}

func randomSuffix8() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func addRandomSuffix(pathname string) string {
	safeName := strings.TrimRight(pathname, "/")
	lastSlash := strings.LastIndex(safeName, "/")
	lastDot := strings.LastIndex(safeName, ".")
	hasExtension := lastDot > lastSlash
	base := safeName
	ext := ""
	if hasExtension {
		base = safeName[:lastDot]
		ext = safeName[lastDot:]
	}
	suffix := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randomSuffix8())
	return fmt.Sprintf("%s-%s%s", base, suffix, ext)
}

var presignAllowedTypes = map[string]string{
	"image/jpeg":    "jpg",
	"image/jpg":     "jpg",
	"image/png":     "png",
	"image/webp":    "webp",
	"image/gif":     "gif",
	"image/bmp":     "bmp",
	"image/svg+xml": "svg",
	"image/tiff":    "tiff",
	"image/heic":    "heic",
	"image/heif":    "heif",
}

func buildPresignObjectKey(uploadType string, merchantCode string, menuID *string, extension string) (string, error) {
	timestamp := time.Now().UnixMilli()
	safeCode := strings.ToLower(strings.TrimSpace(merchantCode))

	switch uploadType {
	case "logo":
		return fmt.Sprintf("merchants/%s/logos/logo-%d.%s", safeCode, timestamp, extension), nil
	case "banner":
		return fmt.Sprintf("merchants/%s/banners/banner-%d.%s", safeCode, timestamp, extension), nil
	case "promo":
		return fmt.Sprintf("merchants/%s/promos/promo-%d.%s", safeCode, timestamp, extension), nil
	case "menu":
		if menuID == nil || strings.TrimSpace(*menuID) == "" {
			return "", errors.New("menuId is required")
		}
		return fmt.Sprintf("merchants/%s/menus/menu-%s-%d.%s", safeCode, strings.TrimSpace(*menuID), timestamp, extension), nil
	case "menu-thumb":
		if menuID == nil || strings.TrimSpace(*menuID) == "" {
			return "", errors.New("menuId is required")
		}
		return fmt.Sprintf("merchants/%s/menus/menu-%s-thumb-%d.%s", safeCode, strings.TrimSpace(*menuID), timestamp, extension), nil
	case "menu-thumb-2x":
		if menuID == nil || strings.TrimSpace(*menuID) == "" {
			return "", errors.New("menuId is required")
		}
		return fmt.Sprintf("merchants/%s/menus/menu-%s-thumb-2x-%d.%s", safeCode, strings.TrimSpace(*menuID), timestamp, extension), nil
	default:
		return "", errors.New("Invalid upload type")
	}
}

// --------------------
// Public: payment proof upload
// --------------------

func (h *Handler) PublicOrderUploadProof(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderNumber := readPathString(r, "orderNumber")
	if orderNumber == "" {
		response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "Order number is required")
		return
	}

	token := r.URL.Query().Get("token")

	// Read + validate file
	data, _, _, ferr := readFileBytes(r, "file", true, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		switch ferr.Kind {
		case fileReadErrMissing:
			response.Error(w, http.StatusBadRequest, "FILE_REQUIRED", "File is required")
		case fileReadErrTooLarge, fileReadErrInvalidType:
			response.Error(w, http.StatusBadRequest, "INVALID_FILE", ferr.Message)
		default:
			response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload payment proof")
		}
		return
	}

	var (
		merchantID   pgtype.Int8
		merchantCode pgtype.Text
		paymentID    pgtype.Int8
	)

	if err := h.DB.QueryRow(ctx, `
		select m.id, m.code, p.id
		from orders o
		join merchants m on m.id = o.merchant_id
		left join payments p on p.order_id = o.id
		where o.order_number = $1
		limit 1
	`, orderNumber).Scan(&merchantID, &merchantCode, &paymentID); err != nil {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !merchantCode.Valid || !utils.VerifyOrderTrackingToken(h.Config.OrderTrackingTokenSecret, token, merchantCode.String, orderNumber) {
		response.Error(w, http.StatusNotFound, "ORDER_NOT_FOUND", "Order not found")
		return
	}

	if !paymentID.Valid {
		response.Error(w, http.StatusNotFound, "PAYMENT_NOT_FOUND", "Payment record not found")
		return
	}

	// Normalize to JPEG (rotate + fit inside 1400)
	jpegBytes, sourceMeta, err := utils.EncodeJpegFitInside(data, maxSideProof, 88)
	if err != nil {
		// Next catches sharp decode/processing errors and returns 500.
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload payment proof")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload payment proof")
		return
	}

	// Best-effort cleanup old proofs
	prefix := fmt.Sprintf("merchants/%s/orders/%s/payment-proof-", merchantCode.String, orderNumber)
	_ = store.DeletePrefix(ctx, prefix)

	key := fmt.Sprintf("merchants/%s/orders/%s/payment-proof-%d-%s.jpg", merchantCode.String, orderNumber, time.Now().UnixMilli(), randomSuffix8())
	url, err := store.PutObject(ctx, key, jpegBytes, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload payment proof")
		return
	}

	meta := map[string]any{
		"format": "jpeg",
		"source": sourceMeta,
	}
	metaJSON, _ := json.Marshal(meta)

	var uploadedAt time.Time
	if err := h.DB.QueryRow(ctx, `
		update payments
		set customer_proof_url = $1,
		    customer_proof_meta = $2,
		    customer_proof_uploaded_at = $3
		where id = $4
		returning customer_proof_uploaded_at
	`, url, metaJSON, time.Now(), paymentID.Int64).Scan(&uploadedAt); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload payment proof")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"proofUrl": url,
			"payment": map[string]any{
				"id":                      paymentID.Int64,
				"customerProofUrl":        url,
				"customerProofMeta":       json.RawMessage(metaJSON),
				"customerProofUploadedAt": uploadedAt,
			},
		},
		"message":    "Payment proof uploaded",
		"statusCode": 200,
	})
}

// --------------------
// Merchant: uploads
// --------------------

func (h *Handler) MerchantUploadQris(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ac, ok := middleware.GetAuthContext(ctx)

	data, _, _, ferr := readFileBytes(r, "file", true, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		switch ferr.Kind {
		case fileReadErrMissing:
			response.Error(w, http.StatusBadRequest, "FILE_REQUIRED", "File is required")
		case fileReadErrTooLarge, fileReadErrInvalidType:
			response.Error(w, http.StatusBadRequest, "INVALID_FILE", ferr.Message)
		default:
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload QRIS image")
		}
		return
	}

	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantCode pgtype.Text
		country      pgtype.Text
		currency     pgtype.Text
	)
	if err := h.DB.QueryRow(ctx, `select code, country, currency from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantCode, &country, &currency); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	isQrisEligible := strings.EqualFold(strings.TrimSpace(country.String), "indonesia") && strings.EqualFold(strings.TrimSpace(currency.String), "idr")
	if !isQrisEligible {
		response.Error(w, http.StatusBadRequest, "QRIS_NOT_ELIGIBLE", "QRIS is available only for merchants in Indonesia (IDR).")
		return
	}

	jpegBytes, sourceMeta, err := utils.EncodeJpegCoverSquare(data, qrisSize, 90)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload QRIS image")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload QRIS image")
		return
	}

	_ = store.DeletePrefix(ctx, fmt.Sprintf("merchants/%s/payment/qris-", merchantCode.String))

	key := fmt.Sprintf("merchants/%s/payment/qris-%d-%s.jpg", merchantCode.String, time.Now().UnixMilli(), randomSuffix8())
	url, err := store.PutObject(ctx, key, jpegBytes, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload QRIS image")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url": url,
			"meta": map[string]any{
				"size":   qrisSize,
				"format": "jpeg",
				"source": sourceMeta,
			},
		},
		"message":    "QRIS image uploaded successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadLogo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ac, ok := middleware.GetAuthContext(ctx)
	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var merchantCode pgtype.Text
	if err := h.DB.QueryRow(ctx, `select code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found for this user")
		return
	}

	data, _, _, ferr := readFileBytes(r, "file", true, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		if ferr.Kind == fileReadErrMissing {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", "No file provided")
			return
		}
		if ferr.Kind == fileReadErrTooLarge || ferr.Kind == fileReadErrInvalidType {
			response.Error(w, http.StatusBadRequest, "VALIDATION_ERROR", ferr.Message)
			return
		}
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload logo")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload logo")
		return
	}

	_ = store.DeletePrefix(ctx, fmt.Sprintf("merchants/%s/logos/logo-", merchantCode.String))

	key := fmt.Sprintf("merchants/%s/logos/logo-%d-%s.jpg", merchantCode.String, time.Now().UnixMilli(), randomSuffix8())
	url, err := store.PutObject(ctx, key, data, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload logo")
		return
	}

	if _, err := h.DB.Exec(ctx, `update merchants set logo_url = $1 where id = $2`, url, *ac.MerchantID); err != nil {
		response.Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to upload logo")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url": url,
		},
		"message":    "Logo uploaded successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadMerchantImage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ac, ok := middleware.GetAuthContext(ctx)

	data, ctLower, _, ferr := readFileBytes(r, "file", false, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		switch ferr.Kind {
		case fileReadErrMissing:
			response.Error(w, http.StatusBadRequest, "FILE_REQUIRED", "File is required")
		case fileReadErrTooLarge, fileReadErrInvalidType:
			response.Error(w, http.StatusBadRequest, "INVALID_FILE", ferr.Message)
		default:
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		}
		return
	}

	imageType := strings.TrimSpace(r.FormValue("type"))
	if imageType == "" || (imageType != "logo" && imageType != "banner") {
		response.Error(w, http.StatusBadRequest, "INVALID_TYPE", "Image type must be \"logo\" or \"banner\"")
		return
	}
	if !utils.ValidateImageContentType(ctLower) {
		response.Error(w, http.StatusBadRequest, "INVALID_FILE", "Invalid file type. Please upload an image file.")
		return
	}

	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var merchantCode pgtype.Text
	if err := h.DB.QueryRow(ctx, `select code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	var prefix string
	var key string
	if imageType == "logo" {
		prefix = fmt.Sprintf("merchants/%s/logos/logo-", merchantCode.String)
		_ = store.DeletePrefix(ctx, prefix)
		key = addRandomSuffix(fmt.Sprintf("merchants/%s/logos/logo-%d.jpg", merchantCode.String, time.Now().UnixMilli()))
	} else {
		prefix = fmt.Sprintf("merchants/%s/banners/banner-", merchantCode.String)
		_ = store.DeletePrefix(ctx, prefix)
		key = addRandomSuffix(fmt.Sprintf("merchants/%s/banners/banner-%d.jpg", merchantCode.String, time.Now().UnixMilli()))
	}

	url, err := store.PutObject(ctx, key, data, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	if imageType == "logo" {
		if _, err := h.DB.Exec(ctx, `update merchants set logo_url = $1 where id = $2`, url, *ac.MerchantID); err != nil {
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
			return
		}
	} else {
		if _, err := h.DB.Exec(ctx, `update merchants set banner_url = $1 where id = $2`, url, *ac.MerchantID); err != nil {
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
			return
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url":  url,
			"type": imageType,
		},
		"message":    fmt.Sprintf("Merchant %s uploaded successfully", imageType),
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadPromoBanner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ac, ok := middleware.GetAuthContext(ctx)

	data, _, _, ferr := readFileBytes(r, "file", true, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		switch ferr.Kind {
		case fileReadErrMissing:
			response.Error(w, http.StatusBadRequest, "FILE_REQUIRED", "File is required")
		case fileReadErrTooLarge, fileReadErrInvalidType:
			response.Error(w, http.StatusBadRequest, "INVALID_FILE", ferr.Message)
		default:
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload promo banner")
		}
		return
	}

	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantCode    string
		promoBannerUrls []string
	)
	if err := h.DB.QueryRow(ctx, `select code, promo_banner_urls from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantCode, &promoBannerUrls); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	if len(promoBannerUrls) >= 10 {
		response.Error(w, http.StatusBadRequest, "LIMIT_REACHED", "Maximum 10 promotional banners allowed")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload promo banner")
		return
	}

	key := addRandomSuffix(fmt.Sprintf("merchants/%s/promos/promo-%d.jpg", merchantCode, time.Now().UnixMilli()))
	url, err := store.PutObject(ctx, key, data, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload promo banner")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url": url,
		},
		"message":    "Promo banner uploaded successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadMenuImage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ac, ok := middleware.GetAuthContext(ctx)

	data, _, _, ferr := readFileBytes(r, "file", true, h.Config.MaxFileSizeBytes)
	if ferr != nil {
		switch ferr.Kind {
		case fileReadErrMissing:
			response.Error(w, http.StatusBadRequest, "FILE_REQUIRED", "File is required")
		case fileReadErrTooLarge, fileReadErrInvalidType:
			response.Error(w, http.StatusBadRequest, "INVALID_FILE", ferr.Message)
		default:
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		}
		return
	}

	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	menuIDRaw := strings.TrimSpace(r.FormValue("menuId"))
	warnings := make([]string, 0)

	var (
		merchantID   int64
		merchantCode string
	)
	if err := h.DB.QueryRow(ctx, `select id, code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantID, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	var (
		menuIDValue          *int64
		previousImageURL     *string
		previousThumbURL     *string
		previousThumbMeta    []byte
		hasPreviousThumbMeta bool
	)
	if menuIDRaw != "" {
		parsed, err := strconv.ParseInt(menuIDRaw, 10, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "INVALID_MENU_ID", "menuId must be numeric")
			return
		}
		menuIDValue = &parsed

		var imgURL pgtype.Text
		var thumbURL pgtype.Text
		var thumbMeta []byte
		if err := h.DB.QueryRow(ctx, `
			select image_url, image_thumb_url, image_thumb_meta
			from menus
			where id=$1 and merchant_id=$2
			limit 1
		`, parsed, merchantID).Scan(&imgURL, &thumbURL, &thumbMeta); err != nil {
			response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu not found")
			return
		}
		if imgURL.Valid {
			v := imgURL.String
			previousImageURL = &v
		}
		if thumbURL.Valid {
			v := thumbURL.String
			previousThumbURL = &v
		}
		if thumbMeta != nil {
			previousThumbMeta = thumbMeta
			hasPreviousThumbMeta = true
		}
	}

	fullJpeg, sourceMeta, err := utils.EncodeJpegOriginal(data, 95)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	if sourceMeta.Width != nil && *sourceMeta.Width < 800 {
		warnings = append(warnings, fmt.Sprintf(
			"Image is quite small (%dpx wide). For best quality in menu detail/zoom, upload an image at least 800px wide.",
			*sourceMeta.Width,
		))
	}

	thumbJpeg, _, err := utils.EncodeJpegCoverSquare(data, menuThumbSize, 80)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	thumb2xJpeg, _, err := utils.EncodeJpegCoverSquare(data, menuThumb2xSize, 80)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	imageKey := menuIDRaw
	if strings.TrimSpace(imageKey) == "" {
		imageKey = fmt.Sprintf("%d", time.Now().UnixMilli())
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	fullKey := addRandomSuffix(fmt.Sprintf("merchants/%s/menus/menu-%s.jpg", merchantCode, imageKey))
	thumbKey := addRandomSuffix(fmt.Sprintf("merchants/%s/menus/menu-%s-thumb.jpg", merchantCode, imageKey))
	thumb2xKey := addRandomSuffix(fmt.Sprintf("merchants/%s/menus/menu-%s-thumb-2x.jpg", merchantCode, imageKey))

	fullURL, err := store.PutObject(ctx, fullKey, fullJpeg, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}
	thumbURL, err := store.PutObject(ctx, thumbKey, thumbJpeg, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}
	thumb2xURL, err := store.PutObject(ctx, thumb2xKey, thumb2xJpeg, "image/jpeg", "public, max-age=31536000, immutable")
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
		return
	}

	meta := menuThumbMeta{
		Format: "jpeg",
		Source: sourceMeta,
		Variants: []menuThumbVariant{
			{Dpr: 1, Width: menuThumbSize, Height: menuThumbSize, URL: thumbURL},
			{Dpr: 2, Width: menuThumb2xSize, Height: menuThumb2xSize, URL: thumb2xURL},
		},
	}
	metaJSON, _ := json.Marshal(meta)

	if menuIDValue != nil {
		if _, err := h.DB.Exec(ctx, `
			update menus
			set image_url = $1,
			    image_thumb_url = $2,
			    image_thumb_meta = $3,
			    updated_by_user_id = $4
			where id = $5
		`, fullURL, thumbURL, metaJSON, ac.UserID, *menuIDValue); err != nil {
			response.Error(w, http.StatusInternalServerError, "UPLOAD_FAILED", "Failed to upload image")
			return
		}

		urlsToDelete := make([]string, 0)
		if previousImageURL != nil && *previousImageURL != fullURL {
			urlsToDelete = append(urlsToDelete, *previousImageURL)
		}
		if previousThumbURL != nil && *previousThumbURL != thumbURL {
			urlsToDelete = append(urlsToDelete, *previousThumbURL)
		}

		if hasPreviousThumbMeta {
			var prev struct {
				Variants []struct {
					URL *string `json:"url"`
				} `json:"variants"`
			}
			if err := json.Unmarshal(previousThumbMeta, &prev); err == nil {
				for _, v := range prev.Variants {
					if v.URL == nil {
						continue
					}
					urlsToDelete = append(urlsToDelete, *v.URL)
				}
			}
		}

		if len(urlsToDelete) > 0 {
			for _, u := range urlsToDelete {
				if strings.TrimSpace(u) == "" {
					continue
				}
				if u == thumbURL || u == thumb2xURL || u == fullURL {
					continue
				}
				if err := store.DeleteURL(ctx, u); err != nil {
					warnings = append(warnings, "Failed to delete a previous image from storage.")
				}
			}
		}
	}

	message := "Image uploaded successfully"
	if len(warnings) > 0 {
		message = "Image uploaded with warnings"
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url":        fullURL,
			"pathname":   fullKey,
			"thumbUrl":   thumbURL,
			"thumb2xUrl": thumb2xURL,
			"thumbMeta":  json.RawMessage(metaJSON),
			"warnings":   warnings,
		},
		"message":    message,
		"statusCode": 200,
	})
}

func (h *Handler) MerchantDeleteImage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		ImageURL        string  `json:"imageUrl"`
		ImageThumbURL   *string `json:"imageThumbUrl"`
		ImageThumb2xURL *string `json:"imageThumb2xUrl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "URL_REQUIRED", "Image URL is required")
		return
	}
	if strings.TrimSpace(body.ImageURL) == "" {
		response.Error(w, http.StatusBadRequest, "URL_REQUIRED", "Image URL is required")
		return
	}

	ac, ok := middleware.GetAuthContext(ctx)
	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantID   int64
		merchantCode string
	)
	if err := h.DB.QueryRow(ctx, `select id, code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantID, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	// Check if the image/thumbnail is used by any menu item
	var foundID int64
	if body.ImageThumbURL != nil && strings.TrimSpace(*body.ImageThumbURL) != "" {
		_ = h.DB.QueryRow(ctx, `
			select id from menus
			where merchant_id=$1 and (image_url=$2 or image_thumb_url=$3)
			limit 1
		`, merchantID, body.ImageURL, strings.TrimSpace(*body.ImageThumbURL)).Scan(&foundID)
	} else {
		_ = h.DB.QueryRow(ctx, `
			select id from menus
			where merchant_id=$1 and image_url=$2
			limit 1
		`, merchantID, body.ImageURL).Scan(&foundID)
	}
	if foundID != 0 {
		response.JSON(w, http.StatusOK, map[string]any{
			"success":    true,
			"message":    "Image is in use, skipping deletion",
			"statusCode": 200,
		})
		return
	}

	merchantCodeLower := strings.ToLower(merchantCode)
	urls := make([]string, 0, 3)
	urls = append(urls, body.ImageURL)
	if body.ImageThumbURL != nil && strings.TrimSpace(*body.ImageThumbURL) != "" {
		urls = append(urls, strings.TrimSpace(*body.ImageThumbURL))
	}
	if body.ImageThumb2xURL != nil && strings.TrimSpace(*body.ImageThumb2xURL) != "" {
		urls = append(urls, strings.TrimSpace(*body.ImageThumb2xURL))
	}
	for _, u := range urls {
		if !strings.Contains(strings.ToLower(u), fmt.Sprintf("/merchants/%s/", merchantCodeLower)) {
			response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Cannot delete this image")
			return
		}
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "DELETE_FAILED", "Failed to delete image")
		return
	}

	for _, u := range urls {
		_ = store.DeleteURL(ctx, u)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"message":    "Image deleted successfully",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadPresign(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		Type        string      `json:"type"`
		ContentType string      `json:"contentType"`
		FileSize    interface{} `json:"fileSize"`
		MenuID      *string     `json:"menuId"`
		AllowTemp   bool        `json:"allowTemp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "TYPE_REQUIRED", "Upload type is required")
		return
	}
	if strings.TrimSpace(body.Type) == "" {
		response.Error(w, http.StatusBadRequest, "TYPE_REQUIRED", "Upload type is required")
		return
	}

	ct := strings.ToLower(strings.TrimSpace(body.ContentType))
	ext, ok := presignAllowedTypes[ct]
	if !ok {
		response.Error(w, http.StatusBadRequest, "INVALID_CONTENT_TYPE", "Invalid content type")
		return
	}

	maxSize := h.Config.MaxFileSizeBytes
	if maxSize <= 0 {
		maxSize = 5 * 1024 * 1024
	}
	var sizeValue int64
	switch v := body.FileSize.(type) {
	case float64:
		sizeValue = int64(v)
	case int64:
		sizeValue = v
	case int:
		sizeValue = int64(v)
	case string:
		parsed, _ := strconv.ParseInt(v, 10, 64)
		sizeValue = parsed
	default:
		sizeValue = 0
	}
	if sizeValue <= 0 || sizeValue > maxSize {
		response.Error(w, http.StatusBadRequest, "INVALID_FILE_SIZE", fmt.Sprintf("File size must be between 1 and %d bytes", maxSize))
		return
	}

	isMenuType := body.Type == "menu" || body.Type == "menu-thumb" || body.Type == "menu-thumb-2x"
	if isMenuType {
		if body.MenuID == nil || strings.TrimSpace(*body.MenuID) == "" {
			response.Error(w, http.StatusBadRequest, "MENU_ID_REQUIRED", "menuId is required")
			return
		}
	}

	ac, ok := middleware.GetAuthContext(ctx)
	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantID   int64
		merchantCode string
	)
	if err := h.DB.QueryRow(ctx, `select id, code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantID, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	if isMenuType && body.MenuID != nil && !body.AllowTemp {
		parsedMenuID, err := strconv.ParseInt(strings.TrimSpace(*body.MenuID), 10, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "INVALID_MENU_ID", "menuId must be numeric")
			return
		}
		var existsID int64
		if err := h.DB.QueryRow(ctx, `select id from menus where id=$1 and merchant_id=$2 limit 1`, parsedMenuID, merchantID).Scan(&existsID); err != nil || existsID == 0 {
			response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu not found")
			return
		}
	}

	objectKey, err := buildPresignObjectKey(body.Type, merchantCode, body.MenuID, ext)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "PRESIGN_FAILED", "Failed to create upload URL")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "PRESIGN_FAILED", "Failed to create upload URL")
		return
	}

	uploadURL, err := store.PresignPutObject(ctx, objectKey, ct, "public, max-age=31536000, immutable", 15*time.Minute)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "PRESIGN_FAILED", "Failed to create upload URL")
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"uploadUrl": uploadURL,
			"objectKey": objectKey,
			"publicUrl": store.PublicURL(objectKey),
			"expiresIn": 900,
		},
		"message":    "Presigned upload URL created",
		"statusCode": 200,
	})
}

func (h *Handler) MerchantUploadConfirm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		Type      string `json:"type"`
		PublicURL string `json:"publicUrl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "INVALID_TYPE", "Type must be logo or banner")
		return
	}

	if body.Type != "logo" && body.Type != "banner" {
		response.Error(w, http.StatusBadRequest, "INVALID_TYPE", "Type must be logo or banner")
		return
	}
	if strings.TrimSpace(body.PublicURL) == "" {
		response.Error(w, http.StatusBadRequest, "URL_REQUIRED", "publicUrl is required")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm upload")
		return
	}
	if _, ok := store.ResolveKeyFromURL(body.PublicURL); !ok {
		response.Error(w, http.StatusBadRequest, "INVALID_URL", "URL is not managed by R2")
		return
	}

	ac, ok := middleware.GetAuthContext(ctx)
	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantCode string
		logoURL      *string
		bannerURL    *string
	)
	if err := h.DB.QueryRow(ctx, `select code, logo_url, banner_url from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantCode, &logoURL, &bannerURL); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	merchantCodeLower := strings.ToLower(merchantCode)
	if !strings.Contains(strings.ToLower(body.PublicURL), fmt.Sprintf("/merchants/%s/", merchantCodeLower)) {
		response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Cannot update this asset")
		return
	}

	if body.Type == "logo" {
		if logoURL != nil && strings.TrimSpace(*logoURL) != "" {
			_ = store.DeleteURL(ctx, *logoURL)
		}
		if _, err := h.DB.Exec(ctx, `update merchants set logo_url = $1 where id = $2`, body.PublicURL, *ac.MerchantID); err != nil {
			response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm upload")
			return
		}
	} else {
		if bannerURL != nil && strings.TrimSpace(*bannerURL) != "" {
			_ = store.DeleteURL(ctx, *bannerURL)
		}
		if _, err := h.DB.Exec(ctx, `update merchants set banner_url = $1 where id = $2`, body.PublicURL, *ac.MerchantID); err != nil {
			response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm upload")
			return
		}
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"url":  body.PublicURL,
			"type": body.Type,
		},
		"message":    fmt.Sprintf("Merchant %s updated successfully", body.Type),
		"statusCode": 200,
	})
}

func (h *Handler) MerchantMenuImageConfirm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		ImageURL        string           `json:"imageUrl"`
		ImageThumbURL   string           `json:"imageThumbUrl"`
		ImageThumb2xURL *string          `json:"imageThumb2xUrl"`
		ImageThumbMeta  *json.RawMessage `json:"imageThumbMeta"`
		MenuID          *string          `json:"menuId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		response.Error(w, http.StatusBadRequest, "URL_REQUIRED", "imageUrl is required")
		return
	}
	if strings.TrimSpace(body.ImageURL) == "" {
		response.Error(w, http.StatusBadRequest, "URL_REQUIRED", "imageUrl is required")
		return
	}
	if strings.TrimSpace(body.ImageThumbURL) == "" {
		response.Error(w, http.StatusBadRequest, "THUMB_URL_REQUIRED", "imageThumbUrl is required")
		return
	}

	store, err := h.makeStore(r)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm menu image")
		return
	}
	urlsToValidate := []string{body.ImageURL, body.ImageThumbURL}
	if body.ImageThumb2xURL != nil && strings.TrimSpace(*body.ImageThumb2xURL) != "" {
		urlsToValidate = append(urlsToValidate, strings.TrimSpace(*body.ImageThumb2xURL))
	}
	for _, u := range urlsToValidate {
		if _, ok := store.ResolveKeyFromURL(u); !ok {
			response.Error(w, http.StatusBadRequest, "INVALID_URL", "URL is not managed by R2")
			return
		}
	}

	ac, ok := middleware.GetAuthContext(ctx)
	if !ok || ac.MerchantID == nil {
		response.Error(w, http.StatusBadRequest, "MERCHANT_ID_REQUIRED", "Merchant ID is required")
		return
	}

	var (
		merchantID   int64
		merchantCode string
	)
	if err := h.DB.QueryRow(ctx, `select id, code from merchants where id=$1 limit 1`, *ac.MerchantID).Scan(&merchantID, &merchantCode); err != nil {
		response.Error(w, http.StatusNotFound, "MERCHANT_NOT_FOUND", "Merchant not found")
		return
	}

	merchantCodeLower := strings.ToLower(merchantCode)
	for _, u := range urlsToValidate {
		if !strings.Contains(strings.ToLower(u), fmt.Sprintf("/merchants/%s/", merchantCodeLower)) {
			response.Error(w, http.StatusForbidden, "UNAUTHORIZED", "Cannot update this asset")
			return
		}
	}

	if body.MenuID != nil && strings.TrimSpace(*body.MenuID) != "" {
		parsedMenuID, err := strconv.ParseInt(strings.TrimSpace(*body.MenuID), 10, 64)
		if err != nil {
			response.Error(w, http.StatusBadRequest, "INVALID_MENU_ID", "menuId must be numeric")
			return
		}

		var existing struct {
			ImageURL      *string
			ImageThumbURL *string
			ThumbMeta     []byte
			HasMeta       bool
		}
		var imgURL pgtype.Text
		var thumbURL pgtype.Text
		var thumbMeta []byte
		if err := h.DB.QueryRow(ctx, `
			select image_url, image_thumb_url, image_thumb_meta
			from menus
			where id=$1 and merchant_id=$2
			limit 1
		`, parsedMenuID, merchantID).Scan(&imgURL, &thumbURL, &thumbMeta); err != nil {
			response.Error(w, http.StatusNotFound, "MENU_NOT_FOUND", "Menu not found")
			return
		}
		if imgURL.Valid {
			v := imgURL.String
			existing.ImageURL = &v
		}
		if thumbURL.Valid {
			v := thumbURL.String
			existing.ImageThumbURL = &v
		}
		if thumbMeta != nil {
			existing.ThumbMeta = thumbMeta
			existing.HasMeta = true
		}

		// Update
		if body.ImageThumbMeta != nil {
			if _, err := h.DB.Exec(ctx, `
				update menus
				set image_url=$1, image_thumb_url=$2, image_thumb_meta=$3
				where id=$4
			`, body.ImageURL, body.ImageThumbURL, []byte(*body.ImageThumbMeta), parsedMenuID); err != nil {
				response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm menu image")
				return
			}
		} else {
			if _, err := h.DB.Exec(ctx, `
				update menus
				set image_url=$1, image_thumb_url=$2
				where id=$3
			`, body.ImageURL, body.ImageThumbURL, parsedMenuID); err != nil {
				response.Error(w, http.StatusInternalServerError, "CONFIRM_FAILED", "Failed to confirm menu image")
				return
			}
		}

		urlsToDelete := make(map[string]bool)
		if existing.ImageURL != nil && *existing.ImageURL != body.ImageURL {
			urlsToDelete[*existing.ImageURL] = true
		}
		if existing.ImageThumbURL != nil && *existing.ImageThumbURL != body.ImageThumbURL {
			urlsToDelete[*existing.ImageThumbURL] = true
		}
		if existing.HasMeta {
			var prev struct {
				Variants []struct {
					URL *string `json:"url"`
				} `json:"variants"`
			}
			if err := json.Unmarshal(existing.ThumbMeta, &prev); err == nil {
				for _, v := range prev.Variants {
					if v.URL == nil || strings.TrimSpace(*v.URL) == "" {
						continue
					}
					candidate := strings.TrimSpace(*v.URL)
					if candidate != body.ImageThumbURL {
						if body.ImageThumb2xURL == nil || candidate != strings.TrimSpace(*body.ImageThumb2xURL) {
							urlsToDelete[candidate] = true
						}
					}
				}
			}
		}

		for u := range urlsToDelete {
			_ = store.DeleteURL(ctx, u)
		}
	}

	thumb2x := any(nil)
	if body.ImageThumb2xURL != nil && strings.TrimSpace(*body.ImageThumb2xURL) != "" {
		thumb2x = strings.TrimSpace(*body.ImageThumb2xURL)
	}
	thumbMeta := any(nil)
	if body.ImageThumbMeta != nil {
		thumbMeta = json.RawMessage(*body.ImageThumbMeta)
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"success": true,
		"data": map[string]any{
			"imageUrl":        body.ImageURL,
			"imageThumbUrl":   body.ImageThumbURL,
			"imageThumb2xUrl": thumb2x,
			"imageThumbMeta":  thumbMeta,
		},
		"message":    "Menu image confirmed successfully",
		"statusCode": 200,
	})
}
