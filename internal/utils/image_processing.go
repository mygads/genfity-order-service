package utils

import (
	"bytes"
	"errors"
	"image"
	"image/jpeg"
	"net/http"
	"strings"

	"github.com/disintegration/imaging"
	"github.com/rwcarlsen/goexif/exif"

	_ "image/gif"
	_ "image/png"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"
)

var allowedImageContentTypes = map[string]bool{
	"image/jpeg":    true,
	"image/jpg":     true,
	"image/png":     true,
	"image/webp":    true,
	"image/gif":     true,
	"image/bmp":     true,
	"image/svg+xml": true,
	"image/tiff":    true,
	"image/heic":    true,
	"image/heif":    true,
}

type ImageSourceMeta struct {
	Width  *int    `json:"width"`
	Height *int    `json:"height"`
	Format *string `json:"format"`
}

func ValidateImageContentType(contentType string) bool {
	ct := strings.TrimSpace(strings.ToLower(contentType))
	if ct == "" {
		return false
	}
	return allowedImageContentTypes[ct]
}

func DetectContentType(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	sample := data
	if len(sample) > 512 {
		sample = sample[:512]
	}
	return http.DetectContentType(sample)
}

func isHeifFamily(data []byte) bool {
	// HEIC/HEIF commonly use ISO BMFF: [size:4][ftyp:4][brand:4]...
	if len(data) < 12 {
		return false
	}
	if string(data[4:8]) != "ftyp" {
		return false
	}
	brand := string(data[8:12])
	switch brand {
	case "heic", "heix", "hevc", "hevx", "mif1", "msf1", "heif":
		return true
	default:
		return false
	}
}

func decodeAndAutoRotate(data []byte) (image.Image, string, error) {
	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		if isHeifFamily(data) {
			if heicImg, heicErr := decodeHEIC(data); heicErr == nil {
				return heicImg, "heic", nil
			}
		}
		return nil, "", err
	}

	// Best-effort EXIF orientation support (similar to sharp().rotate()).
	// Only JPEGs typically carry EXIF; ignore errors.
	if strings.EqualFold(format, "jpeg") {
		if ex, exErr := exif.Decode(bytes.NewReader(data)); exErr == nil {
			if tag, tagErr := ex.Get(exif.Orientation); tagErr == nil {
				if orient, convErr := tag.Int(0); convErr == nil {
					switch orient {
					case 2:
						img = imaging.FlipH(img)
					case 3:
						img = imaging.Rotate180(img)
					case 4:
						img = imaging.FlipV(img)
					case 5:
						img = imaging.Transpose(img)
					case 6:
						img = imaging.Rotate270(img)
					case 7:
						img = imaging.Transverse(img)
					case 8:
						img = imaging.Rotate90(img)
					}
				}
			}
		}
	}

	return img, format, nil
}

func EncodeJpegFitInside(data []byte, maxSide int, quality int) ([]byte, ImageSourceMeta, error) {
	if maxSide <= 0 {
		return nil, ImageSourceMeta{}, errors.New("maxSide must be > 0")
	}
	img, format, err := decodeAndAutoRotate(data)
	if err != nil {
		return nil, ImageSourceMeta{}, err
	}

	b := img.Bounds()
	w := b.Dx()
	h := b.Dy()
	meta := ImageSourceMeta{
		Width:  &w,
		Height: &h,
		Format: ptrString(format),
	}

	resized := imaging.Fit(img, maxSide, maxSide, imaging.Lanczos)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: quality}); err != nil {
		return nil, ImageSourceMeta{}, err
	}
	return buf.Bytes(), meta, nil
}

func EncodeJpegCoverSquare(data []byte, size int, quality int) ([]byte, ImageSourceMeta, error) {
	if size <= 0 {
		return nil, ImageSourceMeta{}, errors.New("size must be > 0")
	}
	img, format, err := decodeAndAutoRotate(data)
	if err != nil {
		return nil, ImageSourceMeta{}, err
	}

	b := img.Bounds()
	w := b.Dx()
	h := b.Dy()
	meta := ImageSourceMeta{
		Width:  &w,
		Height: &h,
		Format: ptrString(format),
	}

	filled := imaging.Fill(img, size, size, imaging.Center, imaging.Lanczos)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, filled, &jpeg.Options{Quality: quality}); err != nil {
		return nil, ImageSourceMeta{}, err
	}
	return buf.Bytes(), meta, nil
}

func EncodeJpegOriginal(data []byte, quality int) ([]byte, ImageSourceMeta, error) {
	img, format, err := decodeAndAutoRotate(data)
	if err != nil {
		return nil, ImageSourceMeta{}, err
	}

	b := img.Bounds()
	w := b.Dx()
	h := b.Dy()
	meta := ImageSourceMeta{
		Width:  &w,
		Height: &h,
		Format: ptrString(format),
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, ImageSourceMeta{}, err
	}
	return buf.Bytes(), meta, nil
}

func ptrString(v string) *string {
	vv := strings.TrimSpace(v)
	if vv == "" {
		return nil
	}
	return &vv
}
