//go:build linux && cgo

package utils

import (
	"bytes"
	"image"

	"github.com/jdeng/goheif"
)

func decodeHEIC(data []byte) (image.Image, error) {
	return goheif.Decode(bytes.NewReader(data))
}
