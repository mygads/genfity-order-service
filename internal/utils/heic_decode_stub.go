//go:build !linux || !cgo

package utils

import (
	"errors"
	"image"
)

func decodeHEIC(_ []byte) (image.Image, error) {
	return nil, errors.New("heic decoding not supported in this build")
}
