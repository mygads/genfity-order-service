package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
)

func base64UrlEncode(input []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(input), "=")
}

func base64UrlDecode(input string) ([]byte, error) {
	padded := input
	if m := len(input) % 4; m != 0 {
		padded += strings.Repeat("=", 4-m)
	}
	return base64.URLEncoding.DecodeString(padded)
}

func CreateOrderTrackingToken(secret, merchantCode, orderNumber string) string {
	payload := merchantCode + ":" + orderNumber
	payloadB64 := base64UrlEncode([]byte(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payloadB64))
	sig := mac.Sum(nil)
	sigB64 := base64UrlEncode(sig)
	return payloadB64 + "." + sigB64
}

func VerifyOrderTrackingToken(secret, token, merchantCode, orderNumber string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return false
	}
	payloadB64 := parts[0]
	sigB64 := parts[1]

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payloadB64))
	expected := mac.Sum(nil)

	actual, err := base64UrlDecode(sigB64)
	if err != nil {
		return false
	}
	if len(actual) != len(expected) {
		return false
	}
	if !hmac.Equal(actual, expected) {
		return false
	}

	payloadRaw, err := base64UrlDecode(payloadB64)
	if err != nil {
		return false
	}
	return string(payloadRaw) == merchantCode+":"+orderNumber
}
