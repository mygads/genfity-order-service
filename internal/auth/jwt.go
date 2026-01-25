package auth

import (
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type UserRole string

const (
	RoleSuperAdmin    UserRole = "SUPER_ADMIN"
	RoleMerchantOwner UserRole = "MERCHANT_OWNER"
	RoleMerchantStaff UserRole = "MERCHANT_STAFF"
	RoleDelivery      UserRole = "DELIVERY"
	RoleCustomer      UserRole = "CUSTOMER"
	RoleInfluencer    UserRole = "INFLUENCER"
)

type Claims struct {
	UserID     string   `json:"userId"`
	SessionID  string   `json:"sessionId"`
	Role       UserRole `json:"role"`
	Email      string   `json:"email"`
	MerchantID *string  `json:"merchantId,omitempty"`
	CustomerID *string  `json:"customerId,omitempty"`
	Name       *string  `json:"name,omitempty"`
	jwt.RegisteredClaims
}

func ParseBearerToken(authHeader string) string {
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func VerifyAccessToken(tokenString string, secret string) (*Claims, error) {
	if tokenString == "" {
		return nil, errors.New("token required")
	}

	claims := &Claims{}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256"}))
	_, err := parser.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}

	if claims.ExpiresAt == nil || claims.ExpiresAt.Time.Before(time.Now()) {
		return nil, errors.New("token expired")
	}
	return claims, nil
}
