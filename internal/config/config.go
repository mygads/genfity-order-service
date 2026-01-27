package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env                      string
	HTTPAddr                 string
	DatabaseURL              string
	JWTSecret                string
	JWTExpirySeconds         int64
	JWTRefreshExpirySeconds  int64
	OrderTrackingTokenSecret string
	MaxFileSizeBytes         int64
	VAPIDPublicKey           string
	RabbitMQURL              string
	RabbitMQWorkerMode       string
	CorsAllowedOrigins       []string
	WSHeartbeatInterval      time.Duration
	WSMerchantPollInterval   time.Duration
	WSCustomerPollInterval   time.Duration
	WSGroupOrderPollInterval time.Duration

	ObjectStoreEndpoint        string
	ObjectStoreRegion          string
	ObjectStoreAccessKeyID     string
	ObjectStoreSecretAccessKey string
	ObjectStoreBucket          string
	ObjectStorePublicBaseURL   string
	ObjectStoreStorageClass    string
}

func Load() Config {
	cfg := Config{
		Env:                      getEnv("APP_ENV", "development"),
		HTTPAddr:                 getEnv("HTTP_ADDR", ":8086"),
		DatabaseURL:              getEnv("DATABASE_URL", ""),
		JWTSecret:                getEnv("JWT_SECRET", ""),
		JWTExpirySeconds:         getEnvInt64("JWT_EXPIRY", 3600),
		JWTRefreshExpirySeconds:  getEnvInt64("JWT_REFRESH_EXPIRY", 604800),
		OrderTrackingTokenSecret: getEnv("ORDER_TRACKING_TOKEN_SECRET", "dev-insecure-tracking-secret"),
		MaxFileSizeBytes:         getEnvInt64("MAX_FILE_SIZE", 5*1024*1024),
		VAPIDPublicKey:           getEnv("VAPID_PUBLIC_KEY", ""),
		RabbitMQURL:              getEnv("RABBITMQ_URL", ""),
		RabbitMQWorkerMode:       getEnv("RABBITMQ_WORKER_MODE", "daemon"),
		CorsAllowedOrigins:       splitCSV(getEnv("CORS_ALLOWED_ORIGINS", "")),
		WSHeartbeatInterval:      getEnvDuration("WS_HEARTBEAT_INTERVAL", 30*time.Second),
		WSMerchantPollInterval:   getEnvDuration("WS_MERCHANT_POLL_INTERVAL", 5*time.Second),
		WSCustomerPollInterval:   getEnvDuration("WS_CUSTOMER_POLL_INTERVAL", 1*time.Second),
		WSGroupOrderPollInterval: getEnvDuration("WS_GROUP_ORDER_POLL_INTERVAL", 5*time.Second),

		// Object store (Cloudflare R2 / S3-compatible)
		ObjectStoreEndpoint:        getEnvFirst([]string{"OBJECT_STORE_ENDPOINT", "R2_S3_ENDPOINT"}, ""),
		ObjectStoreRegion:          getEnvFirst([]string{"OBJECT_STORE_REGION", "R2_REGION"}, "auto"),
		ObjectStoreAccessKeyID:     getEnvFirst([]string{"OBJECT_STORE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"}, ""),
		ObjectStoreSecretAccessKey: getEnvFirst([]string{"OBJECT_STORE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"}, ""),
		ObjectStoreBucket:          getEnvFirst([]string{"OBJECT_STORE_BUCKET", "R2_BUCKET"}, ""),
		ObjectStorePublicBaseURL:   getEnvFirst([]string{"OBJECT_STORE_PUBLIC_BASE_URL", "R2_PUBLIC_BASE_URL"}, ""),
		ObjectStoreStorageClass:    getEnvFirst([]string{"OBJECT_STORE_STORAGE_CLASS", "R2_STORAGE_CLASS"}, "STANDARD"),
	}

	if cfg.MaxFileSizeBytes <= 0 {
		cfg.MaxFileSizeBytes = 5 * 1024 * 1024
	}

	// Back-compat: allow R2_ACCOUNT_ID -> endpoint
	if strings.TrimSpace(cfg.ObjectStoreEndpoint) == "" {
		accountID := strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID"))
		if accountID != "" {
			cfg.ObjectStoreEndpoint = "https://" + accountID + ".r2.cloudflarestorage.com"
		}
	}

	return cfg
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvFirst(keys []string, fallback string) string {
	for _, k := range keys {
		value := strings.TrimSpace(os.Getenv(k))
		if value != "" {
			return value
		}
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
