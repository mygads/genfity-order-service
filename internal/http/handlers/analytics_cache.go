package handlers

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type analyticsCacheEntry struct {
	value     any
	expiresAt time.Time
}

const analyticsCacheMaxEntries = 500

var (
	analyticsCacheMu sync.Mutex
	analyticsCache   = map[string]analyticsCacheEntry{}
)

func analyticsCacheKey(prefix string, merchantID int64, parts ...string) string {
	segments := make([]string, 0, 2+len(parts))
	segments = append(segments, prefix, fmt.Sprint(merchantID))
	segments = append(segments, parts...)
	return strings.Join(segments, "|")
}

func getAnalyticsCache(key string) (any, bool) {
	analyticsCacheMu.Lock()
	defer analyticsCacheMu.Unlock()

	entry, ok := analyticsCache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(analyticsCache, key)
		return nil, false
	}
	return entry.value, true
}

func setAnalyticsCache(key string, value any, ttl time.Duration) {
	analyticsCacheMu.Lock()
	defer analyticsCacheMu.Unlock()

	analyticsCache[key] = analyticsCacheEntry{value: value, expiresAt: time.Now().Add(ttl)}
	if len(analyticsCache) > analyticsCacheMaxEntries {
		analyticsCache = map[string]analyticsCacheEntry{}
	}
}

func invalidateAnalyticsCacheForMerchant(merchantID int64, prefixes ...string) {
	analyticsCacheMu.Lock()
	defer analyticsCacheMu.Unlock()

	merchantKey := fmt.Sprint(merchantID)
	if len(prefixes) == 0 {
		for key := range analyticsCache {
			if strings.Contains(key, "|"+merchantKey+"|") || strings.HasSuffix(key, "|"+merchantKey) || strings.HasPrefix(key, merchantKey+"|") {
				delete(analyticsCache, key)
			}
		}
		return
	}

	for key := range analyticsCache {
		for _, prefix := range prefixes {
			prefixKey := prefix + "|" + merchantKey
			if strings.HasPrefix(key, prefixKey+"|") || key == prefixKey {
				delete(analyticsCache, key)
				break
			}
		}
	}
}
