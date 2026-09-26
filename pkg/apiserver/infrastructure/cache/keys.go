package cache

import "strings"

const (
	applicationComponentsKeyPrefix = "app:components:v6:"
)

// ApplicationComponentsKey returns the cache key for application components query.
func ApplicationComponentsKey(appID string) string {
	return applicationComponentsKeyPrefix + strings.TrimSpace(appID)
}
