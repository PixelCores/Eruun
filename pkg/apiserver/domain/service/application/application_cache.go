package application

import (
	"context"
	"encoding/json"
	"strings"

	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	cacheutil "github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
	"k8s.io/klog/v2"
)

const (
	applicationListCacheKey         = "app:list:v4"
	templateApplicationListCacheKey = "app:template:list:v5"
)

// ApplicationComponentsCacheKey returns the cache key for application components query.
func ApplicationComponentsCacheKey(appID string) string {
	return applicationComponentsCacheKey(appID)
}

func applicationComponentsCacheKey(appID string) string {
	return cacheutil.ApplicationComponentsKey(appID)
}

func (c *applicationsServiceImpl) cacheEnabled() bool {
	return c.Cache != nil && !c.Cache.IsCacheDisabled()
}

func (c *applicationsServiceImpl) loadJSONCache(key string, out interface{}) bool {
	if !c.cacheEnabled() || strings.TrimSpace(key) == "" {
		return false
	}
	raw, err := c.Cache.Load(key)
	if err != nil {
		klog.V(4).Infof("load cache key %s failed: %v", key, err)
		return false
	}
	if raw == "" {
		return false
	}
	if err := json.Unmarshal([]byte(raw), out); err != nil {
		klog.Warningf("decode cache key %s failed, dropping corrupted entry: %v", key, err)
		if delErr := c.Cache.Delete(key); delErr != nil {
			klog.V(4).Infof("delete corrupted cache key %s failed: %v", key, delErr)
		}
		return false
	}
	return true
}

func (c *applicationsServiceImpl) storeJSONCache(key string, value interface{}) {
	if !c.cacheEnabled() || strings.TrimSpace(key) == "" {
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		klog.V(4).Infof("encode cache key %s failed: %v", key, err)
		return
	}
	if err := c.Cache.Store(key, string(raw)); err != nil {
		klog.V(4).Infof("store cache key %s failed: %v", key, err)
	}
}

func (c *applicationsServiceImpl) invalidateCacheKey(key string) {
	if !c.cacheEnabled() || strings.TrimSpace(key) == "" {
		return
	}
	if err := c.Cache.Delete(key); err != nil {
		klog.V(4).Infof("invalidate cache key %s failed: %v", key, err)
	}
}

func (c *applicationsServiceImpl) invalidateApplicationListCaches(ctx context.Context) {
	c.invalidateCacheKey(scopedListCacheKey(ctx, applicationListCacheKey))
	c.invalidateCacheKey(scopedListCacheKey(ctx, templateApplicationListCacheKey))
}

func (c *applicationsServiceImpl) invalidateApplicationComponentsCache(appID string) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return
	}
	c.invalidateCacheKey(applicationComponentsCacheKey(appID))
}

func scopedListCacheKey(ctx context.Context, key string) string {
	if scope, ok := access.FromContext(ctx); ok {
		return key + ":workspace:" + scope.WorkspaceID
	}
	return key
}
