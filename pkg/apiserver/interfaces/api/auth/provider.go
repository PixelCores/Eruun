package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// SystemSettingPolicyProvider loads apiAuth policy from system settings with optional caching.
type SystemSettingPolicyProvider struct {
	repo repository.SystemSettingRepository
	ttl  time.Duration
	now  func() time.Time

	mu      sync.RWMutex
	cached  *spec.APIAuthSettingSpec
	expires time.Time
}

// NewSystemSettingPolicyProvider creates a provider backed by SystemSettingRepository.
func NewSystemSettingPolicyProvider(repo repository.SystemSettingRepository, ttl time.Duration) *SystemSettingPolicyProvider {
	return &SystemSettingPolicyProvider{
		repo: repo,
		ttl:  ttl,
		now:  time.Now,
	}
}

// Load returns the latest apiAuth policy.
func (p *SystemSettingPolicyProvider) Load(ctx context.Context) (*spec.APIAuthSettingSpec, error) {
	if p == nil || p.repo == nil {
		return nil, fmt.Errorf("policy provider not configured")
	}

	now := p.now()
	if cfg, ok := p.loadFromCache(now); ok {
		return cfg, nil
	}

	setting, err := p.repo.FindByType(ctx, model.SystemSettingTypeAPIAuth)
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, ErrPolicyNotFound
		}
		return nil, err
	}

	var cfg spec.APIAuthSettingSpec
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		return nil, fmt.Errorf("decode apiAuth setting: %w", err)
	}
	cfg = spec.NormalizeAPIAuthSetting(cfg)
	if err := spec.ValidateAPIAuthSetting(cfg); err != nil {
		return nil, fmt.Errorf("invalid apiAuth setting: %w", err)
	}

	p.storeCache(&cfg, now)
	return &cfg, nil
}

func (p *SystemSettingPolicyProvider) loadFromCache(now time.Time) (*spec.APIAuthSettingSpec, bool) {
	if p.ttl <= 0 {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cached == nil || now.After(p.expires) {
		return nil, false
	}
	return p.cached, true
}

func (p *SystemSettingPolicyProvider) storeCache(cfg *spec.APIAuthSettingSpec, now time.Time) {
	if p.ttl <= 0 || cfg == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cached = cfg
	p.expires = now.Add(p.ttl)
}
