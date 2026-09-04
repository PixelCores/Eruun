package systemsetting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

var (
	// ErrNotFound indicates urlSecurityPolicy is not configured in system_setting.
	ErrNotFound = errors.New("url security policy not found")
)

// Provider loads outbound URL policy from system settings with optional cache.
type Provider struct {
	store datastore.DataStore
	ttl   time.Duration
	now   func() time.Time

	mu      sync.RWMutex
	cached  *spec.URLSecurityPolicySpec
	expires time.Time
}

// NewProvider creates an URL security policy provider.
func NewProvider(store datastore.DataStore, ttl time.Duration) *Provider {
	return &Provider{
		store: store,
		ttl:   ttl,
		now:   time.Now,
	}
}

// Load returns the current urlSecurityPolicy from system_setting.
func (p *Provider) Load(ctx context.Context) (*spec.URLSecurityPolicySpec, error) {
	if p == nil || p.store == nil {
		return nil, fmt.Errorf("url security policy provider is not configured")
	}

	now := p.now()
	if cfg, ok := p.loadFromCache(now); ok {
		return cfg, nil
	}

	setting := &model.SystemSetting{Type: model.SystemSettingTypeURLSecurityPolicy}
	if err := p.store.Get(ctx, setting); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	var cfg spec.URLSecurityPolicySpec
	if err := json.Unmarshal(setting.Value, &cfg); err != nil {
		return nil, fmt.Errorf("decode urlSecurityPolicy: %w", err)
	}
	cfg = spec.NormalizeURLSecurityPolicy(cfg)
	if err := spec.ValidateURLSecurityPolicy(cfg); err != nil {
		return nil, fmt.Errorf("invalid urlSecurityPolicy: %w", err)
	}

	p.storeCache(&cfg, now)
	return cloneURLSecurityPolicy(&cfg), nil
}

func (p *Provider) loadFromCache(now time.Time) (*spec.URLSecurityPolicySpec, bool) {
	if p.ttl <= 0 {
		return nil, false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cached == nil || now.After(p.expires) {
		return nil, false
	}
	return cloneURLSecurityPolicy(p.cached), true
}

func (p *Provider) storeCache(cfg *spec.URLSecurityPolicySpec, now time.Time) {
	if p.ttl <= 0 || cfg == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cached = cloneURLSecurityPolicy(cfg)
	p.expires = now.Add(p.ttl)
}

func cloneURLSecurityPolicy(in *spec.URLSecurityPolicySpec) *spec.URLSecurityPolicySpec {
	if in == nil {
		return nil
	}
	out := *in
	if in.AllowedHostPatterns != nil {
		out.AllowedHostPatterns = append([]string(nil), in.AllowedHostPatterns...)
	}
	if in.AllowedCIDRs != nil {
		out.AllowedCIDRs = append([]string(nil), in.AllowedCIDRs...)
	}
	return &out
}

// ResolvePolicy loads the effective outbound URL policy from system settings.
func ResolvePolicy(ctx context.Context, provider *Provider) (*spec.URLSecurityPolicySpec, error) {
	if provider == nil {
		return nil, fmt.Errorf("url security policy provider is not configured")
	}
	cfg, err := provider.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load urlSecurityPolicy: %w", err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("urlSecurityPolicy is empty")
	}
	return cfg, nil
}
