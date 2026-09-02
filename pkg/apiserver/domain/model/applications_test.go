package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestApplicationComponentShareStrategy(t *testing.T) {
	tests := []struct {
		name         string
		component    *ApplicationComponent
		wantStrategy domainspec.ShareStrategy
		wantShared   bool
	}{
		{name: "nil component"},
		{name: "missing traits", component: &ApplicationComponent{}},
		{
			name: "default",
			component: &ApplicationComponent{Traits: &JSONStruct{
				"share": map[string]interface{}{"strategy": string(domainspec.ShareStrategyDefault)},
			}},
			wantStrategy: domainspec.ShareStrategyDefault,
			wantShared:   true,
		},
		{
			name: "ignore",
			component: &ApplicationComponent{Traits: &JSONStruct{
				"share": map[string]interface{}{"strategy": string(domainspec.ShareStrategyIgnore)},
			}},
			wantStrategy: domainspec.ShareStrategyIgnore,
			wantShared:   true,
		},
		{
			name: "force",
			component: &ApplicationComponent{Traits: &JSONStruct{
				"share": map[string]interface{}{"strategy": string(domainspec.ShareStrategyForce)},
			}},
			wantStrategy: domainspec.ShareStrategyForce,
			wantShared:   true,
		},
		{
			name: "unknown falls back to default",
			component: &ApplicationComponent{Traits: &JSONStruct{
				"share": map[string]interface{}{"strategy": "future-default"},
			}},
			wantStrategy: domainspec.ShareStrategyDefault,
			wantShared:   true,
		},
		{
			name: "malformed traits",
			component: &ApplicationComponent{Traits: &JSONStruct{
				"share": make(chan int),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, shared := tt.component.ShareStrategy()
			require.Equal(t, tt.wantStrategy, strategy)
			require.Equal(t, tt.wantShared, shared)
			require.Equal(t, tt.wantShared, tt.component.ShareEnabled())
		})
	}
}
