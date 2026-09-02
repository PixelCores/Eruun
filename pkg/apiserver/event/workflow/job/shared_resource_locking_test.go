package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestGuardSharedResource_DefaultUsesLabelSelector(t *testing.T) {
	shareName := "demo-ns"
	var gotSelector string

	lockProvider := locker.NewNoopLocker(shareLockerPrefix)
	unlock, skipped, err := resolveSharedResource(context.Background(), shareName, domainspec.ShareStrategyDefault, domainspec.ResourceService, func(_ context.Context, opts metav1.ListOptions) (int, error) {
		gotSelector = opts.LabelSelector
		return 0, nil
	}, lockProvider)
	require.NoError(t, err)
	require.False(t, skipped)
	require.NotNil(t, unlock)

	wantSelector := labels.Set{config.LabelShareName: shareName}.String()
	require.Equal(t, wantSelector, gotSelector)

	unlock()
}

func TestGuardSharedResource_DefaultRequiresLocker(t *testing.T) {
	unlock, skipped, err := resolveSharedResource(context.Background(), "demo-ns", domainspec.ShareStrategyDefault, domainspec.ResourceService, func(context.Context, metav1.ListOptions) (int, error) {
		t.Fatal("list should not be called when locker is unavailable")
		return 0, nil
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "shared resource locker unavailable")
	require.False(t, skipped)
	require.Nil(t, unlock)
}

func TestGuardSharedResource_IgnoreSkips(t *testing.T) {
	lockProvider := locker.NewNoopLocker(shareLockerPrefix)
	unlock, skipped, err := resolveSharedResource(context.Background(), "demo-ns", domainspec.ShareStrategyIgnore, domainspec.ResourceService, func(context.Context, metav1.ListOptions) (int, error) {
		t.Fatal("list should not be called for ignore strategy")
		return 0, nil
	}, lockProvider)
	require.NoError(t, err)
	require.True(t, skipped)
	require.Nil(t, unlock)
}
