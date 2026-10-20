package jobexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestCleanupClusterResources(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	MarkResourceCreated(ctx, domainspec.ResourceClusterRole, "", "role-a")
	MarkResourceObserved(ctx, domainspec.ResourceClusterRole, "", "role-b")

	var deleted []string
	cleanupClusterResources(ctx, domainspec.ResourceClusterRole, time.Second, "clusterrole", func(_ context.Context, name string) error {
		deleted = append(deleted, name)
		return nil
	}, nil)
	require.Equal(t, []string{"role-a"}, deleted)

	cleanupClusterResources(ctx, domainspec.ResourceClusterRole, time.Second, "clusterrole", func(context.Context, string) error {
		return errors.New("not found")
	}, func(err error) bool {
		return err != nil && err.Error() == "not found"
	})
}
