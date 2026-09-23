package artifacts

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type slowDeliveryObjects struct {
	fakeObjects
	started chan struct{}
	release chan struct{}
}

func (s *slowDeliveryObjects) Put(ctx context.Context, key string, r io.Reader, size int64, digest string) (string, error) {
	close(s.started)
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-s.release:
		return s.fakeObjects.Put(ctx, key, r, size, digest)
	}
}

func TestSlowDestinationDoesNotBlockIndependentSource(t *testing.T) {
	s, db := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	remote := &slowDeliveryObjects{started: make(chan struct{}), release: make(chan struct{})}
	s.objects = remote
	_, err := s.PutResult(ctx, "space-a", "task-a", policy(spec.JobResultTarget{Type: "minio", Mode: "full"}), bytes.NewReader(resultBytes(t)))
	require.NoError(t, err)
	require.NoError(t, db.Add(ctx, &model.WorkflowQueue{TaskID: "task-b", WorkspaceID: "space-a"}))
	_, err = s.PutResult(ctx, "space-a", "task-b", policy(spec.JobResultTarget{Type: "database", Mode: "full"}), bytes.NewReader(resultBytes(t)))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ReconcilePending(ctx, 10) }()
	t.Cleanup(func() {
		close(remote.release)
		require.NoError(t, <-done)
	})
	select {
	case <-remote.started:
	case <-ctx.Done():
		t.Fatal("remote delivery did not begin")
	}
	require.Eventually(t, func() bool {
		rows, err := s.Deliveries(ctx, "space-a", "task-b")
		return err == nil && len(rows) == 1 && rows[0].State == DeliverySucceeded
	}, time.Second, 5*time.Millisecond)
}
