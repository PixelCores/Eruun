package artifacts

import (
	"context"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"io"
)

// PutResult publishes the source and all selected delivery records atomically.
// The source is immutable per execution; duplicate identical uploads return it.
func (s *Store) PutResult(ctx context.Context, workspaceID, taskID string, policy spec.JobResultPolicy, r io.Reader, executionKey string) (*model.JobArtifact, error) {
	return s.PutResultGuarded(ctx, workspaceID, taskID, policy, r, nil, executionKey)
}
