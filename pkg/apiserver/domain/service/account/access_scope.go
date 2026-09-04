package account

import (
	"context"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

type Scope struct {
	UserID           string
	WorkspaceID      string
	Namespace        string
	Role             string
	SystemAdmin      bool
	ClusterOperation bool
}
type contextKey struct{}

func WithScope(ctx context.Context, s Scope) context.Context {
	return context.WithValue(ctx, contextKey{}, s)
}
func FromContext(ctx context.Context) (Scope, bool) {
	s, ok := ctx.Value(contextKey{}).(Scope)
	return s, ok
}
func ForWorkspace(w *model.Workspace) Scope {
	return Scope{WorkspaceID: w.ID, Namespace: w.Namespace, Role: "member"}
}
