package jobexec

import (
	"context"

	"k8s.io/client-go/util/retry"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// Generic get/create/update wrappers shared by multiple resource job controllers.
type getResourceFunc[T any] func(context.Context) (*T, error)
type createResourceFunc[T any] func(context.Context) (*T, error)
type onExistingResourceFunc[T any] func(context.Context, *T) error
type updateResourceFunc[T any] func(context.Context, *T) error

type SharedResourceAccessOptions struct {
	Job             *model.JobTask
	Ack             func()
	Labels          map[string]string
	Kind            domainspec.ResourceKind
	ListFn          resourceListFn
	LockProvider    locker.Locker
	LogSkip         func(domainspec.ShareStrategy)
	ReconcileShared bool
}

type TrackedResourceReconcileOptions[T any] struct {
	SharedResourceAccessOptions
	Namespace       string
	Name            string
	GetFn           getResourceFunc[T]
	CreateFn        createResourceFunc[T]
	OnExisting      onExistingResourceFunc[T]
	IsNotFound      func(error) bool
	IsAlreadyExists func(error) bool
}

// createOrUpdateResource updates an existing resource or creates a new one when missing.
func CreateOrUpdateResource[T any](
	ctx context.Context,
	getFn getResourceFunc[T],
	createFn createResourceFunc[T],
	onExisting onExistingResourceFunc[T],
	isNotFound func(error) bool,
	isAlreadyExists func(error) bool,
) (*T, bool, error) {
	existing, err := getFn(ctx)
	if err == nil {
		if onExisting != nil {
			if err := onExisting(ctx, existing); err != nil {
				return nil, false, err
			}
		}
		return existing, false, nil
	}
	if !isNotFound(err) {
		return nil, false, err
	}
	created, err := createFn(ctx)
	if err == nil {
		return created, true, nil
	}
	if isAlreadyExists(err) {
		existing, err := getFn(ctx)
		if err != nil {
			return nil, false, err
		}
		if onExisting != nil {
			if err := onExisting(ctx, existing); err != nil {
				return nil, false, err
			}
		}
		return existing, false, nil
	}
	return nil, false, err
}

// createOrUpdateTrackedResource wraps createOrUpdateResource and records resource ownership.
func CreateOrUpdateTrackedResource[T any](
	ctx context.Context,
	kind domainspec.ResourceKind,
	namespace string,
	name string,
	getFn getResourceFunc[T],
	createFn createResourceFunc[T],
	onExisting onExistingResourceFunc[T],
	isNotFound func(error) bool,
	isAlreadyExists func(error) bool,
) (*T, bool, error) {
	res, created, err := CreateOrUpdateResource(ctx, getFn, createFn, onExisting, isNotFound, isAlreadyExists)
	if err != nil {
		return nil, false, err
	}
	if created {
		MarkResourceCreated(ctx, kind, namespace, name)
	} else {
		MarkResourceObserved(ctx, kind, namespace, name)
	}
	return res, created, nil
}

func ResolveSharedResourceAccess(ctx context.Context, opts SharedResourceAccessOptions) (func(), bool, error) {
	if opts.ReconcileShared {
		_, strategy := ShareInfoFromLabels(opts.Labels)
		if strategy != domainspec.ShareStrategyIgnore {
			return nil, false, nil
		}
	}
	return handleSharedResource(ctx, opts.Job, opts.Ack, opts.Labels, opts.Kind, opts.ListFn, opts.LockProvider, opts.LogSkip)
}

func ReconcileTrackedResource[T any](ctx context.Context, opts TrackedResourceReconcileOptions[T]) (*T, bool, error) {
	unlock, skipped, err := ResolveSharedResourceAccess(ctx, opts.SharedResourceAccessOptions)
	if err != nil {
		return nil, false, err
	}
	if unlock != nil {
		defer unlock()
	}
	if skipped {
		return nil, false, nil
	}
	return CreateOrUpdateTrackedResource(ctx, opts.Kind, opts.Namespace, opts.Name, opts.GetFn, opts.CreateFn, opts.OnExisting, opts.IsNotFound, opts.IsAlreadyExists)
}

func TrackResourcePresence[T any](ctx context.Context, kind domainspec.ResourceKind, namespace, name string, getFn getResourceFunc[T], isNotFound func(error) bool) error {
	existing, err := getFn(ctx)
	if err != nil {
		if isNotFound(err) {
			MarkResourceCreated(ctx, kind, namespace, name)
			return nil
		}
		return err
	}
	if existing != nil {
		MarkResourceObserved(ctx, kind, namespace, name)
	}
	return nil
}

// updateResourceWithRetry wraps a get+update sequence with conflict retries.
func UpdateResourceWithRetry[T any](
	ctx context.Context,
	getFn getResourceFunc[T],
	updateFn updateResourceFunc[T],
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := getFn(ctx)
		if err != nil {
			return err
		}
		return updateFn(ctx, latest)
	})
}
