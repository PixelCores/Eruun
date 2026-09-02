package job

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

type sharedResourceAccessOptions struct {
	job             *model.JobTask
	ack             func()
	labels          map[string]string
	kind            domainspec.ResourceKind
	listFn          resourceListFn
	lockProvider    locker.Locker
	logSkip         func(domainspec.ShareStrategy)
	reconcileShared bool
}

type trackedResourceReconcileOptions[T any] struct {
	sharedResourceAccessOptions
	namespace       string
	name            string
	getFn           getResourceFunc[T]
	createFn        createResourceFunc[T]
	onExisting      onExistingResourceFunc[T]
	isNotFound      func(error) bool
	isAlreadyExists func(error) bool
}

// getOrCreateResource returns an existing resource or creates a new one when missing.
func getOrCreateResource[T any](
	ctx context.Context,
	getFn getResourceFunc[T],
	createFn createResourceFunc[T],
	isNotFound func(error) bool,
	isAlreadyExists func(error) bool,
) (*T, bool, error) {
	existing, err := getFn(ctx)
	if err == nil {
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
		return existing, false, nil
	}
	return nil, false, err
}

// createOrUpdateResource updates an existing resource or creates a new one when missing.
func createOrUpdateResource[T any](
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
func createOrUpdateTrackedResource[T any](
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
	res, created, err := createOrUpdateResource(ctx, getFn, createFn, onExisting, isNotFound, isAlreadyExists)
	if err != nil {
		return nil, false, err
	}
	if created {
		MarkResourceCreated(ctx, kind, namespace, name)
	} else {
		markResourceObserved(ctx, kind, namespace, name)
	}
	return res, created, nil
}

func resolveSharedResourceAccess(ctx context.Context, opts sharedResourceAccessOptions) (func(), bool, error) {
	if opts.reconcileShared {
		_, strategy := shareInfoFromLabels(opts.labels)
		if strategy != domainspec.ShareStrategyIgnore {
			return nil, false, nil
		}
	}
	return handleSharedResource(ctx, opts.job, opts.ack, opts.labels, opts.kind, opts.listFn, opts.lockProvider, opts.logSkip)
}

func reconcileTrackedResource[T any](ctx context.Context, opts trackedResourceReconcileOptions[T]) (*T, bool, error) {
	unlock, skipped, err := resolveSharedResourceAccess(ctx, opts.sharedResourceAccessOptions)
	if err != nil {
		return nil, false, err
	}
	if unlock != nil {
		defer unlock()
	}
	if skipped {
		return nil, false, nil
	}
	return createOrUpdateTrackedResource(ctx, opts.kind, opts.namespace, opts.name, opts.getFn, opts.createFn, opts.onExisting, opts.isNotFound, opts.isAlreadyExists)
}

func trackResourcePresence[T any](ctx context.Context, kind domainspec.ResourceKind, namespace, name string, getFn getResourceFunc[T], isNotFound func(error) bool) error {
	existing, err := getFn(ctx)
	if err != nil {
		if isNotFound(err) {
			MarkResourceCreated(ctx, kind, namespace, name)
			return nil
		}
		return err
	}
	if existing != nil {
		markResourceObserved(ctx, kind, namespace, name)
	}
	return nil
}

// updateResourceWithRetry wraps a get+update sequence with conflict retries.
func updateResourceWithRetry[T any](
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
