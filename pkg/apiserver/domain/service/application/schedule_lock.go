package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type applicationMutationLockContextKey struct{}

func (c *applicationsServiceImpl) appScheduleLocker() (locker.Locker, error) {
	return schedulelock.ResolveAppScheduleLocker(c.ScheduleLocker, c.Cache)
}

func applicationMutationLockHeld(ctx context.Context, appID string) bool {
	lockedAppID, _ := ctx.Value(applicationMutationLockContextKey{}).(string)
	return strings.TrimSpace(appID) != "" && lockedAppID == strings.TrimSpace(appID)
}

func (c *applicationsServiceImpl) withWritableApplicationLock(
	ctx context.Context,
	appID string,
	operation string,
	run func(context.Context, *model.Applications) error,
) (*model.Applications, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, bcode.ErrApplicationNotExist
	}
	if c.AppRepo == nil {
		return nil, fmt.Errorf("application repository is nil")
	}

	runLocked := func(lockCtx context.Context) (*model.Applications, error) {
		app, err := c.AppRepo.FindByID(lockCtx, appID)
		if err != nil {
			if errors.Is(err, datastore.ErrRecordNotExist) {
				return nil, bcode.ErrApplicationNotExist
			}
			return nil, err
		}
		if app.EffectiveManagementMode() == config.ManagementModeObserve {
			return nil, fmt.Errorf("%w: observe applications are read-only", bcode.ErrApplicationManagementMode)
		}
		if run != nil {
			if err := run(lockCtx, app); err != nil {
				return nil, err
			}
		}
		return app, nil
	}

	if applicationMutationLockHeld(ctx, appID) {
		return runLocked(ctx)
	}
	lockProvider, err := c.appScheduleLocker()
	if err != nil {
		return nil, err
	}
	var current *model.Applications
	err = schedulelock.WithAppScheduleLock(ctx, lockProvider, appID, operation, true, func(lockCtx context.Context) error {
		lockCtx = context.WithValue(lockCtx, applicationMutationLockContextKey{}, appID)
		var runErr error
		current, runErr = runLocked(lockCtx)
		return runErr
	})
	if err != nil {
		return nil, err
	}
	return current, nil
}
