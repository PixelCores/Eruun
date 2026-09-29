package workflow

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func (w *workflowServiceImpl) appScheduleLocker() (locker.Locker, error) {
	if w.ScheduleLocker == nil {
		return nil, bcode.ErrDistributedLockUnavailable
	}
	return w.ScheduleLocker, nil
}
