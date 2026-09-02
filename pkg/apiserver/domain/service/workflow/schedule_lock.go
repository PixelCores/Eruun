package workflow

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/schedulelock"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func (w *workflowServiceImpl) appScheduleLocker() (locker.Locker, error) {
	return schedulelock.ResolveAppScheduleLocker(w.ScheduleLocker, w.Cache)
}
