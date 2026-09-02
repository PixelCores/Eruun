package application

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type blockingApplicationRepository struct {
	repository.ApplicationRepository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int32
}

func (r *blockingApplicationRepository) FindByID(ctx context.Context, id string) (*model.Applications, error) {
	r.reads.Add(1)
	blocked := false
	r.once.Do(func() {
		blocked = true
		close(r.entered)
	})
	if blocked {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.release:
		}
	}
	return r.ApplicationRepository.FindByID(ctx, id)
}

func TestUpdateVersionSerializesTheCompleteApplicationMutation(t *testing.T) {
	store := newInMemoryAppStore()
	store.apps["app-1"] = &model.Applications{ID: "app-1", Name: "shop", Version: "1.0.0"}
	svc := newMockServiceWithStore(store)
	svc.ScheduleLocker = locker.NewMemoryLocker("test-app-schedule")
	blockingRepo := &blockingApplicationRepository{
		ApplicationRepository: svc.AppRepo,
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	svc.AppRepo = blockingRepo

	firstDone := make(chan error, 1)
	go func() {
		_, err := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
			Version:  "1.1.0",
			AutoExec: boolPtr(false),
		})
		firstDone <- err
	}()

	<-blockingRepo.entered
	secondResp, secondErr := svc.UpdateVersion(context.Background(), "app-1", apisv1.UpdateVersionRequest{
		Version:  "1.2.0",
		AutoExec: boolPtr(false),
	})
	require.Nil(t, secondResp)
	require.ErrorIs(t, secondErr, bcode.ErrApplicationOperationLocked)
	require.Equal(t, int32(1), blockingRepo.reads.Load(), "the rejected request must not read mutable application state")

	close(blockingRepo.release)
	require.NoError(t, <-firstDone)
	require.Equal(t, "1.1.0", store.apps["app-1"].Version)
}
