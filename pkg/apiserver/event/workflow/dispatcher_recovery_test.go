package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

type admissionFailureWorkflowService struct {
	stubWorkflowService
	waitingCalled chan struct{}
}

func (s *admissionFailureWorkflowService) WaitingTasks(ctx context.Context, _ int) ([]*model.WorkflowQueue, int, error) {
	select {
	case s.waitingCalled <- struct{}{}:
	case <-ctx.Done():
	}
	return nil, 1, nil
}

func TestDispatcherStillChecksRecoverableWorkAfterAdmissionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.NewConfig()
	cfg.Workflow.DispatchPollInterval = time.Millisecond
	service := &admissionFailureWorkflowService{waitingCalled: make(chan struct{}, 1)}
	// The absent transactional store causes admission to fail before any new
	// resource can receive a permit. Dispatch must still consult its own source.
	w := &Workflow{Cfg: cfg, WorkflowService: service}
	done := make(chan struct{})
	go func() { defer close(done); w.Dispatcher(ctx) }()
	select {
	case <-service.waitingCalled:
	case <-time.After(time.Second):
		t.Fatal("admission failure suppressed workflow recovery dispatch")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

type dispatchAdmissionWorkflowService struct {
	stubWorkflowService
	task    *model.WorkflowQueue
	err     error
	waiting func(context.Context, int) ([]*model.WorkflowQueue, int, error)
	claim   func(*model.WorkflowQueue) (*model.WorkflowQueue, bool, error)
}

func (s *dispatchAdmissionWorkflowService) ClaimTaskForDispatch(_ context.Context, task *model.WorkflowQueue, _ time.Duration) (*model.WorkflowQueue, bool, error) {
	if s.claim != nil {
		return s.claim(task)
	}
	return s.task, s.task != nil, s.err
}

func (s *dispatchAdmissionWorkflowService) WaitingTasks(ctx context.Context, page int) ([]*model.WorkflowQueue, int, error) {
	return s.waiting(ctx, page)
}

func TestClaimAndProcessTaskUsesServiceAdmission(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied", true: "admitted"}[admitted], func(t *testing.T) {
			svc := &dispatchAdmissionWorkflowService{err: errors.New("application admission denied")}
			if admitted {
				svc.err = nil
				svc.task = &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusQueued, RunGeneration: 3, RunToken: "claimed-token"}
			}
			w := &Workflow{WorkflowService: svc}
			var processed *model.WorkflowQueue

			w.claimAndProcessTask(context.Background(), &model.WorkflowQueue{TaskID: "task-1", Status: config.StatusWaiting, RunGeneration: 2}, func(_ context.Context, task *model.WorkflowQueue) error {
				processed = task
				return nil
			})

			if admitted {
				require.Same(t, svc.task, processed)
			} else {
				require.Nil(t, processed)
			}
		})
	}
}

func TestDispatcherAdvancesPastFullRejectedPage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var pages []int
	blocked := make([]*model.WorkflowQueue, 100)
	for i := range blocked {
		blocked[i] = &model.WorkflowQueue{TaskID: fmt.Sprintf("blocked-%d", i), Status: config.StatusWaiting}
	}
	denied := 0
	svc := &dispatchAdmissionWorkflowService{
		waiting: func(_ context.Context, page int) ([]*model.WorkflowQueue, int, error) {
			pages = append(pages, page)
			if len(pages) == 3 {
				cancel()
				return nil, 1, ctx.Err()
			}
			if page == 1 {
				return blocked, 2, nil
			}
			return []*model.WorkflowQueue{{TaskID: "healthy", Status: config.StatusWaiting}}, 1, nil
		},
		claim: func(task *model.WorkflowQueue) (*model.WorkflowQueue, bool, error) {
			if task.TaskID != "healthy" {
				denied++
				return nil, false, errors.New("pending StatefulSet cleanup")
			}
			return &model.WorkflowQueue{TaskID: task.TaskID, Status: config.StatusQueued, RunGeneration: 1, RunToken: "healthy-token"}, true, nil
		},
	}
	cfg := config.NewConfig()
	cfg.Workflow.DispatchPollInterval = time.Millisecond
	queue := &fakeAckQueue{}
	w := &Workflow{Cfg: cfg, WorkflowService: svc, Queue: queue}

	w.Dispatcher(ctx)

	require.Equal(t, []int{1, 2, 1}, pages)
	require.Equal(t, 100, denied)
	require.Len(t, queue.enqueued, 1)
	dispatch, err := UnmarshalTaskDispatch(queue.enqueued[0])
	require.NoError(t, err)
	require.Equal(t, "healthy", dispatch.TaskID)
}

func TestDispatcherStopsBeforeClaimWhenPageQueryIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claimed := false
	svc := &dispatchAdmissionWorkflowService{
		waiting: func(context.Context, int) ([]*model.WorkflowQueue, int, error) {
			cancel()
			return []*model.WorkflowQueue{{TaskID: "task-1"}}, 2, nil
		},
		claim: func(*model.WorkflowQueue) (*model.WorkflowQueue, bool, error) {
			claimed = true
			return nil, false, nil
		},
	}
	cfg := config.NewConfig()
	cfg.Workflow.DispatchPollInterval = time.Millisecond
	w := &Workflow{Cfg: cfg, WorkflowService: svc}

	w.Dispatcher(ctx)

	require.False(t, claimed)
}
