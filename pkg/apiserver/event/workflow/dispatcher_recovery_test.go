package workflow

import (
	"context"
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

func (s *admissionFailureWorkflowService) WaitingTasks(ctx context.Context) ([]*model.WorkflowQueue, error) {
	select {
	case s.waitingCalled <- struct{}{}:
	case <-ctx.Done():
	}
	return nil, nil
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
