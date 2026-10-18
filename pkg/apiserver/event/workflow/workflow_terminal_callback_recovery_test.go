package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	wf "github.com/PixelCores/Eruun/pkg/apiserver/workflow"
)

type terminalCallbackStarvationStore struct {
	*controllerTestStore
	recoveryMu sync.Mutex
	tasks      []*model.WorkflowQueue
	attempts   map[string]int
	reconciled chan struct{}
}

func (s *terminalCallbackStarvationStore) List(ctx context.Context, query datastore.Entity, opts *datastore.ListOptions) ([]datastore.Entity, error) {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	switch q := query.(type) {
	case *model.WorkflowQueue:
		var pending []datastore.Entity
		for _, task := range s.tasks {
			if wf.IsTerminalCallbackPending(task.SchedulingReason) {
				copy := *task
				pending = append(pending, &copy)
			}
		}
		start := (opts.Page - 1) * opts.PageSize
		if start >= len(pending) {
			return nil, nil
		}
		end := min(start+opts.PageSize, len(pending))
		return pending[start:end], nil
	case *model.JobInfo:
		if q.TaskID != "task-09" {
			return []datastore.Entity{&model.JobInfo{TaskID: q.TaskID, Type: string(config.JobCleanupResources), Status: string(config.StatusRunning)}}, nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected recovery query %T", query)
}

func (s *terminalCallbackStarvationStore) Get(ctx context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.controllerTestStore.Get(ctx, entity)
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	for _, current := range s.tasks {
		if current.TaskID != task.TaskID {
			continue
		}
		*task = *current
		s.attempts[task.TaskID]++
		return nil
	}
	return datastore.ErrRecordNotExist
}

func (s *terminalCallbackStarvationStore) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions, updates map[string]interface{}) (bool, error) {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.controllerTestStore.CompareAndSwapWithConditions(ctx, entity, conditions, updates)
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	for _, current := range s.tasks {
		if current.TaskID != task.TaskID {
			continue
		}
		for field, expected := range conditions {
			if field == "scheduling_reason" {
				if current.SchedulingReason != expected {
					return false, nil
				}
			} else if !matchWorkflowQueueField(current, field, expected) {
				return false, nil
			}
		}
		for field, value := range updates {
			if field == "scheduling_reason" {
				current.SchedulingReason = value.(string)
			} else {
				applyWorkflowQueueUpdate(current, field, value)
			}
		}
		if current.TaskID == "task-09" && current.SchedulingReason == wf.TerminalCallbackReconciledReason {
			close(s.reconciled)
		}
		return true, nil
	}
	return false, nil
}

func TestTerminalCallbackRecoveryDoesNotStarveTasksBehindActiveChildren(t *testing.T) {
	store := &terminalCallbackStarvationStore{
		controllerTestStore: &controllerTestStore{},
		attempts:            make(map[string]int), reconciled: make(chan struct{}),
	}
	for i := 1; i <= 9; i++ {
		store.tasks = append(store.tasks, &model.WorkflowQueue{
			TaskID: fmt.Sprintf("task-%02d", i), Status: config.StatusCancelled,
			SchedulingReason: wf.TerminalCallbackPendingReason("manual cancel"),
		})
	}
	cfg := &config.Config{}
	cfg.Workflow.LeaseReaperInterval = time.Millisecond
	runtime := &Workflow{Store: store, Cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	runtime.startTerminalCallbackRecovery(ctx, &wg)
	select {
	case <-store.reconciled:
	case <-time.After(3 * time.Second):
		cancel()
		wg.Wait()
		store.recoveryMu.Lock()
		defer store.recoveryMu.Unlock()
		t.Fatalf("ninth settled task was not reconciled: attempts=%v", store.attempts)
	}
	cancel()
	wg.Wait()
	store.recoveryMu.Lock()
	defer store.recoveryMu.Unlock()
	if store.tasks[8].SchedulingReason != wf.TerminalCallbackReconciledReason {
		t.Fatalf("ninth settled task starved behind active children: attempts=%v reason=%q", store.attempts, store.tasks[8].SchedulingReason)
	}
}
