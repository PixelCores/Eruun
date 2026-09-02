package async

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
)

// ErrExecutorClosed indicates tasks cannot be submitted after Close.
var ErrExecutorClosed = errors.New("async executor closed")

// TaskFunc is the executable unit submitted to BoundedExecutor.
type TaskFunc func()

// BoundedExecutor executes tasks with bounded queue and fixed workers.
type BoundedExecutor struct {
	name string

	tasks chan TaskFunc
	stop  chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewBoundedExecutor creates an executor with fixed worker count and queue size.
func NewBoundedExecutor(name string, workers, queue int) *BoundedExecutor {
	if workers <= 0 {
		workers = 1
	}
	if queue < 0 {
		queue = 0
	}
	if name == "" {
		name = "default"
	}

	exec := &BoundedExecutor{
		name:  name,
		tasks: make(chan TaskFunc, queue),
		stop:  make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		exec.wg.Add(1)
		go exec.worker(i)
	}
	return exec
}

func (e *BoundedExecutor) worker(workerID int) {
	defer e.wg.Done()
	for {
		select {
		case <-e.stop:
			return
		case task := <-e.tasks:
			if task == nil {
				continue
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						err := fmt.Errorf("panic: %v", r)
						klog.ErrorS(err, "bounded executor recovered panic", "executor", e.name, "worker", workerID)
					}
				}()
				task()
			}()
		}
	}
}

// Submit blocks until task is accepted into queue, context canceled, or executor closed.
func (e *BoundedExecutor) Submit(ctx context.Context, task TaskFunc) error {
	if task == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if e.closed.Load() {
		return ErrExecutorClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.stop:
		return ErrExecutorClosed
	case e.tasks <- task:
		return nil
	}
}

// Close stops workers and rejects future submissions.
func (e *BoundedExecutor) Close() {
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		close(e.stop)
		e.wg.Wait()
	})
}
