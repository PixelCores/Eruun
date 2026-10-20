package job

import (
	"context"
	"sync"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

type Pool struct {
	Jobs        []*model.JobTask
	jobsChan    chan *model.JobTask
	ctx         context.Context
	cancel      context.CancelCauseFunc
	wg          sync.WaitGroup
	failureOnce sync.Once
	runErrOnce  sync.Once
	runErr      error
	runtime     *Runtime
}

func (p *Pool) Run() error {
	defer p.cancel(nil)
	for i := 0; i < p.runtime.Concurrency; i++ {
		go p.work()
	}
enqueue:
	for _, task := range p.Jobs {
		if p.ctx.Err() != nil {
			break
		}
		p.wg.Add(1)
		select {
		case p.jobsChan <- task:
		case <-p.ctx.Done():
			p.wg.Done()
			break enqueue
		}
	}
	// all workers return
	close(p.jobsChan)
	p.wg.Wait()
	return p.runErr
}

// The work loop for any single goroutine.
func (p *Pool) work() {
	for job := range p.jobsChan {
		if p.ctx.Err() != nil {
			p.wg.Done()
			continue
		}
		if err := runJobWithEvaluationRecovery(p.ctx, job, p.runtime); err != nil {
			p.runErrOnce.Do(func() {
				p.runErr = err
			})
			p.cancel(err)
		}
		if p.runtime.StopOnFailure && jobStatusFailed(job.Status) {
			p.failureOnce.Do(func() {
				p.cancel(nil)
			})
		}
		p.wg.Done()
	}
}

// NewPool initializes a pool using the run's concurrency and dependencies.
func NewPool(ctx context.Context, jobs []*model.JobTask, runtime *Runtime) *Pool {
	ctxForPool, cancel := context.WithCancelCause(ctx)
	return &Pool{
		Jobs:     jobs,
		jobsChan: make(chan *model.JobTask),
		ctx:      ctxForPool,
		cancel:   cancel,
		runtime:  runtime,
	}
}
