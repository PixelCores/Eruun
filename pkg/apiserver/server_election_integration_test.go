package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/event"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// This backend simulates API-server record/version atomicity, not a Kubernetes
// cluster. Each client keeps the resource version observed by its last Get/write.
type electionLeaseState struct {
	sync.Mutex
	record  *resourcelock.LeaderElectionRecord
	version uint64
}

type electionLeaseClient struct {
	state    *electionLeaseState
	identity string
	version  uint64 // protected by state.Mutex
	offline  atomic.Bool
}

var electionLeaseResource = schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}

func (l *electionLeaseClient) connected(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.offline.Load() {
		return errors.New("simulated lease connection failure")
	}
	return nil
}

func (l *electionLeaseClient) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if err := l.connected(ctx); err != nil {
		return nil, nil, err
	}
	l.state.Lock()
	defer l.state.Unlock()
	if l.state.record == nil {
		return nil, nil, apierrors.NewNotFound(electionLeaseResource, "runtime")
	}
	record := *l.state.record
	l.version = l.state.version
	raw, err := json.Marshal(record)
	return &record, raw, err
}

func (l *electionLeaseClient) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	return l.write(ctx, record, true)
}

func (l *electionLeaseClient) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	return l.write(ctx, record, false)
}

func (l *electionLeaseClient) write(ctx context.Context, record resourcelock.LeaderElectionRecord, create bool) error {
	if err := l.connected(ctx); err != nil {
		return err
	}
	l.state.Lock()
	defer l.state.Unlock()
	if create && l.state.record != nil {
		return apierrors.NewAlreadyExists(electionLeaseResource, "runtime")
	}
	if !create {
		if l.state.record == nil {
			return apierrors.NewNotFound(electionLeaseResource, "runtime")
		}
		if l.version != l.state.version {
			return apierrors.NewConflict(electionLeaseResource, "runtime", errors.New("stale resource version"))
		}
	}
	l.state.version++
	l.state.record = &record
	l.version = l.state.version
	return nil
}

func (l *electionLeaseClient) RecordEvent(string) {}
func (l *electionLeaseClient) Identity() string   { return l.identity }
func (l *electionLeaseClient) Describe() string   { return "memory/runtime" }

func TestRuntimeElectionFailoverWithRealElector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shared := &electionLeaseState{}
	servers := make([]*restServer, 2)
	locks := make([]*electionLeaseClient, 2)
	workers := make([]*testServerWorker, 2)
	done := make([]chan struct{}, 2)
	startupErrors := make(chan error, 4)
	for i, identity := range []string{"candidate-a", "candidate-b"} {
		cfg := config.NewConfig()
		cfg.LeaderConfig.ID = identity
		workers[i] = &testServerWorker{}
		servers[i] = &restServer{cfg: *cfg, eventWorkers: []event.Worker{workers[i]}}
		locks[i] = &electionLeaseClient{state: shared, identity: identity}
		done[i] = make(chan struct{})
		servers[i].startWorkers(ctx, startupErrors)
	}
	t.Cleanup(func() {
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		for i, s := range servers {
			select {
			case <-done[i]:
			case <-stopCtx.Done():
				t.Error("elector did not stop after parent cancellation")
			}
			s.stopWorkers(stopCtx)
			s.drainPromotedWorkers(stopCtx)
		}
	})
	for i, s := range servers {
		election := s.buildRuntimeLeaderElectionConfig(ctx, ctx, locks[i], startupErrors)
		election.LeaseDuration, election.RenewDeadline, election.RetryPeriod = 2*time.Second, 1300*time.Millisecond, 200*time.Millisecond
		go func() {
			defer close(done[i])
			s.runRuntimeLeaderElection(ctx, election) // Uses client-go RunOrDie unchanged.
		}()
	}
	// Request contexts also detect a term ending between the two observations.
	var overlappingAPITerms atomic.Bool
	apiLeader := func() int {
		requests := make([]context.Context, 2)
		for i, s := range servers {
			request, release, ok := s.requestLeadership(context.Background())
			if ok {
				defer release()
				requests[i] = request
			}
		}
		leader := -1
		for i, request := range requests {
			if request != nil && request.Err() == nil {
				if leader != -1 {
					overlappingAPITerms.Store(true)
					return -2
				}
				leader = i
			}
		}
		return leader
	}
	first := -1
	require.Eventually(t, func() bool {
		first = apiLeader()
		for _, s := range servers {
			if ready, _ := s.RuntimeReady(); !ready {
				return false
			}
		}
		return first >= 0
	}, time.Second, 10*time.Millisecond)
	oldRequest, release, ok := servers[first].requestLeadership(context.Background())
	require.True(t, ok)
	defer release()
	locks[first].offline.Store(true)
	require.Eventually(t, func() bool {
		leader := apiLeader()
		ready, _ := servers[first].RuntimeReady()
		return leader == 1-first && oldRequest.Err() != nil && servers[first].RuntimeRole() == "worker" && ready && workers[first].subscribes.Load() >= 2
	}, 8*time.Second, 10*time.Millisecond, "lease renewal failure must cancel the API term, resume intake, and allow takeover")

	cancel()
	for _, stopped := range done {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("election did not exit after shutdown")
		}
	}
	require.Eventually(t, func() bool {
		for _, s := range servers {
			if ready, _ := s.RuntimeReady(); ready {
				return false
			}
		}
		return apiLeader() == -1
	}, time.Second, time.Millisecond)
	for i, s := range servers {
		subscriptions := workers[i].subscribes.Load()
		s.startWorkers(ctx, startupErrors)
		require.Equal(t, subscriptions, workers[i].subscribes.Load(), "cancelled runtime must not restart intake")
	}
	require.False(t, overlappingAPITerms.Load(), "both candidates admitted API requests")
	select {
	case err := <-startupErrors:
		t.Fatalf("unexpected runtime startup error: %v", err)
	default:
	}
}
