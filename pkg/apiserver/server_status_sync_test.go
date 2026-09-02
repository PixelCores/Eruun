package apiserver

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type statusSyncStore struct {
	listResult  []datastore.Entity
	listErr     error
	putErr      error
	casErr      error
	casNotFound bool
	exists      bool
	existsErr   error

	listQuery   *model.ApplicationComponent
	listOptions *datastore.ListOptions

	putCalls int
	putComp  *model.ApplicationComponent

	casCalls      int
	casComp       *model.ApplicationComponent
	casConditions map[string]interface{}
	casField      string
	casValue      interface{}
	casUpdates    map[string]interface{}
}

func (s *statusSyncStore) Add(context.Context, datastore.Entity) error {
	return nil
}

func (s *statusSyncStore) BatchAdd(context.Context, []datastore.Entity) error {
	return nil
}

func (s *statusSyncStore) Put(_ context.Context, entity datastore.Entity) error {
	s.putCalls++
	component, ok := entity.(*model.ApplicationComponent)
	if ok && component != nil {
		copied := *component
		s.putComp = &copied
	}
	return s.putErr
}

func (s *statusSyncStore) Delete(context.Context, datastore.Entity) error {
	return nil
}

func (s *statusSyncStore) DeleteByFilter(context.Context, datastore.Entity, *datastore.FilterOptions) error {
	return nil
}

func (s *statusSyncStore) Get(context.Context, datastore.Entity) error {
	return nil
}

func (s *statusSyncStore) List(_ context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	component, ok := query.(*model.ApplicationComponent)
	if ok && component != nil {
		copied := *component
		s.listQuery = &copied
	}
	if options != nil {
		copied := *options
		s.listOptions = &copied
	}
	return s.listResult, s.listErr
}

func (s *statusSyncStore) Count(context.Context, datastore.Entity, *datastore.FilterOptions) (int64, error) {
	return 0, nil
}

func (s *statusSyncStore) IsExist(context.Context, datastore.Entity) (bool, error) {
	return false, nil
}

func (s *statusSyncStore) IsExistByCondition(context.Context, string, map[string]interface{}, interface{}) (bool, error) {
	return s.exists, s.existsErr
}

func (s *statusSyncStore) CompareAndSwap(context.Context, datastore.Entity, string, interface{}, map[string]interface{}) (bool, error) {
	return false, nil
}

func (s *statusSyncStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.casCalls++
	component, ok := entity.(*model.ApplicationComponent)
	if ok && component != nil {
		copied := *component
		s.casComp = &copied
	}
	s.casConditions = conditions
	s.casUpdates = updates
	if s.casErr != nil {
		return false, s.casErr
	}
	return !s.casNotFound, nil
}

type concurrentStatusSyncStore struct {
	*statusSyncStore

	mu             sync.Mutex
	component      model.ApplicationComponent
	casStarted     chan struct{}
	casRelease     chan struct{}
	casStartedOnce sync.Once
	casCalls       int
}

func newConcurrentStatusSyncStore(component model.ApplicationComponent) *concurrentStatusSyncStore {
	return &concurrentStatusSyncStore{
		statusSyncStore: &statusSyncStore{},
		component:       component,
		casStarted:      make(chan struct{}),
		casRelease:      make(chan struct{}),
	}
}

func (s *concurrentStatusSyncStore) List(_ context.Context, query datastore.Entity, _ *datastore.ListOptions) ([]datastore.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	componentQuery, ok := query.(*model.ApplicationComponent)
	if !ok || componentQuery.AppID != s.component.AppID {
		return nil, nil
	}
	component := s.component
	return []datastore.Entity{&component}, nil
}

func (s *concurrentStatusSyncStore) IsExistByCondition(_ context.Context, _ string, conditions map[string]interface{}, _ interface{}) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.matchesConditionsLocked(conditions), nil
}

func (s *concurrentStatusSyncStore) CompareAndSwapWithConditions(ctx context.Context, _ datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	s.mu.Lock()
	s.casCalls++
	s.mu.Unlock()
	s.casStartedOnce.Do(func() { close(s.casStarted) })

	select {
	case <-s.casRelease:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchesConditionsLocked(conditions) {
		return false, nil
	}
	for field, value := range updates {
		switch field {
		case "status":
			s.component.Status = value.(string)
		case "ready_replicas":
			s.component.ReadyReplicas = value.(int32)
		case "last_abnormal":
			s.component.LastAbnormal = value.(string)
		}
	}
	s.component.UpdateTime = time.Now()
	return true, nil
}

func (s *concurrentStatusSyncStore) matchesConditionsLocked(conditions map[string]interface{}) bool {
	current := map[string]interface{}{
		"app_id":         s.component.AppID,
		"id":             s.component.ID,
		"name":           s.component.Name,
		"status":         s.component.Status,
		"ready_replicas": s.component.ReadyReplicas,
		"last_abnormal":  s.component.LastAbnormal,
		"update_time":    s.component.UpdateTime,
	}
	for field, expected := range conditions {
		if !reflect.DeepEqual(current[field], expected) {
			return false
		}
	}
	return true
}

func (s *concurrentStatusSyncStore) markStopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.component.Status = string(config.ComponentStatusStopped)
	s.component.ReadyReplicas = 0
	s.component.LastAbnormal = ""
}

func (s *concurrentStatusSyncStore) configurationWrite(image string, traits *model.JSONStruct, updateTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.component.Image = image
	s.component.Traits = traits
	s.component.UpdateTime = updateTime
}

func (s *concurrentStatusSyncStore) snapshot() model.ApplicationComponent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.component
}

func (s *concurrentStatusSyncStore) compareAndSwapCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.casCalls
}

func TestSyncComponentStatusQueriesByAppIDAndComponentID(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:       7,
		AppID:    "app-1",
		Name:     "web",
		Replicas: 1,
	}
	store := &statusSyncStore{
		listResult: []datastore.Entity{component},
	}
	server := &restServer{dataStore: store}

	readyReplicas := int32(1)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		ReadyReplicas: &readyReplicas,
	})

	require.NotNil(t, store.listQuery)
	require.Equal(t, "app-1", store.listQuery.AppID)
	require.Empty(t, store.listQuery.Name)
	require.NotNil(t, store.listOptions)
	require.Equal(t, 0, store.listOptions.Page)
	require.Equal(t, 0, store.listOptions.PageSize)
	require.Equal(t, 1, store.casCalls)
	require.Equal(t, map[string]interface{}{
		"app_id":         "app-1",
		"id":             7,
		"status":         "",
		"ready_replicas": int32(0),
		"last_abnormal":  "",
	}, store.casConditions)
	require.Equal(t, string(config.ComponentStatusRunning), store.casUpdates["status"])
	require.Equal(t, int32(1), store.casUpdates["ready_replicas"])
}

func TestSyncComponentStatusUsesComponentIDWhenLabelNameIsNormalized(t *testing.T) {
	component := &model.ApplicationComponent{
		ID:       7,
		AppID:    "app-1",
		Name:     "api.v1",
		Replicas: 1,
	}
	store := &statusSyncStore{
		listResult: []datastore.Entity{component},
	}
	server := &restServer{dataStore: store}

	readyReplicas := int32(1)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "api-v1",
		ReadyReplicas: &readyReplicas,
	})

	require.NotNil(t, store.listQuery)
	require.Equal(t, "app-1", store.listQuery.AppID)
	require.Empty(t, store.listQuery.Name)
	require.Equal(t, 1, store.casCalls)
	require.NotNil(t, store.casComp)
	require.Equal(t, "api.v1", store.casComp.Name)
	require.Equal(t, string(config.ComponentStatusRunning), store.casUpdates["status"])
}

func TestSyncComponentStatusSkipsWhenComponentIDNotFound(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:       9,
				AppID:    "app-1",
				Name:     "web",
				Replicas: 1,
			},
		},
	}
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	readyReplicas := int32(1)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		ReadyReplicas: &readyReplicas,
	})

	require.NotNil(t, store.listQuery)
	require.Equal(t, "app-1", store.listQuery.AppID)
	require.Empty(t, store.listQuery.Name)
	require.Equal(t, 0, store.casCalls)
	require.Nil(t, store.casComp)
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusSkipsStoppedComponent(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:       7,
				AppID:    "app-1",
				Name:     "web",
				Replicas: 3,
				Status:   string(config.ComponentStatusStopped),
			},
		},
	}
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	status := config.ComponentStatusUnknown
	readyReplicas := int32(0)
	replicas := int32(0)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		Status:        &status,
		ReadyReplicas: &readyReplicas,
		Replicas:      &replicas,
	})

	require.Equal(t, 0, store.casCalls)
	require.Nil(t, store.casComp)
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusDropsNoChangeConflictWithoutRetry(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:            7,
				AppID:         "app-1",
				Name:          "web",
				Replicas:      1,
				ReadyReplicas: 1,
				Status:        string(config.ComponentStatusRunning),
			},
		},
		casNotFound: true,
		exists:      true,
	}
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	status := config.ComponentStatusPending
	readyReplicas := int32(0)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		Status:        &status,
		ReadyReplicas: &readyReplicas,
	})

	require.Equal(t, 1, store.casCalls)
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusConcurrentStoppedWriteWinsOverInformer(t *testing.T) {
	store := newConcurrentStatusSyncStore(model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "web",
		Replicas:      1,
		ReadyReplicas: 1,
		Status:        string(config.ComponentStatusRunning),
	})
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	status := config.ComponentStatusPending
	readyReplicas := int32(0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.syncComponentStatus(&informer.ComponentStatusUpdate{
			AppID:         "app-1",
			ComponentID:   7,
			ComponentName: "web",
			Status:        &status,
			ReadyReplicas: &readyReplicas,
		})
	}()

	var releaseOnce sync.Once
	releaseCAS := func() { releaseOnce.Do(func() { close(store.casRelease) }) }
	defer releaseCAS()
	select {
	case <-store.casStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not reach compare-and-swap")
	}
	store.markStopped()
	releaseCAS()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not finish")
	}

	component := store.snapshot()
	require.Equal(t, string(config.ComponentStatusStopped), component.Status)
	require.Equal(t, int32(0), component.ReadyReplicas)
	require.Equal(t, 1, store.compareAndSwapCalls())
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusConcurrentConfigurationWriteDoesNotBlockRuntimeCAS(t *testing.T) {
	initialUpdateTime := time.Date(2026, time.July, 17, 10, 0, 0, 123000000, time.UTC)
	configurationUpdateTime := initialUpdateTime.Add(time.Second)
	initialTraits := model.JSONStruct{"env": map[string]interface{}{"VERSION": "v1"}}
	updatedTraits := model.JSONStruct{"env": map[string]interface{}{"VERSION": "v2"}}
	store := newConcurrentStatusSyncStore(model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "web",
		Image:         "example/web:v1",
		Replicas:      1,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusRestarting),
		Traits:        &initialTraits,
		BaseModel: model.BaseModel{
			UpdateTime: initialUpdateTime,
		},
	})
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	status := config.ComponentStatusRunning
	readyReplicas := int32(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.syncComponentStatus(&informer.ComponentStatusUpdate{
			AppID:         "app-1",
			ComponentID:   7,
			ComponentName: "web",
			Status:        &status,
			ReadyReplicas: &readyReplicas,
		})
	}()

	var releaseOnce sync.Once
	releaseCAS := func() { releaseOnce.Do(func() { close(store.casRelease) }) }
	defer releaseCAS()
	select {
	case <-store.casStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not reach compare-and-swap")
	}
	store.configurationWrite("example/web:v2", &updatedTraits, configurationUpdateTime)
	releaseCAS()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not finish")
	}

	component := store.snapshot()
	require.Equal(t, string(config.ComponentStatusRunning), component.Status)
	require.Equal(t, int32(1), component.ReadyReplicas)
	require.Equal(t, "example/web:v2", component.Image)
	require.Equal(t, &updatedTraits, component.Traits)
	require.Equal(t, 1, store.compareAndSwapCalls())
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusCleaningPodsGoneMarksNotDeployWithZeroValues(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:            7,
				AppID:         "app-1",
				Name:          "web",
				Replicas:      3,
				ReadyReplicas: 3,
				Status:        string(config.ComponentStatusCleaning),
				LastAbnormal:  "terminating old pods",
			},
		},
	}
	server := &restServer{dataStore: store}

	replicas := int32(0)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		Replicas:      &replicas,
	})

	require.Equal(t, 1, store.casCalls)
	require.Equal(t, map[string]interface{}{
		"app_id":         "app-1",
		"id":             7,
		"status":         string(config.ComponentStatusCleaning),
		"ready_replicas": int32(3),
		"last_abnormal":  "terminating old pods",
	}, store.casConditions)
	require.Equal(t, map[string]interface{}{
		"status":         string(config.ComponentStatusNotDeploy),
		"ready_replicas": int32(0),
		"last_abnormal":  "",
	}, store.casUpdates)
}

func TestSyncComponentStatusDropsStaleCleaningCompletionWithoutRetry(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:            7,
				AppID:         "app-1",
				Name:          "web",
				Replicas:      3,
				ReadyReplicas: 3,
				Status:        string(config.ComponentStatusCleaning),
				LastAbnormal:  "terminating old pods",
			},
		},
		casNotFound: true,
		exists:      true,
	}
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "stale components"))
	server := &restServer{dataStore: store, cache: componentCache}

	replicas := int32(0)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		Replicas:      &replicas,
	})

	require.Equal(t, 1, store.casCalls)
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusFailedRecoveryClearsLastAbnormal(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:            7,
				AppID:         "app-1",
				Name:          "web",
				Replicas:      1,
				ReadyReplicas: 0,
				Status:        string(config.ComponentStatusFailed),
				LastAbnormal:  "CrashLoopBackOff",
			},
		},
	}
	server := &restServer{dataStore: store}

	status := config.ComponentStatusRunning
	readyReplicas := int32(1)
	lastAbnormal := ""
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		Status:        &status,
		ReadyReplicas: &readyReplicas,
		LastAbnormal:  &lastAbnormal,
	})

	require.Equal(t, 1, store.casCalls)
	require.Equal(t, map[string]interface{}{
		"app_id":         "app-1",
		"id":             7,
		"status":         string(config.ComponentStatusFailed),
		"ready_replicas": int32(0),
		"last_abnormal":  "CrashLoopBackOff",
	}, store.casConditions)
	require.Equal(t, string(config.ComponentStatusRunning), store.casUpdates["status"])
	require.Equal(t, int32(1), store.casUpdates["ready_replicas"])
	require.Equal(t, "", store.casUpdates["last_abnormal"])
}

func TestSyncComponentStatusInvalidatesCacheAfterConcurrentRefill(t *testing.T) {
	store := newConcurrentStatusSyncStore(model.ApplicationComponent{
		ID:            7,
		AppID:         "app-1",
		Name:          "web",
		Replicas:      1,
		ReadyReplicas: 0,
		Status:        string(config.ComponentStatusPending),
	})
	componentCache := cache.NewMemCache(false)
	cacheKey := cache.ApplicationComponentsKey("app-1")
	require.NoError(t, componentCache.Store(cacheKey, "initial components"))
	server := &restServer{dataStore: store, cache: componentCache}

	status := config.ComponentStatusRunning
	readyReplicas := int32(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.syncComponentStatus(&informer.ComponentStatusUpdate{
			AppID:         "app-1",
			ComponentID:   7,
			ComponentName: "web",
			Status:        &status,
			ReadyReplicas: &readyReplicas,
		})
	}()

	var releaseOnce sync.Once
	releaseCAS := func() { releaseOnce.Do(func() { close(store.casRelease) }) }
	defer releaseCAS()
	select {
	case <-store.casStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not reach compare-and-swap")
	}
	require.NoError(t, componentCache.Store(cacheKey, "refilled components"))
	releaseCAS()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("status sync did not finish")
	}

	component := store.snapshot()
	require.Equal(t, string(config.ComponentStatusRunning), component.Status)
	require.Equal(t, int32(1), component.ReadyReplicas)
	require.False(t, componentCache.Exists(cacheKey))
}

func TestSyncComponentStatusPreservesStartingForNonTerminalInformerStatus(t *testing.T) {
	tests := []struct {
		name          string
		status        config.ComponentStatus
		readyReplicas int32
	}{
		{
			name:          "pending",
			status:        config.ComponentStatusPending,
			readyReplicas: 0,
		},
		{
			name:          "unknown",
			status:        config.ComponentStatusUnknown,
			readyReplicas: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &statusSyncStore{
				listResult: []datastore.Entity{
					&model.ApplicationComponent{
						ID:            7,
						AppID:         "app-1",
						Name:          "web",
						Replicas:      3,
						ReadyReplicas: 0,
						Status:        string(config.ComponentStatusStarting),
					},
				},
			}
			server := &restServer{dataStore: store}

			replicas := int32(3)
			lastAbnormal := ""
			server.syncComponentStatus(&informer.ComponentStatusUpdate{
				AppID:         "app-1",
				ComponentID:   7,
				ComponentName: "web",
				Status:        &tt.status,
				ReadyReplicas: &tt.readyReplicas,
				Replicas:      &replicas,
				LastAbnormal:  &lastAbnormal,
			})

			require.Equal(t, 1, store.casCalls)
			require.Equal(t, string(config.ComponentStatusStarting), store.casUpdates["status"])
			require.Equal(t, tt.readyReplicas, store.casUpdates["ready_replicas"])
			require.Equal(t, "", store.casUpdates["last_abnormal"])
		})
	}
}

func TestSyncComponentStatusPreservesStartingForReadyReplicaPending(t *testing.T) {
	store := &statusSyncStore{
		listResult: []datastore.Entity{
			&model.ApplicationComponent{
				ID:            7,
				AppID:         "app-1",
				Name:          "web",
				Replicas:      3,
				ReadyReplicas: 0,
				Status:        string(config.ComponentStatusStarting),
			},
		},
	}
	server := &restServer{dataStore: store}

	readyReplicas := int32(1)
	server.syncComponentStatus(&informer.ComponentStatusUpdate{
		AppID:         "app-1",
		ComponentID:   7,
		ComponentName: "web",
		ReadyReplicas: &readyReplicas,
	})

	require.Equal(t, 1, store.casCalls)
	require.Equal(t, string(config.ComponentStatusStarting), store.casUpdates["status"])
	require.Equal(t, int32(1), store.casUpdates["ready_replicas"])
}

func TestSyncComponentStatusPreservesDeployingForNonTerminalInformerStatus(t *testing.T) {
	tests := []struct {
		name          string
		status        config.ComponentStatus
		readyReplicas int32
	}{
		{
			name:          "pending",
			status:        config.ComponentStatusPending,
			readyReplicas: 0,
		},
		{
			name:          "unknown",
			status:        config.ComponentStatusUnknown,
			readyReplicas: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &statusSyncStore{
				listResult: []datastore.Entity{
					&model.ApplicationComponent{
						ID:            7,
						AppID:         "app-1",
						Name:          "web",
						Replicas:      3,
						ReadyReplicas: 0,
						Status:        string(config.ComponentStatusDeploying),
					},
				},
			}
			server := &restServer{dataStore: store}

			replicas := int32(3)
			lastAbnormal := ""
			server.syncComponentStatus(&informer.ComponentStatusUpdate{
				AppID:         "app-1",
				ComponentID:   7,
				ComponentName: "web",
				Status:        &tt.status,
				ReadyReplicas: &tt.readyReplicas,
				Replicas:      &replicas,
				LastAbnormal:  &lastAbnormal,
			})

			require.Equal(t, 1, store.casCalls)
			require.Equal(t, string(config.ComponentStatusDeploying), store.casUpdates["status"])
			require.Equal(t, tt.readyReplicas, store.casUpdates["ready_replicas"])
			require.Equal(t, "", store.casUpdates["last_abnormal"])
		})
	}
}

func TestSyncComponentStatusAllowsStartingToReachTerminalInformerStatus(t *testing.T) {
	tests := []struct {
		name         string
		status       config.ComponentStatus
		lastAbnormal string
	}{
		{
			name:   "running",
			status: config.ComponentStatusRunning,
		},
		{
			name:         "failed",
			status:       config.ComponentStatusFailed,
			lastAbnormal: "CrashLoopBackOff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &statusSyncStore{
				listResult: []datastore.Entity{
					&model.ApplicationComponent{
						ID:            7,
						AppID:         "app-1",
						Name:          "web",
						Replicas:      1,
						ReadyReplicas: 0,
						Status:        string(config.ComponentStatusStarting),
					},
				},
			}
			server := &restServer{dataStore: store}

			readyReplicas := int32(1)
			if tt.status == config.ComponentStatusFailed {
				readyReplicas = 0
			}
			server.syncComponentStatus(&informer.ComponentStatusUpdate{
				AppID:         "app-1",
				ComponentID:   7,
				ComponentName: "web",
				Status:        &tt.status,
				ReadyReplicas: &readyReplicas,
				LastAbnormal:  &tt.lastAbnormal,
			})

			require.Equal(t, 1, store.casCalls)
			require.Equal(t, string(tt.status), store.casUpdates["status"])
			require.Equal(t, readyReplicas, store.casUpdates["ready_replicas"])
			require.Equal(t, tt.lastAbnormal, store.casUpdates["last_abnormal"])
		})
	}
}

func TestSyncComponentStatusAllowsDeployingToReachTerminalInformerStatus(t *testing.T) {
	tests := []struct {
		name         string
		status       config.ComponentStatus
		lastAbnormal string
	}{
		{
			name:   "running",
			status: config.ComponentStatusRunning,
		},
		{
			name:         "failed",
			status:       config.ComponentStatusFailed,
			lastAbnormal: "CrashLoopBackOff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &statusSyncStore{
				listResult: []datastore.Entity{
					&model.ApplicationComponent{
						ID:            7,
						AppID:         "app-1",
						Name:          "web",
						Replicas:      1,
						ReadyReplicas: 0,
						Status:        string(config.ComponentStatusDeploying),
					},
				},
			}
			server := &restServer{dataStore: store}

			readyReplicas := int32(1)
			if tt.status == config.ComponentStatusFailed {
				readyReplicas = 0
			}
			server.syncComponentStatus(&informer.ComponentStatusUpdate{
				AppID:         "app-1",
				ComponentID:   7,
				ComponentName: "web",
				Status:        &tt.status,
				ReadyReplicas: &readyReplicas,
				LastAbnormal:  &tt.lastAbnormal,
			})

			require.Equal(t, 1, store.casCalls)
			require.Equal(t, string(tt.status), store.casUpdates["status"])
			require.Equal(t, readyReplicas, store.casUpdates["ready_replicas"])
			require.Equal(t, tt.lastAbnormal, store.casUpdates["last_abnormal"])
		})
	}
}

var _ datastore.ConditionalCompareAndSwap = (*statusSyncStore)(nil)
