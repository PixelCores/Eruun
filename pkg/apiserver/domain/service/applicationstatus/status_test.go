package applicationstatus

import (
	"testing"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/stretchr/testify/require"
)

func TestAggregateApplicationStatusStartingPriority(t *testing.T) {
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "starting beats cleaning and pending",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "worker", Status: string(config.ComponentStatusCleaning)},
				{Name: "api", Status: string(config.ComponentStatusPending)},
			},
			want: "starting",
		},
		{
			name: "restarting beats starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "api", Status: string(config.ComponentStatusRestarting)},
			},
			want: "restarting",
		},
		{
			name: "failed beats starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusStarting)},
				{Name: "db", Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
		{
			name: "deploying beats updating and starting",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusDeploying)},
				{Name: "api", Status: string(config.ComponentStatusUpdating)},
				{Name: "worker", Status: string(config.ComponentStatusStarting)},
			},
			want: "deploying",
		},
		{
			name: "failed beats deploying",
			components: []*model.ApplicationComponent{
				{Name: "web", Status: string(config.ComponentStatusDeploying)},
				{Name: "db", Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, AggregateWithReferenceTime(tt.components, time.Now()))
		})
	}
}

func TestAggregateApplicationStatusUsesServingComponentsForAvailability(t *testing.T) {
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "stopped webservice beats running store for app availability",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "stopped",
		},
		{
			name: "running webservice keeps app running when store is stopped",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusStopped)},
			},
			want: "running",
		},
		{
			name: "starting webservice keeps start recovery visible with stopped store",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStarting)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusStopped)},
			},
			want: "starting",
		},
		{
			name: "starting webservice keeps start recovery visible with running store",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStarting)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "starting",
		},
		{
			name: "store restart stays globally visible with running webservice",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRestarting)},
			},
			want: "restarting",
		},
		{
			name: "store only app keeps existing aggregate behavior",
			components: []*model.ApplicationComponent{
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "running",
		},
		{
			name: "store failure still fails app with stopped webservice",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusFailed)},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, AggregateWithReferenceTime(tt.components, time.Now()))
		})
	}
}

func TestAggregateApplicationStatusPrefersManagedServingComponents(t *testing.T) {
	shareTraits := func(strategy domainspec.ShareStrategy) *model.JSONStruct {
		return &model.JSONStruct{
			"share": map[string]interface{}{
				"strategy": string(strategy),
			},
		}
	}
	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "stopped managed workloads are not hidden by running shared proxy and stores",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusStopped)},
				{Name: "proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning), Traits: shareTraits(domainspec.ShareStrategyDefault)},
				{Name: "mysql", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
				{Name: "redis", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "stopped",
		},
		{
			name: "pending shared proxy does not hide ready managed workloads",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "frontend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyDefault)},
				{Name: "mysql", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
				{Name: "redis", ComponentType: config.StoreJob, Status: string(config.ComponentStatusRunning)},
			},
			want: "running",
		},
		{
			name: "shared ignore pending does not hide managed running",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "ignored-proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyIgnore)},
			},
			want: "running",
		},
		{
			name: "unknown shared strategy pending does not hide managed running",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "future-proxy", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategy("future-default"))},
			},
			want: "running",
		},
		{
			name: "shared failure remains globally visible",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "shared-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusFailed), Traits: shareTraits(domainspec.ShareStrategyDefault)},
			},
			want: "failed",
		},
		{
			name: "shared only application falls back to shared availability",
			components: []*model.ApplicationComponent{
				{Name: "shared-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyDefault)},
			},
			want: "pending",
		},
		{
			name: "share force remains managed",
			components: []*model.ApplicationComponent{
				{Name: "backend", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning)},
				{Name: "forced-socket", ComponentType: config.ServerJob, Status: string(config.ComponentStatusPending), Traits: shareTraits(domainspec.ShareStrategyForce)},
			},
			want: "pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, AggregateWithReferenceTime(tt.components, time.Now()))
		})
	}
}

func TestAggregateApplicationStatusSmoothsRecentPodBackedFailures(t *testing.T) {
	now := time.Unix(1000, 0)
	recentFailureTime := now.Add(-config.DefaultApplicationStatusTransientFailedWindow + time.Second)
	expiredFailureTime := now.Add(-config.DefaultApplicationStatusTransientFailedWindow - time.Second)

	tests := []struct {
		name       string
		components []*model.ApplicationComponent
		want       string
	}{
		{
			name: "running webservice keeps app running when store failure is recent",
			components: []*model.ApplicationComponent{
				{Name: "web", ComponentType: config.ServerJob, Status: string(config.ComponentStatusRunning), Replicas: 1, ReadyReplicas: 1},
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 1,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "running",
		},
		{
			name: "store only recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "zero replica recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      0,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "updating shrink to zero recent failed component is treated as pending while recovering",
			components: []*model.ApplicationComponent{
				{
					Name:          "web",
					ComponentType: config.ServerJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      0,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "pending",
		},
		{
			name: "expired pod backed failure still fails app",
			components: []*model.ApplicationComponent{
				{
					Name:          "db",
					ComponentType: config.StoreJob,
					Status:        string(config.ComponentStatusFailed),
					Replicas:      1,
					ReadyReplicas: 0,
					BaseModel:     model.BaseModel{UpdateTime: expiredFailureTime},
				},
			},
			want: "failed",
		},
		{
			name: "non pod backed recent failure still fails app",
			components: []*model.ApplicationComponent{
				{
					Name:          "settings",
					ComponentType: config.ConfJob,
					Status:        string(config.ComponentStatusFailed),
					BaseModel:     model.BaseModel{UpdateTime: recentFailureTime},
				},
			},
			want: "failed",
		},
		{
			name: "missing update time still fails app",
			components: []*model.ApplicationComponent{
				{Name: "db", ComponentType: config.StoreJob, Status: string(config.ComponentStatusFailed), Replicas: 1},
			},
			want: "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, AggregateWithReferenceTime(tt.components, now))
		})
	}
}
