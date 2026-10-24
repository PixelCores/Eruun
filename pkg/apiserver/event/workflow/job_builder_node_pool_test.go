package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

type componentProfileStore struct {
	datastore.DataStore
	deleted bool
}

func (s *componentProfileStore) Get(_ context.Context, entity datastore.Entity) error {
	if s.deleted {
		return datastore.ErrRecordNotExist
	}
	switch value := entity.(type) {
	case *model.NodePool:
		value.NodeSelector = map[string]string{"pool": "gpu"}
		value.Tolerations = []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists}}
	default:
		return datastore.ErrRecordNotExist
	}
	return nil
}

func TestComponentDeploymentResolvesNodePool(t *testing.T) {
	traits, err := model.NewJSONStructByStruct(spec.Traits{NodePool: "gpu"})
	require.NoError(t, err)
	component := &model.ApplicationComponent{Name: "api", AppID: "app", Namespace: "demo", ComponentType: config.ServerJob, Image: "nginx:1.27", Traits: traits}
	store := &componentProfileStore{}
	buckets, err := buildJobsForComponent(context.Background(), store, component, &model.WorkflowQueue{AppID: "app", TaskID: "task"}, 60, spec.GatewayConfig{}, "execution")
	require.NoError(t, err)
	count := 0
	for _, jobs := range buckets {
		for _, job := range jobs {
			count++
			if deployment, ok := job.JobInfo.(*appsv1.Deployment); ok {
				require.Equal(t, "gpu", deployment.Spec.Template.Spec.NodeSelector["pool"])
				require.Len(t, deployment.Spec.Template.Spec.Tolerations, 1)
				require.Empty(t, deployment.Spec.Template.Spec.ServiceAccountName)
				deploy, err := jobexec.PrepareTask(job, "app", &model.Workspace{ID: "space", Namespace: "demo"}, spec.WorkspaceConfig{})
				require.NoError(t, err)
				require.True(t, deploy)
				require.NotNil(t, deployment.Spec.Template.Spec.AutomountServiceAccountToken)
				require.False(t, *deployment.Spec.Template.Spec.AutomountServiceAccountToken)
				require.Equal(t, "gpu", deployment.Spec.Template.Spec.NodeSelector["pool"])
				require.Len(t, deployment.Spec.Template.Spec.Tolerations, 1)
				t.Log("PrepareTask: rendered nodePool Deployment passed workspace policy; token automount=false")
			}
		}
	}
	require.Equal(t, 1, count)
	store.deleted = true
	_, err = buildJobsForComponent(context.Background(), store, component, &model.WorkflowQueue{}, 60, spec.GatewayConfig{}, "")
	require.ErrorContains(t, err, `nodePool "gpu"`)
}
