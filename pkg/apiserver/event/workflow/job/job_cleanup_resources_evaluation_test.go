package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestEvaluationCleanupDeletesOnlyComponentRunners(t *testing.T) {
	for _, removed := range []bool{false, true} {
		name := "cleanup_resources"
		if removed {
			name = "version_update_remove"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			traits, err := model.NewJSONStructByStruct(spec.Traits{Evaluation: &spec.EvaluationTraitSpec{
				Env: "daytona", Model: "provider/model", Agent: "terminus-2", TaskPackageID: "tasks",
			}})
			require.NoError(t, err)
			component := &model.ApplicationComponent{
				ID: 7, Name: "LLM evaluation", AppID: "app-eval", Namespace: "tenant-a",
				ComponentType: config.InstantJob, Traits: traits,
			}
			labelsFor := func(app, name string) map[string]string {
				return map[string]string{config.LabelAppID: app, config.LabelComponentName: naming.BoundedLabelValue(name)}
			}
			ownedLabels := labelsFor(component.AppID, component.Name)
			objects := []runtime.Object{}
			for _, name := range []string{"eruun-eval-execution-a", "eruun-eval-execution-b"} {
				objects = append(objects,
					&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: component.Namespace, UID: types.UID(name), Labels: ownedLabels}},
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: component.Namespace, Labels: ownedLabels,
						OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: name, UID: types.UID(name)}}}},
				)
			}
			unrelated := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "other-component", Namespace: component.Namespace, Labels: labelsFor(component.AppID, "other")}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-app", Namespace: component.Namespace, Labels: labelsFor("other-app", component.Name)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-namespace", Namespace: "tenant-b", Labels: ownedLabels}},
				{ObjectMeta: metav1.ObjectMeta{Name: buildJobName(component.Name, component.ResourceNameKey()), Namespace: component.Namespace}},
			}
			for _, item := range unrelated {
				objects = append(objects, item, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: item.Name + "-pod", Namespace: item.Namespace, Labels: item.Labels}})
			}
			client := fake.NewSimpleClientset(objects...)
			client.PrependReactor("delete", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				deletion := action.(ktesting.DeleteAction)
				require.NotNil(t, deletion.GetDeleteOptions().Preconditions)
				require.Equal(t, types.UID(deletion.GetName()), *deletion.GetDeleteOptions().Preconditions.UID)
				return false, nil, nil
			})
			store := &cleanupComponentStore{component: component}
			task := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: "cleanup-task", JobType: string(config.JobCleanupResources), JobInfo: component, Timeout: 3,
			}
			if removed {
				store.component = nil
				task.InternalInfo = versionUpdateRemoveCleanupInternalInfo()
				store.jobInfo = &model.JobInfo{ID: 8, Type: task.JobType, AppID: task.AppID, TaskID: task.TaskID,
					ServiceName: task.Name, Status: string(config.StatusQueued), InternalInfo: task.InternalInfo}
			}
			ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
			require.NotNil(t, ctl)
			require.NoError(t, ctl.Run(ctx))
			require.Equal(t, config.StatusCompleted, task.Status)
			for _, name := range []string{"eruun-eval-execution-a", "eruun-eval-execution-b"} {
				_, err := client.BatchV1().Jobs(component.Namespace).Get(ctx, name, metav1.GetOptions{})
				require.True(t, k8serrors.IsNotFound(err))
				_, err = client.CoreV1().Pods(component.Namespace).Get(ctx, name+"-pod", metav1.GetOptions{})
				require.True(t, k8serrors.IsNotFound(err))
			}
			for _, item := range unrelated {
				_, err := client.BatchV1().Jobs(item.Namespace).Get(ctx, item.Name, metav1.GetOptions{})
				require.NoError(t, err)
				_, err = client.CoreV1().Pods(item.Namespace).Get(ctx, item.Name+"-pod", metav1.GetOptions{})
				require.NoError(t, err)
			}
		})
	}
}
