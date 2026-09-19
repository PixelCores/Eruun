package application

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func TestEvaluationResourceCleanupAcrossApplicationLifecycle(t *testing.T) {
	for _, operation := range []string{"cleanup", "application_delete", "component_remove"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			app := &model.Applications{ID: "app-eval", Name: "evaluation", Namespace: "tenant-a"}
			component := &model.ApplicationComponent{
				ID: 7, AppID: app.ID, Name: "LLM evaluation", Namespace: app.Namespace, ComponentType: config.InstantJob,
				Traits: mustJSONStruct(spec.Traits{Evaluation: &spec.EvaluationTraitSpec{
					Env: "daytona", Model: "provider/model", Agent: "terminus-2", TaskPackageID: "tasks",
				}}),
			}
			labelsFor := func(app, name string) map[string]string {
				return map[string]string{config.LabelAppID: app, config.LabelComponentName: naming.BoundedLabelValue(name)}
			}
			owned := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "eruun-eval-execution-a", Namespace: app.Namespace, UID: "runner-a", Labels: labelsFor(app.ID, component.Name)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "eruun-eval-execution-b", Namespace: app.Namespace, UID: "runner-b", Labels: labelsFor(app.ID, component.Name)}},
			}
			unrelated := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "other-component", Namespace: app.Namespace, Labels: labelsFor(app.ID, "other")}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-app", Namespace: app.Namespace, Labels: labelsFor("other-app", component.Name)}},
				{ObjectMeta: metav1.ObjectMeta{Name: "other-namespace", Namespace: "tenant-b", Labels: labelsFor(app.ID, component.Name)}},
				{ObjectMeta: metav1.ObjectMeta{Name: naming.JobName(component.Name, app.Name), Namespace: app.Namespace}},
			}
			objects := []runtime.Object{}
			for _, item := range append(owned, unrelated...) {
				objects = append(objects, item)
			}
			client := fake.NewSimpleClientset(objects...)
			client.PrependReactor("delete", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				deletion := action.(ktesting.DeleteAction)
				options := deletion.GetDeleteOptions()
				require.NotNil(t, options.Preconditions)
				require.NotNil(t, options.Preconditions.UID)
				current, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), action.GetNamespace(), deletion.GetName())
				require.NoError(t, err)
				require.Equal(t, current.(*batchv1.Job).UID, *options.Preconditions.UID)
				require.NotNil(t, options.PropagationPolicy)
				require.Equal(t, metav1.DeletePropagationBackground, *options.PropagationPolicy)
				return false, nil, nil
			})
			store := &cleanupStore{app: app, components: []*model.ApplicationComponent{component}, applications: map[string]*model.Applications{app.ID: app}}
			svc := &applicationsServiceImpl{
				KubeClient: client, Store: store, AppRepo: &mockCleanupAppRepo{store: store},
				ComponentRepo: &mockCleanupComponentRepo{store: store}, WorkflowQueueRepo: &mockWorkflowQueueRepo{},
				ScheduleLocker: locker.NewMemoryLocker("evaluation-cleanup"),
			}
			switch operation {
			case "cleanup":
				response, err := svc.CleanupApplicationResources(ctx, app.ID)
				require.NoError(t, err)
				require.Empty(t, response.FailedResources)
				require.ElementsMatch(t, []string{"Job:tenant-a/eruun-eval-execution-a", "Job:tenant-a/eruun-eval-execution-b"}, response.DeletedResources)
			case "application_delete":
				cascade := newCascadeDeleteStore()
				cascade.apps[app.ID], cascade.components[component.Name] = app, component
				svc.Store, svc.AppRepo, svc.ComponentRepo = cascade, &cascadeAppRepo{store: cascade}, &cascadeComponentRepo{store: cascade}
				response, err := svc.DeleteApplicationCascade(ctx, app.ID, apisv1.DeleteApplicationRequest{WaitSeconds: int64Ptr(0)})
				require.NoError(t, err)
				require.Empty(t, response.FailedResources)
				require.Empty(t, cascade.apps)
			case "component_remove":
				require.NoError(t, svc.cleanupVersionUpdateRemovedComponent(ctx, component))
			}
			for _, item := range owned {
				_, err := client.BatchV1().Jobs(item.Namespace).Get(ctx, item.Name, metav1.GetOptions{})
				require.True(t, k8serrors.IsNotFound(err))
			}
			for _, item := range unrelated {
				_, err := client.BatchV1().Jobs(item.Namespace).Get(ctx, item.Name, metav1.GetOptions{})
				require.NoError(t, err)
			}
		})
	}
}

func TestEvaluationResourceCleanupReportsListAndIdentityFailures(t *testing.T) {
	for _, operation := range []string{"list", "delete"} {
		t.Run(operation, func(t *testing.T) {
			component := &model.ApplicationComponent{
				Name: "evaluation", AppID: "app-eval", Namespace: "tenant-a", ComponentType: config.InstantJob,
				Traits: mustJSONStruct(spec.Traits{Evaluation: &spec.EvaluationTraitSpec{TaskPackageID: "tasks"}}),
			}
			item := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "eruun-eval-execution-a", Namespace: component.Namespace, UID: "original",
				Labels: map[string]string{config.LabelAppID: component.AppID, config.LabelComponentName: component.Name}}}
			client := fake.NewSimpleClientset(item)
			failure := errors.New("list unavailable")
			if operation == "delete" {
				failure = k8serrors.NewConflict(schema.GroupResource{Group: "batch", Resource: "jobs"}, item.Name, errors.New("UID changed"))
			}
			client.PrependReactor(operation, "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				if operation == "delete" {
					options := action.(ktesting.DeleteAction).GetDeleteOptions()
					require.NotNil(t, options.Preconditions)
					require.Equal(t, types.UID("original"), *options.Preconditions.UID)
				}
				return true, nil, failure
			})
			svc := &applicationsServiceImpl{KubeClient: client}
			require.ErrorIs(t, svc.cleanupVersionUpdateRemovedComponent(context.Background(), component), failure)
			_, err := client.BatchV1().Jobs(item.Namespace).Get(context.Background(), item.Name, metav1.GetOptions{})
			require.NoError(t, err)
		})
	}
}
