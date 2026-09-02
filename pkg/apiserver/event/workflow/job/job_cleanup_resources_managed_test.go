package job

import (
	"context"

	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"

	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestCleanupResourcesJobCtlDeletesGeneratedAndLabeledResources(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    props,
	}
	ownedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: buildWebServiceName(component.Name, component.AppID), Namespace: component.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: buildServiceName(component.Name, component.AppID), Namespace: component.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "extra-service", Namespace: component.Namespace, Labels: ownedLabels}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-web", Namespace: component.Namespace, Labels: ownedLabels}},
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "web-ingress", Namespace: component.Namespace, Labels: ownedLabels}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "web-config", Namespace: component.Namespace, Labels: ownedLabels}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "web-secret", Namespace: component.Namespace, Labels: ownedLabels}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "web-sa", Namespace: component.Namespace, Labels: ownedLabels}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "web-role", Namespace: component.Namespace, Labels: ownedLabels}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "web-rb", Namespace: component.Namespace, Labels: ownedLabels}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "web-cluster-role", Labels: ownedLabels}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "web-cluster-rb", Labels: ownedLabels}},
	)
	store := &cleanupComponentStore{component: component}
	ackCount := 0
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, func() { ackCount++ })
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, 1, ackCount)
	require.Equal(t, config.StatusCompleted, task.Status)
	require.NotNil(t, store.putComponent)
	require.Equal(t, string(config.ComponentStatusNotDeploy), store.putComponent.Status)

	_, err = client.AppsV1().Deployments(component.Namespace).Get(ctx, buildWebServiceName(component.Name, component.AppID), metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, buildServiceName(component.Name, component.AppID), metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, "extra-service", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, "data-web", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.NetworkingV1().Ingresses(component.Namespace).Get(ctx, "web-ingress", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().ConfigMaps(component.Namespace).Get(ctx, "web-config", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Secrets(component.Namespace).Get(ctx, "web-secret", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().ServiceAccounts(component.Namespace).Get(ctx, "web-sa", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().Roles(component.Namespace).Get(ctx, "web-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().RoleBindings(component.Namespace).Get(ctx, "web-rb", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().ClusterRoles().Get(ctx, "web-cluster-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().ClusterRoleBindings().Get(ctx, "web-cluster-rb", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlPreservesTraitGeneratedRBAC(t *testing.T) {
	traitsPlu.ResetTraitProcessorsForTest()
	traitsPlu.RegisterAllProcessors()
	t.Cleanup(traitsPlu.ResetTraitProcessorsForTest)

	ctx := context.Background()
	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "labeler",
		AppID:         "app-rbac",
		Namespace:     "ops",
		ComponentType: config.ServerJob,
		Image:         "busybox:latest",
	}
	traitsJSON, err := model.NewJSONStructByStruct(&spec.Traits{
		RBAC: []spec.RBACPolicySpec{
			{
				ServiceAccount: "pod-labeler-sa",
				RoleName:       "pod-labeler-role",
				BindingName:    "pod-labeler-binding",
				Rules: []spec.RBACRuleSpec{{
					APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "patch"},
				}},
			},
			{
				ServiceAccount: "cluster-labeler-sa",
				ClusterScope:   true,
				RoleName:       "pod-labeler-cluster-role",
				BindingName:    "pod-labeler-cluster-binding",
				Rules: []spec.RBACRuleSpec{{
					APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"get"},
				}},
			},
		},
	})
	require.NoError(t, err)
	component.Traits = traitsJSON

	deploymentName := buildWebServiceName(component.Name, component.AppID)
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: component.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-sa", Namespace: component.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "cluster-labeler-sa", Namespace: component.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-role", Namespace: component.Namespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-binding", Namespace: component.Namespace}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-role"}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-binding"}},
	)
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		JobType: string(config.JobCleanupResources), JobInfo: component, Timeout: 1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, &cleanupComponentStore{component: component}, nil)
	require.NotNil(t, ctl)
	require.NoError(t, ctl.Run(ctx))

	_, err = client.AppsV1().Deployments(component.Namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().ServiceAccounts(component.Namespace).Get(ctx, "pod-labeler-sa", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().ServiceAccounts(component.Namespace).Get(ctx, "cluster-labeler-sa", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().Roles(component.Namespace).Get(ctx, "pod-labeler-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().RoleBindings(component.Namespace).Get(ctx, "pod-labeler-binding", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().ClusterRoles().Get(ctx, "pod-labeler-cluster-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().ClusterRoleBindings().Get(ctx, "pod-labeler-cluster-binding", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlKeepsProtectedSharedResources(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    props,
	}
	ownedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	sharedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
		config.LabelShareName:     "shared-web",
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
	ignoredLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
		config.LabelShareName:     "ignored-web",
		config.LabelShareStrategy: string(config.ShareStrategyIgnore),
	}
	forceLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
		config.LabelShareName:     "force-web",
		config.LabelShareStrategy: string(config.ShareStrategyForce),
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: buildWebServiceName(component.Name, component.AppID), Namespace: component.Namespace, Labels: sharedLabels}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-shared-pod", Namespace: component.Namespace, Labels: sharedLabels}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: buildServiceName(component.Name, component.AppID), Namespace: component.Namespace, Labels: ignoredLabels}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-web", Namespace: component.Namespace, Labels: sharedLabels}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "web-role", Namespace: component.Namespace, Labels: sharedLabels}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "web-cluster-role", Labels: ignoredLabels}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "web-config", Namespace: component.Namespace, Labels: ownedLabels}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "web-secret", Namespace: component.Namespace, Labels: forceLabels}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "web-rb", Namespace: component.Namespace, Labels: ownedLabels}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)

	_, err = client.AppsV1().Deployments(component.Namespace).Get(ctx, buildWebServiceName(component.Name, component.AppID), metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Pods(component.Namespace).Get(ctx, "web-shared-pod", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, buildServiceName(component.Name, component.AppID), metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().PersistentVolumeClaims(component.Namespace).Get(ctx, "data-web", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().Roles(component.Namespace).Get(ctx, "web-role", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.RbacV1().ClusterRoles().Get(ctx, "web-cluster-role", metav1.GetOptions{})
	require.NoError(t, err)

	_, err = client.CoreV1().ConfigMaps(component.Namespace).Get(ctx, "web-config", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Secrets(component.Namespace).Get(ctx, "web-secret", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.RbacV1().RoleBindings(component.Namespace).Get(ctx, "web-rb", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlDeletesResidualComponentPods(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{
		Command: []string{"sh", "-c", "sleep 3600"},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            2,
		Name:          "batch",
		AppID:         "app-reset",
		Namespace:     "default",
		Image:         "busybox:1.36",
		ComponentType: config.InstantJob,
		Properties:    props,
	}
	ownedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	client := fake.NewSimpleClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: buildJobName(component.Name, component.AppID), Namespace: component.Namespace}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "batch-residual-job", Namespace: component.Namespace}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-owned-pod",
			Namespace: component.Namespace,
			Labels:    ownedLabels,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "batch-residual-job"},
			},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "other-pod",
			Namespace: component.Namespace,
			Labels: map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: "other",
			},
		}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   2,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)

	_, err = client.BatchV1().Jobs(component.Namespace).Get(ctx, buildJobName(component.Name, component.AppID), metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.BatchV1().Jobs(component.Namespace).Get(ctx, "batch-residual-job", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Pods(component.Namespace).Get(ctx, "batch-owned-pod", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Pods(component.Namespace).Get(ctx, "other-pod", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlKeepsProtectedResidualPods(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{
		Command: []string{"sh", "-c", "sleep 3600"},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            2,
		Name:          "batch",
		AppID:         "app-reset",
		Namespace:     "default",
		Image:         "busybox:1.36",
		ComponentType: config.InstantJob,
		Properties:    props,
	}
	ownedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
	}
	sharedLabels := map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: component.Name,
		config.LabelShareName:     "shared-batch",
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
	client := fake.NewSimpleClientset(
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "batch-residual-job", Namespace: component.Namespace, Labels: sharedLabels}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-owned-pod",
			Namespace: component.Namespace,
			Labels:    ownedLabels,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "batch-residual-job"},
			},
		}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)

	_, err = client.BatchV1().Jobs(component.Namespace).Get(ctx, "batch-residual-job", metav1.GetOptions{})
	require.NoError(t, err)
	_, err = client.CoreV1().Pods(component.Namespace).Get(ctx, "batch-owned-pod", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCleanupResourcesJobCtlDeletesTraitDefinedServicesByName(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "custom-public-web",
				Labels: map[string]string{
					config.LabelAppID:         "other-app",
					config.LabelComponentName: "other-component",
				},
				Selector: map[string]string{"role": "web"},
				Ports: []spec.ServicePortTraitSpec{
					{Name: "http", Port: 80, TargetPort: 8080},
				},
			},
			{
				Name:     "custom-headless-web",
				Headless: true,
				Labels: map[string]string{
					config.LabelAppID:         "not-app-reset",
					config.LabelComponentName: "not-web",
				},
				Selector: map[string]string{"role": "web"},
				Ports: []spec.ServicePortTraitSpec{
					{Name: "grpc", Port: 9090, TargetPort: 9090},
				},
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    props,
		Traits:        traits,
	}
	client := fake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-public-web",
			Namespace: component.Namespace,
			Labels: map[string]string{
				config.LabelAppID:         "other-app",
				config.LabelComponentName: "other-component",
			},
		}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-headless-web",
			Namespace: component.Namespace,
			Labels: map[string]string{
				config.LabelAppID:         "not-app-reset",
				config.LabelComponentName: "not-web",
			},
		}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))

	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, "custom-public-web", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, "custom-headless-web", metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err))
}

func TestCleanupResourcesJobCtlKeepsProtectedTraitDefinedServices(t *testing.T) {
	ctx := context.Background()
	props, err := model.NewJSONStructByStruct(model.Properties{})
	require.NoError(t, err)
	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "custom-public-web",
				Labels: map[string]string{
					config.LabelAppID:         "other-app",
					config.LabelComponentName: "other-component",
					config.LabelShareName:     "shared-public-web",
					config.LabelShareStrategy: string(config.ShareStrategyDefault),
				},
				Selector: map[string]string{"role": "web"},
				Ports: []spec.ServicePortTraitSpec{
					{Name: "http", Port: 80, TargetPort: 8080},
				},
			},
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		ID:            1,
		Name:          "web",
		AppID:         "app-reset",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Properties:    props,
		Traits:        traits,
	}
	client := fake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name:      "custom-public-web",
			Namespace: component.Namespace,
			Labels: map[string]string{
				config.LabelAppID:         "other-app",
				config.LabelComponentName: "other-component",
				config.LabelShareName:     "shared-public-web",
				config.LabelShareStrategy: string(config.ShareStrategyDefault),
			},
		}},
	)
	store := &cleanupComponentStore{component: component}
	task := &model.JobTask{
		Name:      component.Name,
		Namespace: component.Namespace,
		AppID:     component.AppID,
		JobType:   string(config.JobCleanupResources),
		JobInfo:   component,
		Timeout:   1,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)

	require.NoError(t, ctl.Run(ctx))
	require.Equal(t, config.StatusCompleted, task.Status)

	_, err = client.CoreV1().Services(component.Namespace).Get(ctx, "custom-public-web", metav1.GetOptions{})
	require.NoError(t, err)
}
