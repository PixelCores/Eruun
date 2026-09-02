package workflow

import (
	"context"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	wfNaming "github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func TestCreateObjectJobsFromResultIngressNaming(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "Gateway",
		AppID:     "App-1",
		Namespace: "default",
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-1",
		ProjectID:  "proj-1",
		AppID:      "App-1",
	}

	t.Run("auto name when ingress missing name", func(t *testing.T) {
		ing := &networkingv1.Ingress{}
		jobs, err := CreateObjectJobsFromResult([]client.Object{ing}, component, task, nil, int64(config.DefaultJobTaskTimeout))
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		expected := wfNaming.IngressName(component.Name, component.ResourceAppNameOrID())
		require.Equal(t, expected, jobs[0].Name)

		ingressObj, ok := jobs[0].JobInfo.(*networkingv1.Ingress)
		require.True(t, ok)
		require.Equal(t, expected, ingressObj.Name)
		require.Equal(t, component.Namespace, ingressObj.Namespace)
	})

	t.Run("normalize pvc name and namespace", func(t *testing.T) {
		baseName := "DataVol"
		canonical := wfNaming.PVCName(baseName, component.ResourceAppNameOrID())
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      canonical,
				Namespace: component.Namespace,
			},
		}

		j, err := CreateObjectJobsFromResult([]client.Object{pvc}, component, task, nil, int64(config.DefaultJobTaskTimeout))
		require.NoError(t, err)
		require.Len(t, j, 1)
		require.Equal(t, canonical, j[0].Name)

		pvcObj, ok := j[0].JobInfo.(*corev1.PersistentVolumeClaim)
		require.True(t, ok)
		require.Equal(t, canonical, pvcObj.Name)
		require.Equal(t, component.Namespace, pvcObj.Namespace)
	})

	t.Run("fill namespace when pvc missing it", func(t *testing.T) {
		canonical := wfNaming.PVCName("cache", component.ResourceAppNameOrID())
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: canonical,
			},
		}

		j, err := CreateObjectJobsFromResult([]client.Object{pvc}, component, task, nil, int64(config.DefaultJobTaskTimeout))
		require.NoError(t, err)
		require.Len(t, j, 1)

		pvcObj, ok := j[0].JobInfo.(*corev1.PersistentVolumeClaim)
		require.True(t, ok)
		require.Equal(t, component.Namespace, pvcObj.Namespace)
		require.Equal(t, canonical, j[0].Name)
	})

	t.Run("normalize existing ingress name", func(t *testing.T) {
		ing := &networkingv1.Ingress{}
		baseName := "CustomRoute"
		ing.Name = baseName

		jobs, err := CreateObjectJobsFromResult([]client.Object{ing}, component, task, nil, int64(config.DefaultJobTaskTimeout))
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		expected := wfNaming.IngressName(baseName, component.ResourceAppNameOrID())
		require.Equal(t, expected, jobs[0].Name)

		ingressObj, ok := jobs[0].JobInfo.(*networkingv1.Ingress)
		require.True(t, ok)
		require.Equal(t, expected, ingressObj.Name)
		require.Equal(t, component.Namespace, ingressObj.Namespace)
	})

	t.Run("append system labels to ingress", func(t *testing.T) {
		ing := &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					config.LabelAppID:         "evil-app",
					config.LabelComponentName: "wrong-component",
					"layer":                   "db",
				},
			},
		}
		jobs, err := CreateObjectJobsFromResult([]client.Object{ing}, component, task, nil, int64(config.DefaultJobTaskTimeout))
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		ingressObj, ok := jobs[0].JobInfo.(*networkingv1.Ingress)
		require.True(t, ok)
		require.Equal(t, wfNaming.NormalizeLabelValue("App-1"), ingressObj.Labels[config.LabelAppID])
		require.Equal(t, wfNaming.BoundedLabelValue("Gateway"), ingressObj.Labels[config.LabelComponentName])
		require.Equal(t, "db", ingressObj.Labels["layer"])
	})
}

func TestCreateObjectJobsFromResultIgnoresConfigAndSecret(t *testing.T) {
	component := &model.ApplicationComponent{Name: "app", Namespace: "demo", AppID: "aid"}
	task := &model.WorkflowQueue{WorkflowID: "wf", ProjectID: "proj", AppID: "aid"}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "app-config"}, Data: map[string]string{"key": "value"}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "app-secret"}}
	jobs, err := CreateObjectJobsFromResult([]client.Object{cm, secret}, component, task, nil, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Empty(t, jobs, "configmap/secret should be ignored; dedicated jobs exist elsewhere")
}

func TestCreateObjectJobsFromResultRBAC(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:      "Labeler",
		AppID:     "App-2",
		Namespace: "ops",
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-rbac",
		ProjectID:  "proj-rbac",
		AppID:      component.AppID,
	}

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-sa"}}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-role"},
		Rules: []rbacv1.PolicyRule{{
			Verbs:     []string{"get"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		}},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-binding"},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      sa.Name,
			Namespace: component.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			APIGroup: rbacv1.GroupName,
			Name:     role.Name,
		},
	}
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-role"},
		Rules: []rbacv1.PolicyRule{{
			Verbs:     []string{"list"},
			APIGroups: []string{""},
			Resources: []string{"pods"},
		}},
	}
	clusterBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-labeler-cluster-binding"},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      sa.Name,
			Namespace: component.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			APIGroup: rbacv1.GroupName,
			Name:     clusterRole.Name,
		},
	}

	objs := []client.Object{sa, role, binding, clusterRole, clusterBinding}
	jobs, err := CreateObjectJobsFromResult(objs, component, task, nil, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Len(t, jobs, 5)

	jobTypes := make(map[string]bool)
	for _, job := range jobs {
		jobTypes[job.JobType] = true
		require.NotNil(t, job.JobInfo)
		var labels map[string]string
		switch job.JobType {
		case string(config.JobDeployServiceAccount):
			saObj, ok := job.JobInfo.(*corev1.ServiceAccount)
			require.True(t, ok)
			require.Equal(t, component.Namespace, saObj.Namespace)
			labels = saObj.Labels
		case string(config.JobDeployRole):
			roleObj, ok := job.JobInfo.(*rbacv1.Role)
			require.True(t, ok)
			require.Equal(t, component.Namespace, roleObj.Namespace)
			labels = roleObj.Labels
		case string(config.JobDeployRoleBinding):
			bindingObj, ok := job.JobInfo.(*rbacv1.RoleBinding)
			require.True(t, ok)
			require.Equal(t, component.Namespace, bindingObj.Namespace)
			labels = bindingObj.Labels
		case string(config.JobDeployClusterRole):
			clusterRoleObj, ok := job.JobInfo.(*rbacv1.ClusterRole)
			require.True(t, ok)
			labels = clusterRoleObj.Labels
		case string(config.JobDeployClusterRoleBinding):
			clusterBindingObj, ok := job.JobInfo.(*rbacv1.ClusterRoleBinding)
			require.True(t, ok)
			labels = clusterBindingObj.Labels
		default:
			t.Fatalf("unexpected job type %s", job.JobType)
		}
		require.Equal(t, "ops", labels[config.LabelShareName])
		require.Equal(t, string(config.ShareStrategyDefault), labels[config.LabelShareStrategy])
	}

	require.True(t, jobTypes[string(config.JobDeployServiceAccount)])
	require.True(t, jobTypes[string(config.JobDeployRole)])
	require.True(t, jobTypes[string(config.JobDeployRoleBinding)])
	require.True(t, jobTypes[string(config.JobDeployClusterRole)])
	require.True(t, jobTypes[string(config.JobDeployClusterRoleBinding)])
}

func TestSecretJobNameNormalization(t *testing.T) {
	secretProps, err := model.NewJSONStructByStruct(model.Properties{Secret: map[string]string{"token": "value"}})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "ApiKey",
		AppID:         "App-Env",
		Namespace:     "",
		ComponentType: config.SecretJob,
		Properties:    secretProps,
	}

	task := &model.WorkflowQueue{
		WorkflowID: "wf-secret",
		ProjectID:  "proj-1",
		AppID:      component.AppID,
	}

	ctx := context.Background()
	buckets := buildJobsForComponent(ctx, component, task, int64(config.DefaultJobTaskTimeout), "")
	jobs := buckets[config.JobPriorityMaxHigh]
	require.Len(t, jobs, 1)

	expectedName := component.Name
	require.Equal(t, expectedName, jobs[0].Name)

	secretInput, ok := jobs[0].JobInfo.(*model.SecretInput)
	require.True(t, ok)
	require.Equal(t, expectedName, secretInput.Name)
	require.Equal(t, config.DefaultNamespace, secretInput.Namespace)
}

func TestBuildJobsForComponent_ShareIgnoreSkipsJobs(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Share: &spec.ShareTraitSpec{
			Strategy: string(config.ShareStrategyIgnore),
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "proxy",
		AppID:         "app-share",
		Namespace:     "default",
		ComponentType: config.ServerJob,
		Traits:        traitsJSON,
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-share",
		ProjectID:  "proj-share",
		AppID:      component.AppID,
		TaskID:     "task-share",
	}

	buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
	require.Greater(t, countJobs(buckets), 0)
	for _, jobs := range buckets {
		for _, job := range jobs {
			require.Equal(t, config.StatusSkipped, job.Status)
		}
	}
}

func TestBuildJobsForComponentAppliesFailurePolicyOnlyToInstantJobTask(t *testing.T) {
	traitsPlu.RegisterAllProcessors()
	failurePolicy := workflowconfig.WorkflowFailurePolicyCleanupFailed
	propertiesJSON, err := model.NewJSONStructByStruct(model.Properties{
		RunPolicy:     string(workflowconfig.JobRunPolicyRecreate),
		FailurePolicy: &failurePolicy,
	})
	require.NoError(t, err)
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{{
			Name:      "data",
			Type:      "persistent",
			MountPath: "/data",
			Size:      "1Gi",
			ClaimName: "mysql-update-data",
			TmpCreate: false,
		}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "mysql-update-job",
		AppID:         "app-1",
		Namespace:     "default",
		ComponentType: config.InstantJob,
		Image:         "skeema-tool:latest",
		Properties:    propertiesJSON,
		Traits:        traitsJSON,
	}
	task := &model.WorkflowQueue{WorkflowID: "wf-1", AppID: "app-1", TaskID: "task-1"}

	buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
	require.Len(t, buckets[config.JobPriorityNormal], 1)
	require.Equal(t, workflowconfig.WorkflowFailurePolicyCleanupFailed, buckets[config.JobPriorityNormal][0].FailurePolicy)
	require.NotEmpty(t, buckets[config.JobPriorityHigh])
	for _, additionalTask := range buckets[config.JobPriorityHigh] {
		require.Empty(t, additionalTask.FailurePolicy)
	}
}

func TestBuildJobsForComponentAddsShareLabelsToWorkloadPodTemplates(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Share: &spec.ShareTraitSpec{
			Strategy: string(config.ShareStrategyDefault),
		},
	})
	require.NoError(t, err)
	propsJSON, err := model.NewJSONStructByStruct(model.Properties{
		Ports:    []model.Ports{{Port: 80}},
		Schedule: "* * * * *",
	})
	require.NoError(t, err)

	task := &model.WorkflowQueue{
		WorkflowID: "wf-share",
		ProjectID:  "proj-share",
		AppID:      "app-share",
		TaskID:     "task-share",
	}
	shareAssertions := func(t *testing.T, labels map[string]string) {
		t.Helper()
		require.Equal(t, "default", labels[config.LabelShareName])
		require.Equal(t, string(config.ShareStrategyDefault), labels[config.LabelShareStrategy])
	}
	selectorAssertions := func(t *testing.T, labels map[string]string) {
		t.Helper()
		require.NotContains(t, labels, config.LabelShareName)
		require.NotContains(t, labels, config.LabelShareStrategy)
	}

	t.Run("deployment", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:          "proxy",
			AppID:         task.AppID,
			Namespace:     "default",
			Image:         "nginx:1.21",
			Replicas:      1,
			ComponentType: config.ServerJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
		buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
		deploy, ok := buckets[config.JobPriorityNormal][0].JobInfo.(*appsv1.Deployment)
		require.True(t, ok)
		require.Equal(t, "proxy", deploy.Name)
		shareAssertions(t, deploy.Labels)
		shareAssertions(t, deploy.Spec.Template.Labels)
		selectorAssertions(t, deploy.Spec.Selector.MatchLabels)
		svc, ok := buckets[config.JobPriorityHigh][0].JobInfo.(*applyv1.ServiceApplyConfiguration)
		require.True(t, ok)
		require.Equal(t, "proxy", *svc.Name)
	})

	t.Run("statefulset", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:          "store",
			AppID:         task.AppID,
			Namespace:     "default",
			Image:         "mysql:8",
			Replicas:      1,
			ComponentType: config.StoreJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
		buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
		sts, ok := buckets[config.JobPriorityNormal][0].JobInfo.(*appsv1.StatefulSet)
		require.True(t, ok)
		require.Equal(t, "store", sts.Name)
		require.Equal(t, "store", sts.Spec.ServiceName)
		shareAssertions(t, sts.Labels)
		shareAssertions(t, sts.Spec.Template.Labels)
		selectorAssertions(t, sts.Spec.Selector.MatchLabels)
	})

	t.Run("job", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:          "batch",
			AppID:         task.AppID,
			Namespace:     "default",
			Image:         "busybox:1.36",
			ComponentType: config.InstantJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
		buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
		jobInfo, ok := buckets[config.JobPriorityNormal][0].JobInfo.(*batchv1.Job)
		require.True(t, ok)
		require.Equal(t, "batch", jobInfo.Name)
		shareAssertions(t, jobInfo.Labels)
		shareAssertions(t, jobInfo.Spec.Template.Labels)
	})

	t.Run("cronjob", func(t *testing.T) {
		component := &model.ApplicationComponent{
			Name:          "scheduled",
			AppID:         task.AppID,
			Namespace:     "default",
			Image:         "busybox:1.36",
			ComponentType: config.ScheduledJob,
			Properties:    propsJSON,
			Traits:        traitsJSON,
		}
		buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
		cronInfo, ok := buckets[config.JobPriorityNormal][0].JobInfo.(*batchv1.CronJob)
		require.True(t, ok)
		require.Equal(t, "scheduled", cronInfo.Name)
		shareAssertions(t, cronInfo.Labels)
		shareAssertions(t, cronInfo.Spec.JobTemplate.Labels)
		shareAssertions(t, cronInfo.Spec.JobTemplate.Spec.Template.Labels)
	})
}

func TestCreateObjectJobsFromResult_ShareIgnoreSkipsAdditionalJobs(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Share: &spec.ShareTraitSpec{
			Strategy: string(config.ShareStrategyIgnore),
		},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "cache",
		AppID:         "app-share",
		Namespace:     "demo",
		ComponentType: config.StoreJob,
		Traits:        traitsJSON,
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-share",
		ProjectID:  "proj-share",
		AppID:      component.AppID,
		TaskID:     "task-share",
	}

	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data"}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "cache-sa"}}
	jobs, err := CreateObjectJobsFromResult([]client.Object{pvc, sa}, component, task, nil, int64(config.DefaultJobTaskTimeout))
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	for _, job := range jobs {
		require.Equal(t, config.StatusSkipped, job.Status)
	}
}

func TestBuildJobsForComponent_ServiceTraitOverridesLegacyServiceGeneration(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name: "mysql-master",
				Selector: map[string]string{
					"mysql-pod-role": "master",
				},
				Labels: map[string]string{
					"layer": "db",
				},
				Ports: []spec.ServicePortTraitSpec{
					{
						Name:       "mysql",
						Port:       3306,
						TargetPort: 3306,
						Protocol:   "TCP",
					},
				},
			},
			{
				Name:     "mysql-headless",
				Headless: true,
				Selector: map[string]string{
					"name": "mysql",
				},
				Ports: []spec.ServicePortTraitSpec{
					{
						Name:       "mysql",
						Port:       3306,
						TargetPort: 3306,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	propsJSON, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 80}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "mysql",
		AppID:         "app-service",
		Namespace:     "nvg",
		ComponentType: config.ServerJob,
		Image:         "mysql:8.0",
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-service",
		ProjectID:  "proj-service",
		AppID:      component.AppID,
		TaskID:     "task-service",
	}

	buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
	normalJobs := buckets[config.JobPriorityNormal]
	require.Len(t, normalJobs, 1)
	_, ok := normalJobs[0].JobInfo.(*appsv1.Deployment)
	require.True(t, ok)

	var serviceJobs []*model.JobTask
	for _, jobTask := range buckets[config.JobPriorityHigh] {
		if jobTask.JobType == string(config.JobDeployService) {
			serviceJobs = append(serviceJobs, jobTask)
		}
	}
	require.Len(t, serviceJobs, 2)

	expectedServiceNames := map[string]bool{
		"mysql-master":   false,
		"mysql-headless": false,
	}

	for _, serviceJob := range serviceJobs {
		svcInfo, ok := serviceJob.JobInfo.(*applyv1.ServiceApplyConfiguration)
		require.True(t, ok)
		require.NotNil(t, svcInfo.Name)
		require.NotNil(t, svcInfo.Spec)

		name := *svcInfo.Name
		_, exists := expectedServiceNames[name]
		require.True(t, exists, "unexpected service name %s", name)
		expectedServiceNames[name] = true

		switch name {
		case "mysql-master":
			require.Equal(t, "master", svcInfo.Spec.Selector["mysql-pod-role"])
			require.Equal(t, "db", svcInfo.Labels["layer"])
			require.NotContains(t, svcInfo.Spec.Selector, config.LabelAppID)
		case "mysql-headless":
			require.Equal(t, "mysql", svcInfo.Spec.Selector["name"])
			require.NotNil(t, svcInfo.Spec.ClusterIP)
			require.Equal(t, corev1.ClusterIPNone, *svcInfo.Spec.ClusterIP)
		}
	}

	for svcName, found := range expectedServiceNames {
		require.True(t, found, "expected generated service %s", svcName)
	}
}

func TestBuildJobsForComponent_StoreServiceTraitPrecedesStatefulSet(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name:     "mysql",
				Type:     string(config.ServiceAccessInternal),
				Headless: true,
				Selector: map[string]string{
					"name": "mysql",
				},
				Ports: []spec.ServicePortTraitSpec{
					{
						Name:       "mysql",
						Port:       3306,
						TargetPort: 3306,
						Protocol:   "TCP",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	propsJSON, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 3306}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "mysql-8",
		AppID:         "app-store",
		Namespace:     "nvg",
		ComponentType: config.StoreJob,
		Image:         "mysql:8.0",
		Replicas:      2,
		Properties:    propsJSON,
		Traits:        traitsJSON,
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-store",
		ProjectID:  "proj-store",
		AppID:      component.AppID,
		TaskID:     "task-store",
	}

	buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
	serviceJobs := buckets[config.JobPriorityHigh]
	require.Len(t, serviceJobs, 1)
	require.Equal(t, string(config.JobDeployService), serviceJobs[0].JobType)
	svcInfo, ok := serviceJobs[0].JobInfo.(*applyv1.ServiceApplyConfiguration)
	require.True(t, ok)
	require.NotNil(t, svcInfo.Name)
	require.Equal(t, "mysql", *svcInfo.Name)

	normalJobs := buckets[config.JobPriorityNormal]
	require.Len(t, normalJobs, 1)
	statefulSet, ok := normalJobs[0].JobInfo.(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, "mysql", statefulSet.Spec.ServiceName)
}

func TestBuildJobsForComponent_DefaultServiceFromPortsUsesHighPriority(t *testing.T) {
	propsJSON, err := model.NewJSONStructByStruct(model.Properties{
		Ports: []model.Ports{{Port: 8080}},
	})
	require.NoError(t, err)

	component := &model.ApplicationComponent{
		Name:          "api",
		AppID:         "app-ports",
		Namespace:     "nvg",
		ComponentType: config.ServerJob,
		Image:         "api:latest",
		Properties:    propsJSON,
	}
	task := &model.WorkflowQueue{
		WorkflowID: "wf-ports",
		ProjectID:  "proj-ports",
		AppID:      component.AppID,
		TaskID:     "task-ports",
	}

	buckets := buildJobsForComponent(context.Background(), component, task, int64(config.DefaultJobTaskTimeout), "")
	serviceJobs := buckets[config.JobPriorityHigh]
	require.Len(t, serviceJobs, 1)
	require.Equal(t, string(config.JobDeployService), serviceJobs[0].JobType)
	svcInfo, ok := serviceJobs[0].JobInfo.(*applyv1.ServiceApplyConfiguration)
	require.True(t, ok)
	require.NotNil(t, svcInfo.Spec)
	require.Len(t, svcInfo.Spec.Ports, 1)
	require.Equal(t, int32(8080), *svcInfo.Spec.Ports[0].Port)

	normalJobs := buckets[config.JobPriorityNormal]
	require.Len(t, normalJobs, 1)
	_, ok = normalJobs[0].JobInfo.(*appsv1.Deployment)
	require.True(t, ok)
}

func TestServiceTraitsForComponent_FallbackFromProperties(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:  "api",
		AppID: "app-1",
	}
	properties := &model.Properties{
		Ports: []model.Ports{
			{Port: 8080},
			{Port: 9090},
		},
	}

	traits := serviceTraitsForComponent(component, properties)
	require.Len(t, traits, 1)
	require.Empty(t, traits[0].Name)
	require.Equal(t, "internal", traits[0].Type)
	require.Equal(t, "app-1", traits[0].Selector[config.LabelAppID])
	require.Equal(t, "api", traits[0].Selector[config.LabelComponentName])
	require.Len(t, traits[0].Ports, 2)
	require.Equal(t, int32(8080), traits[0].Ports[0].Port)
	require.Equal(t, int32(8080), traits[0].Ports[0].TargetPort)
	require.Equal(t, int32(9090), traits[0].Ports[1].Port)
	require.Equal(t, int32(9090), traits[0].Ports[1].TargetPort)
}
