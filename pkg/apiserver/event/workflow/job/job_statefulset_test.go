package job

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func int32Ptr(v int32) *int32 { return &v }
func int64Ptr(v int64) *int64 { return &v }
func boolPtr(v bool) *bool    { return &v }

func TestDeployStatefulSetJobCtl_UpdateExisting(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := WithCleanupTracker(context.Background())

	job := &model.JobTask{
		Name:      "mysql",
		Namespace: "ops",
		AppID:     "app-1",
		TaskID:    "task-sts-template",
		JobType:   string(config.JobDeployStore),
	}
	statefulSetName := buildStoreSeverName(job.Name, job.AppID)

	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: "ops",
			Labels:    map[string]string{"app": "mysql"},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "mysql",
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "mysql"},
			},
			Replicas: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "mysql"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: "mysql:5.7",
					}},
				},
			},
		},
	}
	if _, err := client.AppsV1().StatefulSets("ops").Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to seed statefulset: %v", err)
	}

	desired := existing.DeepCopy()
	desired.Spec.Replicas = int32Ptr(2)
	desired.Spec.Template.Spec.Containers[0].Image = "mysql:8.0"
	desired.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}

	job.JobInfo = desired
	ctl := NewDeployStatefulSetJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	if err := ctl.run(ctx); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	updated, err := client.AppsV1().StatefulSets("ops").Get(ctx, statefulSetName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected statefulset to exist: %v", err)
	}
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 2 {
		t.Fatalf("expected replicas updated to 2, got %v", updated.Spec.Replicas)
	}
	if len(updated.Spec.Template.Spec.Containers) == 0 || updated.Spec.Template.Spec.Containers[0].Image != "mysql:8.0" {
		t.Fatalf("expected image updated to mysql:8.0, got %#v", updated.Spec.Template.Spec.Containers)
	}
	require.Equal(t, "task-sts-template", updated.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{
		config.AnnotationJobTaskID: "task-sts-template",
	}, ctl.expectedPodTemplateAnnotations)
	if updated.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		t.Fatalf("expected retention policy to be set")
	}
	if updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected whenDeleted=Delete, got %s", updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	}
	if updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected whenScaled=Retain, got %s", updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	}
}

func TestDeployStatefulSetJobCtl_SkipsUnchangedUpdate(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := WithCleanupTracker(context.Background())

	job := &model.JobTask{
		Name:      "mysql",
		Namespace: "ops",
		AppID:     "app-1",
		TaskID:    "task-sts-noop",
		JobType:   string(config.JobDeployStore),
	}
	statefulSetName := buildStoreSeverName(job.Name, job.AppID)

	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        statefulSetName,
			Namespace:   "ops",
			Labels:      map[string]string{"app": "mysql"},
			Annotations: map[string]string{"owner": "eruun"},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "mysql",
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "mysql"},
			},
			Replicas: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "mysql"},
					Annotations: map[string]string{"owner": "eruun"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: "mysql:8.0",
					}},
					RestartPolicy:                 corev1.RestartPolicyAlways,
					DNSPolicy:                     corev1.DNSClusterFirst,
					SchedulerName:                 corev1.DefaultSchedulerName,
					TerminationGracePeriodSeconds: int64Ptr(30),
					EnableServiceLinks:            boolPtr(true),
				},
			},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
	if _, err := client.AppsV1().StatefulSets("ops").Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to seed statefulset: %v", err)
	}

	desired := existing.DeepCopy()
	desired.Spec.Template.Spec.RestartPolicy = ""
	desired.Spec.Template.Spec.DNSPolicy = ""
	desired.Spec.Template.Spec.SchedulerName = ""
	desired.Spec.Template.Spec.TerminationGracePeriodSeconds = nil
	desired.Spec.Template.Spec.EnableServiceLinks = nil
	job.JobInfo = desired
	ctl := NewDeployStatefulSetJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	if err := ctl.run(ctx); err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if updates := countClientActions(client, "update", "statefulsets"); updates != 0 {
		t.Fatalf("expected no statefulset update, got %d", updates)
	}
	updated, err := client.AppsV1().StatefulSets("ops").Get(ctx, statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, updated.Spec.Template.Annotations, config.AnnotationJobTaskID)
	require.Empty(t, ctl.expectedPodTemplateAnnotations)
}

func TestDeployStatefulSetJobCtl_RestoresTaskAnnotationForUpToDateStatefulSet(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := WithCleanupTracker(context.Background())

	job := &model.JobTask{
		Name:      "mysql",
		Namespace: "ops",
		AppID:     "app-1",
		TaskID:    "task-sts-template",
		JobType:   string(config.JobDeployStore),
	}
	statefulSetName := buildStoreSeverName(job.Name, job.AppID)

	existing := comparableStatefulSet()
	existing.Name = statefulSetName
	existing.Namespace = "ops"
	existing.Spec.Template.Annotations[config.AnnotationJobTaskID] = "task-sts-template"
	if _, err := client.AppsV1().StatefulSets("ops").Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to seed statefulset: %v", err)
	}

	desired := comparableStatefulSet()
	desired.Name = statefulSetName
	desired.Namespace = "ops"
	job.JobInfo = desired
	ctl := NewDeployStatefulSetJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, ctl.run(ctx))

	updated, err := client.AppsV1().StatefulSets("ops").Get(ctx, statefulSetName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "task-sts-template", updated.Spec.Template.Annotations[config.AnnotationJobTaskID])
	require.Equal(t, map[string]string{
		config.AnnotationJobTaskID: "task-sts-template",
	}, ctl.expectedPodTemplateAnnotations)
	require.Equal(t, 0, countClientActions(client, "update", "statefulsets"))
}

func TestGenerateStoreService_UsesCommand(t *testing.T) {
	properties := model.Properties{
		Command: []string{"sh", "-c", "echo ok"},
	}
	propsJSON, err := model.NewJSONStructByStruct(properties)
	if err != nil {
		t.Fatalf("failed to build properties json: %v", err)
	}

	component := &model.ApplicationComponent{
		Name:       "log-tick-store",
		AppID:      "app-1",
		Namespace:  "default",
		Image:      "busybox:1.36",
		Replicas:   1,
		Properties: propsJSON,
	}

	result := GenerateStoreService(component)
	if result == nil {
		t.Fatalf("expected result, got nil")
	}

	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	if !ok || statefulSet == nil {
		t.Fatalf("expected statefulset service, got %#v", result.Service)
	}
	if len(statefulSet.Spec.Template.Spec.Containers) == 0 {
		t.Fatalf("expected at least one container")
	}

	got := statefulSet.Spec.Template.Spec.Containers[0].Command
	if !reflect.DeepEqual(got, properties.Command) {
		t.Fatalf("expected command %v, got %v", properties.Command, got)
	}
	require.Equal(t, workflowconfig.DefaultWorkflowImagePullPolicy, statefulSet.Spec.Template.Spec.Containers[0].ImagePullPolicy)
	require.NotNil(t, statefulSet.Spec.PersistentVolumeClaimRetentionPolicy)
	require.Equal(t, appsv1.DeletePersistentVolumeClaimRetentionPolicyType, statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, statefulSet.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
}

func TestGenerateStoreService_UsesHeadlessServiceTraitName(t *testing.T) {
	traitsJSON, err := model.NewJSONStructByStruct(spec.Traits{
		Service: []spec.ServiceTraitSpec{
			{
				Name:     "mysql",
				Type:     "internal",
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

	component := &model.ApplicationComponent{
		Name:          "mysql-8",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      2,
		ComponentType: config.StoreJob,
		Traits:        traitsJSON,
	}

	result := GenerateStoreService(component)
	require.NotNil(t, result)

	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, "mysql", statefulSet.Spec.ServiceName)
}

func TestGenerateStoreService_DefaultsServiceNameWithoutHeadlessTrait(t *testing.T) {
	component := &model.ApplicationComponent{
		Name:          "mysql-8",
		AppID:         "app-1",
		Namespace:     "default",
		Image:         "mysql:8.0",
		Replicas:      1,
		ComponentType: config.StoreJob,
	}

	result := GenerateStoreService(component)
	require.NotNil(t, result)

	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, buildServiceName(component.Name, component.ResourceAppNameOrID()), statefulSet.Spec.ServiceName)
}

func TestGenerateStoreService_BoundsStatefulSetNameAndUsesStableSelector(t *testing.T) {
	properties, err := model.NewJSONStructByStruct(model.Properties{
		Labels: map[string]string{
			"name": "penalty shootout 2026-m2606241344ccufxh-mysql",
		},
	})
	require.NoError(t, err)
	component := &model.ApplicationComponent{
		Name:            "m2605081521cctqpk-redis-redis",
		AppID:           "lyhemnnmr48fmmifdf3f1ukl",
		ResourceAppName: "m2605081521cctqpk",
		Namespace:       "default",
		Image:           "redis:6.2",
		Replicas:        1,
		ComponentType:   config.StoreJob,
		Properties:      properties,
	}

	result := GenerateStoreService(component)
	require.NotNil(t, result)

	statefulSet, ok := result.Service.(*appsv1.StatefulSet)
	require.True(t, ok)
	require.Equal(t, "m2605081521cctqpk-redis-redis", statefulSet.Name)
	require.LessOrEqual(t, len(statefulSet.Name), 52)
	require.LessOrEqual(t, len(statefulSet.Labels[config.LabelManagedBy]), 63)
	require.Equal(t, map[string]string{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(component.Name),
	}, statefulSet.Spec.Selector.MatchLabels)
	require.Equal(t, component.AppID, statefulSet.Spec.Template.Labels[config.LabelAppID])
	require.Equal(t, naming.BoundedLabelValue(component.Name), statefulSet.Spec.Template.Labels[config.LabelComponentName])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-mysql", statefulSet.Labels["name"])
	require.Equal(t, "penalty-shootout-2026-m2606241344ccufxh-mysql", statefulSet.Spec.Template.Labels["name"])
}

func TestBuildUpdatedStatefulSetCopiesPVCRetentionPolicy(t *testing.T) {
	current := &appsv1.StatefulSet{}
	desired := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}

	updated := buildUpdatedStatefulSet(current, desired)
	require.NotNil(t, updated.Spec.PersistentVolumeClaimRetentionPolicy)
	require.Equal(t, appsv1.DeletePersistentVolumeClaimRetentionPolicyType, updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, updated.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
}

func TestStatefulSetNeedsUpdateWhenPVCRetentionPolicyDiffers(t *testing.T) {
	current := &appsv1.StatefulSet{}
	desired := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}

	require.True(t, statefulSetNeedsUpdate(current, desired))
}

func TestStatefulSetNeedsUpdateIgnoresPodSpecDefaults(t *testing.T) {
	current, desired := statefulSetWithDefaultedCurrentAndBareDesired()

	require.False(t, statefulSetNeedsUpdate(current, desired))
}

func TestStatefulSetNeedsUpdateDoesNotMutateInputs(t *testing.T) {
	current, desired := statefulSetWithDefaultedCurrentAndBareDesired()
	currentBefore := current.DeepCopy()
	desiredBefore := desired.DeepCopy()

	require.False(t, statefulSetNeedsUpdate(current, desired))
	require.Equal(t, currentBefore, current)
	require.Equal(t, desiredBefore, desired)
}

func statefulSetWithDefaultedCurrentAndBareDesired() (*appsv1.StatefulSet, *appsv1.StatefulSet) {
	current := comparableStatefulSet()
	current.ResourceVersion = "12"
	current.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager:    "kube-apiserver",
		Operation:  metav1.ManagedFieldsOperationUpdate,
		APIVersion: "apps/v1",
	}}
	current.Status.ReadyReplicas = 1
	current.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	current.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	current.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
	current.Spec.Template.Spec.TerminationGracePeriodSeconds = int64Ptr(30)
	current.Spec.Template.Spec.EnableServiceLinks = boolPtr(true)
	current.Spec.Template.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	current.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	current.Spec.Template.Spec.Containers[0].Ports[0].Protocol = corev1.ProtocolTCP
	current.Spec.Template.Spec.InitContainers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	current.Spec.Template.Spec.InitContainers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	current.Spec.Template.Spec.Containers[0].LivenessProbe = defaultedHTTPProbe("/health", 8080)
	current.Spec.Template.Spec.Containers[0].StartupProbe = defaultedTCPProbe(3306)
	current.Spec.Template.Spec.InitContainers[0].ReadinessProbe = defaultedExecProbe("test", "-f", "/tmp/ready")

	desired := comparableStatefulSet()
	desired.Spec.Template.Spec.RestartPolicy = ""
	desired.Spec.Template.Spec.DNSPolicy = ""
	desired.Spec.Template.Spec.SchedulerName = ""
	desired.Spec.Template.Spec.TerminationGracePeriodSeconds = nil
	desired.Spec.Template.Spec.EnableServiceLinks = nil
	desired.Spec.Template.Spec.Containers[0].TerminationMessagePath = ""
	desired.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = ""
	desired.Spec.Template.Spec.Containers[0].Ports[0].Protocol = ""
	desired.Spec.Template.Spec.InitContainers[0].TerminationMessagePath = ""
	desired.Spec.Template.Spec.InitContainers[0].TerminationMessagePolicy = ""
	desired.Spec.Template.Spec.Containers[0].LivenessProbe = bareHTTPProbe("/health", 8080)
	desired.Spec.Template.Spec.Containers[0].StartupProbe = bareTCPProbe(3306)
	desired.Spec.Template.Spec.InitContainers[0].ReadinessProbe = bareExecProbe("test", "-f", "/tmp/ready")

	return current, desired
}

func TestStatefulSetNeedsUpdateDetectsControlledFieldChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*appsv1.StatefulSet)
	}{
		{
			name: "replicas",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Replicas = int32Ptr(2)
			},
		},
		{
			name: "container image",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Template.Spec.Containers[0].Image = "mysql:8.4"
			},
		},
		{
			name: "container port",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort = 3307
			},
		},
		{
			name: "labels",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Labels["tier"] = "store"
			},
		},
		{
			name: "annotations",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Annotations["revision"] = "2"
			},
		},
		{
			name: "template config",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Template.Spec.NodeSelector["disk"] = "ssd"
			},
		},
		{
			name: "service account",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Template.Spec.ServiceAccountName = "mysql-v2"
			},
		},
		{
			name: "automount token",
			mutate: func(sts *appsv1.StatefulSet) {
				sts.Spec.Template.Spec.AutomountServiceAccountToken = boolPtr(false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			current := comparableStatefulSet()
			desired := comparableStatefulSet()
			tc.mutate(desired)

			require.True(t, statefulSetNeedsUpdate(current, desired))
		})
	}
}

func TestStatefulSetNeedsUpdateDetectsProbeChanges(t *testing.T) {
	current := comparableStatefulSet()
	current.Spec.Template.Spec.Containers[0].LivenessProbe = defaultedHTTPProbe("/health", 8080)
	desiredPath := comparableStatefulSet()
	desiredPath.Spec.Template.Spec.Containers[0].LivenessProbe = bareHTTPProbe("/ready", 8080)
	require.True(t, statefulSetNeedsUpdate(current, desiredPath))

	desiredPeriod := comparableStatefulSet()
	periodProbe := bareHTTPProbe("/health", 8080)
	periodProbe.PeriodSeconds = 5
	desiredPeriod.Spec.Template.Spec.Containers[0].LivenessProbe = periodProbe
	require.True(t, statefulSetNeedsUpdate(current, desiredPeriod))
}

func TestStatefulSetPodTemplateChangedPreservesExistingTaskIDAnnotation(t *testing.T) {
	current := comparableStatefulSet()
	current.Spec.Template.Annotations[config.AnnotationJobTaskID] = "old-task"
	desired := comparableStatefulSet()

	require.False(t, statefulSetPodTemplateChanged(current, desired))
	require.False(t, statefulSetNeedsUpdate(current, desired))
}

func bareHTTPProbe(path string, port int) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: path,
				Port: intstr.FromInt(port),
			},
		},
	}
}

func defaultedHTTPProbe(path string, port int) *corev1.Probe {
	probe := bareHTTPProbe(path, port)
	probe.HTTPGet.Scheme = corev1.URISchemeHTTP
	return defaultedProbe(probe)
}

func bareTCPProbe(port int) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Port: intstr.FromInt(port),
			},
		},
	}
}

func defaultedTCPProbe(port int) *corev1.Probe {
	return defaultedProbe(bareTCPProbe(port))
}

func bareExecProbe(command ...string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: command},
		},
	}
}

func defaultedExecProbe(command ...string) *corev1.Probe {
	return defaultedProbe(bareExecProbe(command...))
}

func defaultedProbe(probe *corev1.Probe) *corev1.Probe {
	probe.TimeoutSeconds = 1
	probe.PeriodSeconds = 10
	probe.SuccessThreshold = 1
	probe.FailureThreshold = 3
	return probe
}

func comparableStatefulSet() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mysql",
			Namespace:   "ops",
			Labels:      map[string]string{"app": "mysql"},
			Annotations: map[string]string{"owner": "eruun"},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "mysql",
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "mysql"},
			},
			Replicas: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"app": "mysql"},
					Annotations: map[string]string{"owner": "eruun"},
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:  "init-mysql",
						Image: "busybox:1.36",
					}},
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: "mysql:8.0",
						Ports: []corev1.ContainerPort{{
							ContainerPort: 3306,
						}},
						Env: []corev1.EnvVar{{
							Name:  "MYSQL_DATABASE",
							Value: "app",
						}},
					}},
					NodeSelector:                  map[string]string{"pool": "store"},
					ServiceAccountName:            "mysql",
					AutomountServiceAccountToken:  boolPtr(true),
					RestartPolicy:                 corev1.RestartPolicyAlways,
					DNSPolicy:                     corev1.DNSClusterFirst,
					SchedulerName:                 corev1.DefaultSchedulerName,
					TerminationGracePeriodSeconds: int64Ptr(30),
					EnableServiceLinks:            boolPtr(true),
				},
			},
		},
	}
}

func TestDeployStatefulSetJobCtlWaitTimeoutWithPodAbnormalReturnsFailed(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitTestPod("app-1", "mysql", "CrashLoopBackOff"))

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "mysql",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
}

func TestDeployStatefulSetJobCtlWaitRequiresResourceWaiter(t *testing.T) {
	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:      "mysql",
				Namespace: "default",
				AppID:     "app-1",
				Timeout:   1,
			},
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resource waiter is required")
}

func TestDeployStatefulSetJobCtlWaitUsesBoundedComponentLabel(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitReadyTestPod("app-1", naming.BoundedLabelValue("MySQL_DB")))

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "MySQL_DB",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.NoError(t, err)
}

func TestDeployStatefulSetJobCtlWaitTimeoutWithPendingPodReturnsTimeout(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(newWaitTestPod("app-1", "mysql", "ContainerCreating"))

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "mysql",
				AppID:   "app-1",
				Timeout: 1,
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)
}

func TestDeployStatefulSetJobCtlWaitIgnoresReadyPodWithDifferentImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(withWaitPodImage(newWaitReadyTestPod("app-1", "mysql"), "mysql:5.7"))

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "mysql",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: statefulSetWithImage("mysql:8.0"),
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusTimeout, statusErr.Status)
}

func TestDeployStatefulSetJobCtlWaitUsesExpectedImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	waiter.OnPodAdd(withWaitPodImage(newWaitReadyTestPod("app-1", "mysql"), "mysql:8.0"))

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "mysql",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: statefulSetWithImage("mysql:8.0"),
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
	}

	require.NoError(t, ctl.wait(context.Background()))
}

func TestDeployStatefulSetJobCtlWaitRequiresExpectedTaskAnnotationWithSameImage(t *testing.T) {
	waiter := informer.NewResourceReadyWaiter()
	defer waiter.Close()

	oldReady := withWaitPodImage(newWaitReadyTestPod("app-1", "mysql"), "mysql:8.0")
	oldReady.Name = "mysql-old"
	newAbnormal := withWaitPodAnnotations(withWaitPodImage(newWaitTestPod("app-1", "mysql", "CrashLoopBackOff"), "mysql:8.0"), map[string]string{
		config.AnnotationJobTaskID: "task-sts-template",
	})
	newAbnormal.Name = "mysql-new"
	waiter.OnPodAdd(oldReady)
	waiter.OnPodAdd(newAbnormal)

	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			job: &model.JobTask{
				Name:    "mysql",
				AppID:   "app-1",
				Timeout: 1,
				JobInfo: statefulSetWithImage("mysql:8.0"),
			},
			resourceWaiter: waiter,
		},
		desiredReplicas: 1,
		expectedPodTemplateAnnotations: map[string]string{
			config.AnnotationJobTaskID: "task-sts-template",
		},
	}

	err := ctl.wait(context.Background())
	require.Error(t, err)

	statusErr, ok := ExtractStatusError(err)
	require.True(t, ok)
	require.Equal(t, config.StatusFailed, statusErr.Status)
	require.Contains(t, err.Error(), "CrashLoopBackOff")
}

func statefulSetWithImage(image string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: image,
					}},
				},
			},
		},
	}
}
