package job

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func TestDeployCleanupDeletesCreatedDeployment(t *testing.T) {
	client := fake.NewSimpleClientset()
	jobTask := &model.JobTask{Name: "web", Namespace: "default"}
	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			namespace: jobTask.Namespace,
			job:       jobTask,
			client:    client,
		},
	}

	ctx := WithCleanupTracker(context.Background())
	MarkResourceCreated(ctx, config.ResourceDeployment, "default", "web")

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corePodTemplate("web"),
		},
	}
	if _, err := client.AppsV1().Deployments("default").Create(context.Background(), deploy, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}

	ctl.Clean(ctx)

	if _, err := client.AppsV1().Deployments("default").Get(context.Background(), "web", metav1.GetOptions{}); err == nil {
		t.Fatalf("expected deployment to be deleted during cleanup")
	}
}

func TestDeployCleanupDeletesObservedSharedDeploymentWhenPodAbnormal(t *testing.T) {
	deployName := buildWebServiceName("web", "app-1")
	resourceLabels := map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "web",
		config.LabelComponentID:   "7",
		config.LabelShareName:     "shared-web",
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				Template: corePodTemplate("web"),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-0",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "web",
					config.LabelComponentID:   "7",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "web",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
					},
				},
			},
		},
	)
	jobTask := &model.JobTask{
		Name:      "web",
		AppID:     "app-1",
		Namespace: "default",
		JobInfo: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
		},
	}
	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			namespace: jobTask.Namespace,
			job:       jobTask,
			client:    client,
		},
	}

	ctx := WithCleanupTracker(context.Background())
	markResourceObserved(ctx, config.ResourceDeployment, "default", deployName)

	ctl.Clean(ctx)

	if _, err := client.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected observed shared deployment to be deleted when pod is abnormal")
	}
}

func TestDeployCleanupKeepsObservedSharedDeploymentWithoutAbnormalPod(t *testing.T) {
	deployName := buildWebServiceName("web", "app-1")
	resourceLabels := map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "web",
		config.LabelComponentID:   "7",
		config.LabelShareName:     "shared-web",
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				Template: corePodTemplate("web"),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-0",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "web",
					config.LabelComponentID:   "7",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "web",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"},
						},
					},
				},
			},
		},
	)
	jobTask := &model.JobTask{
		Name:      "web",
		AppID:     "app-1",
		Namespace: "default",
		JobInfo: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deployName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
		},
	}
	ctl := &DeployJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			namespace: jobTask.Namespace,
			job:       jobTask,
			client:    client,
		},
	}

	ctx := WithCleanupTracker(context.Background())
	markResourceObserved(ctx, config.ResourceDeployment, "default", deployName)

	ctl.Clean(ctx)

	if _, err := client.AppsV1().Deployments("default").Get(context.Background(), deployName, metav1.GetOptions{}); err != nil {
		t.Fatalf("expected observed shared deployment to be kept, got err=%v", err)
	}
}

func TestStatefulSetCleanupDeletesObservedSharedStatefulSetWhenPodAbnormal(t *testing.T) {
	statefulSetName := buildStoreSeverName("db", "app-1")
	resourceLabels := map[string]string{
		config.LabelAppID:         "app-1",
		config.LabelComponentName: "db",
		config.LabelComponentID:   "8",
		config.LabelShareName:     "shared-db",
		config.LabelShareStrategy: string(config.ShareStrategyDefault),
	}
	client := fake.NewSimpleClientset(
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}},
				Template: corePodTemplate("db"),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "db-0",
				Namespace: "default",
				Labels: map[string]string{
					config.LabelAppID:         "app-1",
					config.LabelComponentName: "db",
					config.LabelComponentID:   "8",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "db",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
					},
				},
			},
		},
	)
	jobTask := &model.JobTask{
		Name:      "db",
		AppID:     "app-1",
		Namespace: "default",
		JobInfo: &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: "default",
				Labels:    resourceLabels,
			},
		},
	}
	ctl := &DeployStatefulSetJobCtl{
		deployNamespacedResourceJobBase: deployNamespacedResourceJobBase{
			namespace: "default",
			job:       jobTask,
			client:    client,
		},
	}

	ctx := WithCleanupTracker(context.Background())
	markResourceObserved(ctx, config.ResourceStatefulSet, "default", statefulSetName)

	ctl.Clean(ctx)

	if _, err := client.AppsV1().StatefulSets("default").Get(context.Background(), statefulSetName, metav1.GetOptions{}); err == nil {
		t.Fatalf("expected observed shared statefulset to be deleted when pod is abnormal")
	}
}

func corePodTemplate(name string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: name, Image: "nginx", Ports: []corev1.ContainerPort{{ContainerPort: 80}}}}},
	}
}
