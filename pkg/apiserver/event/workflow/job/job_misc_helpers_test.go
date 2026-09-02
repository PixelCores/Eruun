package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

func TestNormalizePodSpecForCompareIncludesVolumeMountSubPathFields(t *testing.T) {
	base := corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  "api",
			Image: "nginx:1.25",
			VolumeMounts: []corev1.VolumeMount{{
				Name:        "logs",
				MountPath:   "/app/log",
				SubPathExpr: "$(POD_IP)/logs",
			}},
		}},
	}
	same := base.DeepCopy()
	require.True(t, apiequality.Semantic.DeepEqual(normalizePodSpecForCompare(base), normalizePodSpecForCompare(*same)))

	changedExpr := base.DeepCopy()
	changedExpr.Containers[0].VolumeMounts[0].SubPathExpr = "$(INSTANCE_ID)/logs"
	require.False(t, apiequality.Semantic.DeepEqual(normalizePodSpecForCompare(base), normalizePodSpecForCompare(*changedExpr)))

	changedSubPath := base.DeepCopy()
	changedSubPath.Containers[0].VolumeMounts[0].SubPathExpr = ""
	changedSubPath.Containers[0].VolumeMounts[0].SubPath = "fixed/logs"
	require.False(t, apiequality.Semantic.DeepEqual(normalizePodSpecForCompare(base), normalizePodSpecForCompare(*changedSubPath)))

	changedReadOnly := base.DeepCopy()
	changedReadOnly.Containers[0].VolumeMounts[0].ReadOnly = true
	require.False(t, apiequality.Semantic.DeepEqual(normalizePodSpecForCompare(base), normalizePodSpecForCompare(*changedReadOnly)))
}

func TestDeployPVCJobCtlCreatesMissingPVC(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	client := fake.NewSimpleClientset()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-mysql",
			Namespace: "ops",
			Labels:    map[string]string{"app": "mysql"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	job := &model.JobTask{
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		AppID:     "app-1",
		JobType:   string(config.JobDeployPVC),
		JobInfo:   pvc.DeepCopy(),
	}
	ctl := NewDeployPVCJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))

	require.NoError(t, ctl.run(ctx))

	created, err := client.CoreV1().PersistentVolumeClaims("ops").Get(ctx, "data-mysql", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, resource.MustParse("1Gi"), created.Spec.Resources.Requests[corev1.ResourceStorage])
	if creates := countClientActions(client, "create", "persistentvolumeclaims"); creates != 1 {
		t.Fatalf("expected one pvc create, got %d", creates)
	}
}

func TestDeployPVCJobCtlSkipsExistingPVCSpecUpdate(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	client := fake.NewSimpleClientset()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-mysql",
			Namespace: "ops",
			Labels:    map[string]string{"app": "mysql"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("ops").Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to seed pvc: %v", err)
	}
	desired := pvc.DeepCopy()
	desired.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
	desired.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")

	job := &model.JobTask{
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		AppID:     "app-1",
		JobType:   string(config.JobDeployPVC),
		JobInfo:   desired,
	}
	ctl := NewDeployPVCJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	if err := ctl.run(ctx); err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if updates := countClientActions(client, "update", "persistentvolumeclaims"); updates != 0 {
		t.Fatalf("expected no pvc update, got %d", updates)
	}
	kept, err := client.CoreV1().PersistentVolumeClaims("ops").Get(ctx, "data-mysql", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, kept.Spec.AccessModes)
	require.Equal(t, resource.MustParse("1Gi"), kept.Spec.Resources.Requests[corev1.ResourceStorage])
}

func TestDeployPVCJobCtlCleanPreservesTrackedPVC(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	client := fake.NewSimpleClientset()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-mysql",
			Namespace: "ops",
		},
	}
	_, err := client.CoreV1().PersistentVolumeClaims("ops").Create(ctx, pvc, metav1.CreateOptions{})
	require.NoError(t, err)
	MarkResourceCreated(ctx, domainspec.ResourcePVC, "ops", "data-mysql")

	job := &model.JobTask{
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		AppID:     "app-1",
		JobType:   string(config.JobDeployPVC),
		JobInfo:   pvc.DeepCopy(),
	}
	ctl := NewDeployPVCJobCtl(job, client, &noopStore{}, func() {}, locker.NewNoopLocker(shareLockerPrefix))
	require.NotNil(t, ctl)

	ctl.Clean(ctx)

	_, err = client.CoreV1().PersistentVolumeClaims("ops").Get(ctx, "data-mysql", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, 0, countClientActions(client, "delete", "persistentvolumeclaims"))
}

func TestIngressReady(t *testing.T) {
	require.False(t, ingressReady(nil))
	require.True(t, ingressReady(&networkingv1.Ingress{}))
}

func TestCleanupClusterResources(t *testing.T) {
	ctx := WithCleanupTracker(context.Background())
	MarkResourceCreated(ctx, domainspec.ResourceClusterRole, "", "role-a")
	markResourceObserved(ctx, domainspec.ResourceClusterRole, "", "role-b")

	var deleted []string
	cleanupClusterResources(ctx, domainspec.ResourceClusterRole, time.Second, "clusterrole", func(_ context.Context, name string) error {
		deleted = append(deleted, name)
		return nil
	}, nil)
	require.Equal(t, []string{"role-a"}, deleted)

	cleanupClusterResources(ctx, domainspec.ResourceClusterRole, time.Second, "clusterrole", func(context.Context, string) error {
		return errors.New("not found")
	}, func(err error) bool {
		return err != nil && err.Error() == "not found"
	})
}
