package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestCleanupResourcesJobCtlOnlyDefersGenericOwnerJobDeletesForRequiredStatefulSetCleanup(t *testing.T) {
	ownerJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "mysql-owner", Namespace: "default", UID: types.UID("owner-job-uid"), ResourceVersion: "21",
	}}
	tests := []struct {
		name         string
		internalInfo string
		wantDeletes  int
		wantRefs     int
	}{
		{
			name:         "required StatefulSet cleanup defers every generic reference",
			internalInfo: versionUpdateRequireStatefulSetDeletionInternalInfo(),
		},
		{
			name:         "ordinary v1 cleanup keeps generic Job deletion",
			internalInfo: versionUpdateRemoveCleanupInternalInfo(),
			wantDeletes:  1,
			wantRefs:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(ownerJob.DeepCopy())
			ctl := NewCleanupResourcesJobCtl(&model.JobTask{
				JobType: string(config.JobCleanupResources), InternalInfo: tt.internalInfo,
			}, client, &noopStore{}, nil)
			require.NotNil(t, ctl)
			ctl.requiredStatefulSetPodTarget = &requiredStatefulSetPodDeletionTarget{
				ref: cleanupResourceRef{kind: domainspec.ResourceStatefulSet, namespace: ownerJob.Namespace, name: "mysql"},
				ownerJobs: map[string]requiredStatefulSetPodOwnerJobIdentity{
					ownerJob.Name: {name: ownerJob.Name, uid: ownerJob.UID},
				},
			}
			deleted := cleanupResourceSet{seen: make(map[string]struct{})}
			ctl.deleteTrackedResource(context.Background(), &deleted, domainspec.ResourceJob, ownerJob.Namespace, ownerJob.Name, false, func(ctx context.Context) error {
				return ctl.deleteJob(ctx, ownerJob.Namespace, ownerJob.Name)
			})

			require.Empty(t, deleted.errs)
			require.Len(t, deleted.refs, tt.wantRefs)
			require.Equal(t, tt.wantDeletes, countClientActions(client, "delete", "jobs"))
		})
	}
}

func TestCleanupResourcesJobCtlRunDefersLabeledOwnerJobToRequiredStatefulSetPodReconciler(t *testing.T) {
	tests := []struct {
		name         string
		internalInfo func(*testing.T) string
	}{
		{
			name: "v2",
			internalInfo: func(*testing.T) string {
				return versionUpdateRequireStatefulSetDeletionInternalInfo()
			},
		},
		{
			name: "v3",
			internalInfo: func(t *testing.T) string {
				return versionUpdateRequireStatefulSetPVCDeletionInternalInfo(t, "data")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			component := &model.ApplicationComponent{
				ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
			}
			labels := map[string]string{
				config.LabelAppID:         component.AppID,
				config.LabelComponentName: component.Name,
			}
			statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
			statefulSet := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("statefulset-uid"),
					ResourceVersion: "11", Labels: labels,
				},
				Spec: appsv1.StatefulSetSpec{PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
					WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				}},
			}
			ownerJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-owner", Namespace: component.Namespace, UID: types.UID("owner-job-uid"),
				ResourceVersion: "21", Labels: labels,
			}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "mysql-owner-pod", Namespace: component.Namespace, UID: types.UID("owner-pod-uid"),
				ResourceVersion: "31", Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: ownerJob.Name, UID: ownerJob.UID,
				}},
			}}
			client := fake.NewSimpleClientset(statefulSet, ownerJob, pod)
			jobDeleteOptions := make([]metav1.DeleteOptions, 0, 1)
			nonOrphanCascades := 0
			client.Fake.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
				deleteAction := action.(k8stesting.DeleteAction)
				options := deleteAction.GetDeleteOptions()
				jobDeleteOptions = append(jobDeleteOptions, options)
				if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationOrphan {
					nonOrphanCascades++
					require.NoError(t, client.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name))
				}
				return false, nil, nil
			})

			internalInfo := tt.internalInfo(t)
			store := &cleanupComponentStore{
				jobInfo: &model.JobInfo{
					ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
					TaskID: "task-owner-job-" + tt.name, Status: string(config.StatusQueued),
					InternalInfo: internalInfo, ServiceName: component.Name,
				},
			}
			task := &model.JobTask{
				Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
				TaskID: store.jobInfo.TaskID, JobType: string(config.JobCleanupResources), JobInfo: component,
				InternalInfo: internalInfo, Timeout: 4,
			}
			ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
			require.NotNil(t, ctl)

			require.NoError(t, ctl.Run(ctx))
			require.Equal(t, 0, nonOrphanCascades, "the generic Job delete must never run before the dedicated orphan delete")
			require.Len(t, jobDeleteOptions, 1)
			require.Equal(t, 1, countClientActions(client, "delete", "jobs"))
			requireRequiredStatefulSetOwnerJobDeleteIdentity(t, client, ownerJob)
			require.Equal(t, 1, countClientActions(client, "delete", "pods"), "the Pod must survive owner deletion and be removed explicitly")

			checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(store.jobInfo.InternalInfo)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, checkpoint.OwnerJobsCaptured)
			require.True(t, requiredStatefulSetPodCheckpointContainsPod(checkpoint, pod.Name, pod.UID))
			require.Equal(t, []requiredStatefulSetPodOwnerJobCheckpoint{{
				PodNames: []string{pod.Name}, Name: ownerJob.Name, UID: ownerJob.UID,
			}}, checkpoint.OwnerJobs)
		})
	}
}

func TestCleanupResourcesJobCtlRunConvergesDeferredLabeledJobWithoutPods(t *testing.T) {
	fixture := newRequiredStatefulSetLabeledOwnerRunFixture(
		t, versionUpdateRequireStatefulSetDeletionInternalInfo(), "task-owner-job-without-pods",
	)
	require.NoError(t, fixture.client.Tracker().Delete(
		corev1.SchemeGroupVersion.WithResource("pods"), fixture.pod.Namespace, fixture.pod.Name,
	))

	require.NoError(t, fixture.ctl.Run(context.Background()))
	require.Equal(t, 1, countClientActions(fixture.client, "delete", "jobs"))
	require.Equal(t, 0, countClientActions(fixture.client, "delete", "pods"))
	requireRequiredStatefulSetOwnerJobDeleteIdentity(t, fixture.client, fixture.ownerJob)

	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(fixture.store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []requiredStatefulSetPodOwnerJobCheckpoint{{
		Name: fixture.ownerJob.Name, UID: fixture.ownerJob.UID,
	}}, checkpoint.OwnerJobs)
}

func TestCleanupResourcesJobCtlRunCapturesUnlabeledPodOwnedByDeferredLabeledJob(t *testing.T) {
	fixture := newRequiredStatefulSetLabeledOwnerRunFixture(
		t, versionUpdateRequireStatefulSetDeletionInternalInfo(), "task-owner-job-unlabeled-pod",
	)
	livePod, err := fixture.client.CoreV1().Pods(fixture.pod.Namespace).Get(context.Background(), fixture.pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	livePod.Labels = nil
	livePod.ResourceVersion = "32"
	require.NoError(t, fixture.client.Tracker().Update(
		corev1.SchemeGroupVersion.WithResource("pods"), livePod, livePod.Namespace,
	))

	require.NoError(t, fixture.ctl.Run(context.Background()))
	require.Equal(t, 1, countClientActions(fixture.client, "delete", "jobs"))
	require.Equal(t, 1, countClientActions(fixture.client, "delete", "pods"))
	requireRequiredStatefulSetOwnerJobDeleteIdentity(t, fixture.client, fixture.ownerJob)
	requireRequiredStatefulSetPodDeleteIdentity(t, fixture.client, livePod)

	checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(fixture.store.jobInfo.InternalInfo)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, requiredStatefulSetPodCheckpointContainsPod(checkpoint, livePod.Name, livePod.UID))
}

func TestCleanupResourcesJobCtlRunNeverGenericallyDeletesChangedCheckpointedOwnerJob(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*testing.T, *requiredStatefulSetLabeledOwnerRunFixture)
		wantDetail string
	}{
		{
			name: "same-name replacement",
			mutate: func(t *testing.T, fixture *requiredStatefulSetLabeledOwnerRunFixture) {
				require.NoError(t, fixture.client.Tracker().Delete(
					batchv1.SchemeGroupVersion.WithResource("jobs"), fixture.ownerJob.Namespace, fixture.ownerJob.Name,
				))
				replacement := fixture.ownerJob.DeepCopy()
				replacement.UID = types.UID("replacement-owner-job-uid")
				replacement.ResourceVersion = "22"
				require.NoError(t, fixture.client.Tracker().Add(replacement))
			},
			wantDetail: "does not match Pod owner UID",
		},
		{
			name: "late share",
			mutate: func(t *testing.T, fixture *requiredStatefulSetLabeledOwnerRunFixture) {
				object, err := fixture.client.Tracker().Get(
					batchv1.SchemeGroupVersion.WithResource("jobs"), fixture.ownerJob.Namespace, fixture.ownerJob.Name,
				)
				require.NoError(t, err)
				job := object.(*batchv1.Job).DeepCopy()
				job.Labels[config.LabelShareName] = "late-share"
				job.Labels[config.LabelShareStrategy] = string(domainspec.ShareStrategyDefault)
				job.ResourceVersion = "22"
				require.NoError(t, fixture.client.Tracker().Update(
					batchv1.SchemeGroupVersion.WithResource("jobs"), job, job.Namespace,
				))
			},
			wantDetail: "protected by live share labels",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newRequiredStatefulSetLabeledOwnerRunFixture(
				t, versionUpdateRequireStatefulSetDeletionInternalInfo(), "task-owner-job-change",
			)
			mutatedAfterPreflight := false
			fixture.client.Fake.PrependReactor("delete", "statefulsets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				if !mutatedAfterPreflight {
					mutatedAfterPreflight = true
					tt.mutate(t, fixture)
				}
				return false, nil, nil
			})

			err := fixture.ctl.Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantDetail)
			require.True(t, mutatedAfterPreflight)
			require.Equal(t, 0, countClientActions(fixture.client, "delete", "jobs"), "no generic Job reference may bypass the checkpointed identity/share checks")
			require.Equal(t, 0, countClientActions(fixture.client, "delete", "pods"))
			_, err = fixture.client.BatchV1().Jobs(fixture.ownerJob.Namespace).Get(context.Background(), fixture.ownerJob.Name, metav1.GetOptions{})
			require.NoError(t, err)
			_, err = fixture.client.CoreV1().Pods(fixture.pod.Namespace).Get(context.Background(), fixture.pod.Name, metav1.GetOptions{})
			require.NoError(t, err)

			checkpoint, found, err := parseRequiredStatefulSetPodDeletionCheckpoint(fixture.store.jobInfo.InternalInfo)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, checkpoint.OwnerJobsCaptured)
			require.Equal(t, []requiredStatefulSetPodOwnerJobCheckpoint{{
				PodNames: []string{fixture.pod.Name}, Name: fixture.ownerJob.Name, UID: fixture.ownerJob.UID,
			}}, checkpoint.OwnerJobs)
		})
	}
}

type requiredStatefulSetLabeledOwnerRunFixture struct {
	ownerJob *batchv1.Job
	pod      *corev1.Pod
	client   *fake.Clientset
	store    *cleanupComponentStore
	ctl      *CleanupResourcesJobCtl
}

func newRequiredStatefulSetLabeledOwnerRunFixture(
	t *testing.T,
	internalInfo string,
	taskID string,
) *requiredStatefulSetLabeledOwnerRunFixture {
	t.Helper()
	component := &model.ApplicationComponent{
		ID: 1, Name: "mysql", AppID: "app-1", Namespace: "default", ComponentType: config.StoreJob,
	}
	resourceLabels := func() map[string]string {
		return map[string]string{
			config.LabelAppID:         component.AppID,
			config.LabelComponentName: component.Name,
		}
	}
	statefulSetName := buildStoreSeverName(component.Name, component.ResourceNameKey())
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: statefulSetName, Namespace: component.Namespace, UID: types.UID("statefulset-uid"),
			ResourceVersion: "11", Labels: resourceLabels(),
		},
		Spec: appsv1.StatefulSetSpec{PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}},
	}
	ownerJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "mysql-owner", Namespace: component.Namespace, UID: types.UID("owner-job-uid"),
		ResourceVersion: "21", Labels: resourceLabels(),
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "mysql-owner-pod", Namespace: component.Namespace, UID: types.UID("owner-pod-uid"),
		ResourceVersion: "31", Labels: resourceLabels(),
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "batch/v1", Kind: "Job", Name: ownerJob.Name, UID: ownerJob.UID,
		}},
	}}
	client := fake.NewSimpleClientset(statefulSet, ownerJob, pod)
	store := &cleanupComponentStore{jobInfo: &model.JobInfo{
		ID: 10, Type: string(config.JobCleanupResources), AppID: component.AppID,
		TaskID: taskID, Status: string(config.StatusQueued), InternalInfo: internalInfo, ServiceName: component.Name,
	}}
	task := &model.JobTask{
		Name: component.Name, Namespace: component.Namespace, AppID: component.AppID,
		TaskID: taskID, JobType: string(config.JobCleanupResources), JobInfo: component,
		InternalInfo: internalInfo, Timeout: 4,
	}
	ctl := NewCleanupResourcesJobCtl(task, client, store, nil)
	require.NotNil(t, ctl)
	return &requiredStatefulSetLabeledOwnerRunFixture{
		ownerJob: ownerJob, pod: pod, client: client, store: store, ctl: ctl,
	}
}
