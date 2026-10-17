package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestCleanupRunVolumesRespectsLifecycleAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        config.Status
		jobStatus     config.Status
		cancelPending bool
		liveLease     bool
		missingPVC    bool
		missingRun    bool
		missingLabel  bool
		wrongName     bool
		wrongNS       bool
		wrongType     bool
		application   bool
		noVolume      bool
		wantDelete    bool
		wantError     string
	}{
		{name: "completed", status: config.StatusCompleted, wantDelete: true},
		{name: "passed", status: config.StatusPassed, wantDelete: true},
		{name: "skipped", status: config.StatusSkipped, wantDelete: true},
		{name: "failed", status: config.StatusFailed, wantDelete: true},
		{name: "timeout", status: config.StatusTimeout, wantDelete: true},
		{name: "rejected", status: config.StatusReject, wantDelete: true},
		{name: "not run", status: config.StatusNotRun, wantDelete: true},
		{name: "missing PVC is complete", status: config.StatusCompleted, missingPVC: true},
		{name: "unknown run is retained", status: config.StatusCompleted, missingRun: true},
		{name: "unlabelled PVC is retained", status: config.StatusCompleted, missingLabel: true},
		{name: "waiting", status: config.StatusWaiting},
		{name: "queued", status: config.StatusQueued},
		{name: "running", status: config.StatusRunning},
		{name: "cancelled before execution", status: config.StatusCancelled, wantDelete: true},
		{name: "cancelled owner still stopping", status: config.StatusCancelled, liveLease: true},
		{name: "cancelled child still running", status: config.StatusCancelled, jobStatus: config.StatusRunning},
		{name: "cancelled cleanup pending", status: config.StatusCancelled, jobStatus: config.StatusCancelled, cancelPending: true},
		{name: "cancelled cleanup complete", status: config.StatusCancelled, jobStatus: config.StatusCancelled, wantDelete: true},
		{name: "name mismatch", status: config.StatusCompleted, wrongName: true, wantError: "ownership"},
		{name: "namespace mismatch", status: config.StatusCompleted, wrongNS: true, wantError: "workspace namespace"},
		{name: "workflow run", status: config.StatusCompleted, wrongType: true, wantError: "ownership"},
		{name: "application run", status: config.StatusCompleted, application: true, wantError: "ownership"},
		{name: "volume not declared", status: config.StatusCompleted, noVolume: true, wantError: "does not declare volume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			request := commandRequest()
			if !tc.noVolume {
				request.Volume = &spec.JobVolume{Size: "1Gi", StorageClass: "work-storage"}
			}
			service.Config.Accounts.Workspace.StorageClasses = []string{"work-storage"}
			snapshot, err := json.Marshal(request.JobSpec)
			require.NoError(t, err)
			task := &model.WorkflowQueue{TaskID: "run-1", WorkspaceID: "space", Type: config.WorkflowTaskTypeJob, Status: tc.status, JobSpec: string(snapshot)}
			if tc.liveLease {
				task.RunToken = "active-owner"
			}
			if tc.application {
				task.AppID = "application"
			}
			if tc.wrongType {
				task.Type = config.WorkflowTaskTypeWorkflow
			}
			if !tc.missingRun {
				require.NoError(t, raw.Add(ctx, task))
			}
			if tc.jobStatus != "" {
				record := &model.JobInfo{TaskID: task.TaskID, WorkspaceID: "space", Type: string(config.JobCommand), Status: string(tc.jobStatus)}
				if tc.cancelPending {
					record.SchedulingReason = "parent workflow cancelled"
					require.True(t, workflowjob.IsCancelledJobCleanupPending(record))
				}
				require.NoError(t, raw.Add(ctx, record))
			}
			client := service.Kube.(*fake.Clientset)
			name := spec.JobVolumeName(task.TaskID)
			claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "space-ns", UID: "volume-uid", ResourceVersion: "7",
				Labels: map[string]string{spec.JobTaskLabel: naming.NormalizeLabelValue(task.TaskID)}}}
			if tc.missingLabel {
				claim.Labels = nil
			}
			if tc.wrongName {
				claim.Name = "existing-user-pvc"
			}
			if tc.wrongNS {
				claim.Namespace = "another-space"
			}
			if !tc.missingPVC {
				require.NoError(t, client.Tracker().Add(claim))
			}
			err = service.cleanupRunVolumes(context.Background())
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
			deleted := false
			for _, action := range client.Actions() {
				require.NotEqual(t, "get", action.GetVerb(), "cleanup must use the listed PVC without a per-run GET")
				if action.GetVerb() != "delete" {
					require.Equal(t, "list", action.GetVerb())
					options := action.(k8stesting.ListActionImpl).GetListOptions()
					require.Empty(t, action.GetNamespace())
					require.Equal(t, spec.JobTaskLabel, options.LabelSelector)
					require.EqualValues(t, 100, options.Limit)
					continue
				}
				deletion := action.(k8stesting.DeleteAction)
				require.Equal(t, name, deletion.GetName())
				require.Equal(t, claim.Namespace, deletion.GetNamespace())
				require.NotNil(t, deletion.GetDeleteOptions().Preconditions)
				require.Equal(t, claim.UID, *deletion.GetDeleteOptions().Preconditions.UID)
				require.Equal(t, claim.ResourceVersion, *deletion.GetDeleteOptions().Preconditions.ResourceVersion)
				deleted = true
			}
			require.Equal(t, tc.wantDelete, deleted)
			_, err = client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if tc.wantDelete || tc.missingPVC {
				require.True(t, k8serrors.IsNotFound(err))
			} else {
				require.NoError(t, err, "unowned or unfinished run volumes must remain")
			}
			if tc.wantDelete {
				client.ClearActions()
				require.NoError(t, service.cleanupRunVolumes(context.Background()), "repeated cleanup is complete when PVC is absent")
				require.Len(t, client.Actions(), 1)
				require.Equal(t, "list", client.Actions()[0].GetVerb())
			}
		})
	}
}

func TestCleanupRunVolumesRetriesDeleteErrorsAndAcceptsNotFound(t *testing.T) {
	for _, notFound := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "not found"}[notFound], func(t *testing.T) {
			service, raw, ctx := testJobService(t)
			request := commandRequest()
			request.Volume = &spec.JobVolume{Size: "1Gi", StorageClass: "work-storage"}
			service.Config.Accounts.Workspace.StorageClasses = []string{"work-storage"}
			accepted, err := service.Submit(ctx, request)
			require.NoError(t, err)
			task := &model.WorkflowQueue{TaskID: accepted.TaskID}
			require.NoError(t, raw.Get(ctx, task))
			task.Status = config.StatusCompleted
			require.NoError(t, raw.Put(ctx, task))
			client := service.Kube.(*fake.Clientset)
			name := spec.JobVolumeName(task.TaskID)
			require.NoError(t, client.Tracker().Add(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "space-ns", UID: "volume-uid",
				Labels: map[string]string{spec.JobTaskLabel: naming.NormalizeLabelValue(task.TaskID)}}}))
			attempts := 0
			client.PrependReactor("delete", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
				attempts++
				if attempts > 1 {
					return false, nil, nil
				}
				if notFound {
					return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, name)
				}
				return true, nil, errors.New("temporary Kubernetes failure")
			})
			err = service.cleanupRunVolumes(context.Background())
			if notFound {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "temporary Kubernetes failure")
			}
			require.NoError(t, service.cleanupRunVolumes(context.Background()))
			require.Equal(t, 2, attempts)
			_, err = client.CoreV1().PersistentVolumeClaims("space-ns").Get(ctx, name, metav1.GetOptions{})
			require.True(t, k8serrors.IsNotFound(err))
		})
	}
}

func TestCleanupRunVolumesFollowsContinue(t *testing.T) {
	service, raw, ctx := testJobService(t)
	request := commandRequest()
	request.Volume = &spec.JobVolume{Size: "1Gi"}
	snapshot, err := json.Marshal(request.JobSpec)
	require.NoError(t, err)
	client := service.Kube.(*fake.Clientset)
	claims := make([]corev1.PersistentVolumeClaim, 3)
	for i := range claims {
		task := &model.WorkflowQueue{TaskID: fmt.Sprintf("run-%d", i), WorkspaceID: "space", Type: config.WorkflowTaskTypeJob,
			Status: config.StatusCompleted, JobSpec: string(snapshot)}
		require.NoError(t, raw.Add(ctx, task))
		claims[i] = corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: spec.JobVolumeName(task.TaskID), Namespace: "space-ns",
			Labels: map[string]string{spec.JobTaskLabel: naming.NormalizeLabelValue(task.TaskID)}}}
		require.NoError(t, client.Tracker().Add(&claims[i]))
	}
	pages := 0
	client.PrependReactor("list", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		require.Less(t, pages, len(claims))
		options := action.(k8stesting.ListActionImpl).GetListOptions()
		require.Empty(t, action.GetNamespace())
		require.Equal(t, spec.JobTaskLabel, options.LabelSelector)
		require.EqualValues(t, 100, options.Limit)
		if pages == 0 {
			require.Empty(t, options.Continue)
		} else {
			require.Equal(t, fmt.Sprintf("page-%d", pages), options.Continue)
		}
		// Short pages must still follow the server's opaque Continue token.
		page := &corev1.PersistentVolumeClaimList{Items: []corev1.PersistentVolumeClaim{claims[pages]}}
		pages++
		if pages < len(claims) {
			page.Continue = fmt.Sprintf("page-%d", pages)
		}
		return true, page, nil
	})
	require.NoError(t, service.cleanupRunVolumes(context.Background()))
	require.Equal(t, len(claims), pages)
	for _, claim := range claims {
		_, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		require.True(t, k8serrors.IsNotFound(err))
	}
}
