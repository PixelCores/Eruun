package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/jobs/artifacts"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var CheckpointGVR = schema.GroupVersionResource{Group: "agents.kruise.io", Version: "v1alpha1", Resource: "checkpoints"}

const (
	checkpointOwnerAnnotation   = "eruun.io/checkpoint-id"
	checkpointPodAnnotation     = "eruun.io/checkpoint-pod-uid"
	checkpointRestoreAnnotation = "checkpoint.alibabacloud.com/restore-from"
)

func checkpointMembers(row *model.JobCheckpoint) ([]CheckpointMember, error) {
	var members []CheckpointMember
	if err := json.Unmarshal(row.Members, &members); err != nil {
		return nil, fmt.Errorf("decode checkpoint members: %w", err)
	}
	if len(members) < 1 || len(members) > 1024 {
		return nil, fmt.Errorf("invalid checkpoint member count")
	}
	return members, nil
}

func ownedCheckpoint(row *model.JobCheckpoint, member CheckpointMember, object *unstructured.Unstructured) bool {
	if object == nil || object.GetUID() == "" || object.GetNamespace() != row.Namespace || object.GetName() != member.SnapshotName || object.GetAnnotations()[checkpointOwnerAnnotation] != row.ID || object.GetAnnotations()[checkpointPodAnnotation] != member.PodUID || object.GetLabels()[config.LabelManagedBy] != "eruun" {
		return false
	}
	if member.SnapshotUID != "" && string(object.GetUID()) != member.SnapshotUID {
		return false
	}
	podName, _, _ := unstructured.NestedString(object.Object, "spec", "podName")
	keepRunning, _, _ := unstructured.NestedBool(object.Object, "spec", "keepRunning")
	contents, _, _ := unstructured.NestedStringSlice(object.Object, "spec", "persistentContents")
	return podName == member.PodName && keepRunning && reflect.DeepEqual(contents, []string{"filesystem"})
}

func checkpointPhase(object *unstructured.Unstructured) (phase, snapshotID string) {
	phase, _, _ = unstructured.NestedString(object.Object, "status", "phase")
	snapshotID, _, _ = unstructured.NestedString(object.Object, "status", "checkpointId")
	return
}

func (s *Service) checkpointSource(ctx context.Context, row *model.JobCheckpoint, member CheckpointMember) (map[string]interface{}, error) {
	sandbox := &model.JobSandbox{ID: member.SandboxID}
	if err := s.Store.Get(ctx, sandbox); err != nil {
		return nil, err
	}
	if sandbox.ExecutionKey != row.ExecutionKey || sandbox.WorkspaceID != row.WorkspaceID || sandbox.RunnerUID != row.RunnerUID || sandbox.SandboxUID != member.SandboxUID || sandbox.PodUID != member.PodUID || sandbox.PodName != member.PodName || sandbox.ReleaseRequested {
		return nil, ErrRunnerConflict
	}
	object, err := s.SandboxClient.Resource(SandboxGVR).Namespace(row.Namespace).Get(ctx, sandbox.SandboxName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if !ownedSandbox(sandbox, object) || string(object.GetUID()) != member.SandboxUID || sandboxReadyPodUID(object) != member.PodUID {
		return nil, ErrRunnerConflict
	}
	pod, err := s.Kube.CoreV1().Pods(row.Namespace).Get(ctx, member.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if !ownedSandboxPod(sandbox, pod, member.PodUID) {
		return nil, ErrRunnerConflict
	}
	value, found, err := unstructured.NestedMap(object.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("checkpoint source specification missing")
	}
	return value, nil
}

func (s *Service) checkpointFailed(ctx context.Context, auth *runnerAuthorization, row *model.JobCheckpoint, reason string) error {
	return s.mutateCheckpoint(ctx, auth, row, func(_ artifacts.Backend, current *model.JobCheckpoint, now time.Time) error {
		current.State, current.Reason, current.ExpiresAt = sandboxFailed, reason, now
		current.LeaseToken, current.LeaseUntil, current.ReconcileAt = "", nil, now
		return nil
	})
}

func (s *Service) advanceCheckpoint(ctx context.Context, auth *runnerAuthorization, row *model.JobCheckpoint) error {
	members, err := checkpointMembers(row)
	if err != nil {
		return err
	}
	allReady := true
	client := s.SandboxClient.Resource(CheckpointGVR).Namespace(row.Namespace)
	// Revalidate every member on each pass, including snapshots already observed
	// as successful. Bound parallel reads so a complete set does not require the
	// sum of all Kubernetes round trips to fit the operation and Runner deadlines.
	// Writes stay serial and retain the claim/lease check before every mutation.
	sources := make([]map[string]interface{}, len(members))
	snapshots := make([]*unstructured.Unstructured, len(members))
	group, observeCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for index, member := range members {
		group.Go(func() error {
			source, err := s.checkpointSource(observeCtx, row, member)
			if k8serrors.IsNotFound(err) || err == ErrRunnerConflict {
				return ErrRunnerConflict
			}
			if err != nil {
				return fmt.Errorf("read checkpoint source: %w", err)
			}
			sources[index] = source
			object, err := client.Get(observeCtx, member.SnapshotName, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("observe checkpoint snapshot: %w", err)
			}
			snapshots[index] = object
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		if err == ErrRunnerConflict {
			return s.checkpointFailed(ctx, auth, row, "source_identity_changed")
		}
		return err
	}
	for index := range members {
		member := &members[index]
		source := sources[index]
		if len(member.SandboxSpec) == 0 {
			member.SandboxSpec, err = json.Marshal(source)
			if err != nil {
				return err
			}
			// Save the exact source template before issuing the first cloud call.
			if err := s.mutateCheckpoint(ctx, auth, row, func(_ artifacts.Backend, current *model.JobCheckpoint, _ time.Time) error {
				current.Members, err = json.Marshal(members)
				return err
			}); err != nil {
				return err
			}
			if row.State != sandboxPending {
				return nil
			}
		} else {
			var saved map[string]interface{}
			if err := json.Unmarshal(member.SandboxSpec, &saved); err != nil {
				return err
			}
			// JSON comparison avoids int64 versus float64 differences after storage.
			current, _ := json.Marshal(source)
			previous, _ := json.Marshal(saved)
			if string(current) != string(previous) {
				return s.checkpointFailed(ctx, auth, row, "source_spec_changed")
			}
		}
		object := snapshots[index]
		if object == nil {
			if member.SnapshotUID != "" {
				return s.checkpointFailed(ctx, auth, row, "snapshot_missing")
			}
			wait, reserveErr := repository.ReserveResourceCreation(ctx, s.Store)
			if reserveErr != nil {
				return reserveErr
			}
			if wait > 0 {
				allReady = false
				continue
			}
			// Recheck the claim immediately before creating an external resource.
			member.CreateRequested = true
			if err := s.mutateCheckpoint(ctx, auth, row, func(_ artifacts.Backend, current *model.JobCheckpoint, _ time.Time) error {
				current.Members, err = json.Marshal(members)
				return err
			}); err != nil {
				return err
			}
			if row.State != sandboxPending {
				return nil
			}
			desired := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "agents.kruise.io/v1alpha1", "kind": "Checkpoint", "spec": map[string]interface{}{"podName": member.PodName, "keepRunning": true, "persistentContents": []interface{}{"filesystem"}}}}
			desired.SetName(member.SnapshotName)
			desired.SetNamespace(row.Namespace)
			desired.SetLabels(map[string]string{config.LabelManagedBy: "eruun", "eruun.io/task-id": row.TaskID})
			desired.SetAnnotations(map[string]string{checkpointOwnerAnnotation: row.ID, checkpointPodAnnotation: member.PodUID})
			object, err = client.Create(ctx, desired, metav1.CreateOptions{})
			if err != nil {
				object, err = client.Get(ctx, member.SnapshotName, metav1.GetOptions{})
			}
		}
		if err != nil {
			return fmt.Errorf("observe checkpoint snapshot: %w", err)
		}
		if !ownedCheckpoint(row, *member, object) || object.GetDeletionTimestamp() != nil {
			return s.checkpointFailed(ctx, auth, row, "snapshot_identity_changed")
		}
		member.SnapshotUID = string(object.GetUID())
		phase, snapshotID := checkpointPhase(object)
		if phase == "Failed" {
			return s.checkpointFailed(ctx, auth, row, "snapshot_failed")
		}
		if phase == "Succeeded" && snapshotID != "" {
			member.SnapshotID = snapshotID
		} else {
			allReady = false
		}
		if err := s.mutateCheckpoint(ctx, auth, row, func(_ artifacts.Backend, current *model.JobCheckpoint, _ time.Time) error {
			current.Members, err = json.Marshal(members)
			return err
		}); err != nil {
			return err
		}
		if row.State != sandboxPending {
			return nil
		}
	}
	return s.mutateCheckpoint(ctx, auth, row, func(tx artifacts.Backend, current *model.JobCheckpoint, now time.Time) error {
		current.LeaseToken, current.LeaseUntil, current.ReconcileAt = "", nil, now.Add(15*time.Second)
		if !allReady {
			return nil
		}
		if err := artifacts.VerifyCheckpoint(ctx, tx, current.WorkspaceID, current.MaterialID, current.ExecutionKey); err != nil {
			return fmt.Errorf("verify checkpoint material: %w", err)
		}
		current.State = sandboxReady
		// Keep at most two complete points, plus an actively referenced older
		// point until its restored execution ends. Expired rows are cleaned by
		// the same maintenance loop as partial snapshots.
		rows, err := tx.List(ctx, &model.JobCheckpoint{WorkspaceID: row.WorkspaceID, ExecutionKey: row.ExecutionKey, State: sandboxReady}, &datastore.ListOptions{Page: 1, PageSize: 5, FilterOptions: datastore.FilterOptions{NotEqual: []datastore.ComparisonQueryOption{{Key: "cleaned", Value: true}}}, SortBy: []datastore.SortOption{{Key: "create_time", Order: datastore.SortOrderDescending}, {Key: "id", Order: datastore.SortOrderDescending}}})
		if err != nil {
			return err
		}
		for index, entity := range rows {
			old := entity.(*model.JobCheckpoint)
			if index >= 1 && old.ReferencedByExecutionKey == "" {
				old.ExpiresAt, old.ReconcileAt = now, now
				if err := putCheckpoint(ctx, tx, old); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// checkpointRestore is used only after the new execution was fenced and the
// old resources isolated. It preserves the source Pod specification byte for
// byte; metadata identifies the new Sandbox/Pod and restore handle.
func (s *Service) checkpointRestore(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox, desired *unstructured.Unstructured) (bool, error) {
	id := auth.evaluation.ResumeCheckpointID
	if id == "" {
		return false, nil
	}
	if !auth.evaluation.RecoveryIsolated {
		return false, bcode.ErrUnauthorized
	}
	point := &model.JobCheckpoint{ID: id}
	if err := s.Store.Get(ctx, point); err != nil {
		return false, err
	}
	now, err := s.Store.CurrentDatabaseTime(ctx)
	if err != nil {
		return false, err
	}
	if point.WorkspaceID != row.WorkspaceID || point.TaskID != row.TaskID || point.ReferencedByExecutionKey != row.ExecutionKey || point.State != sandboxReady || point.Cleaned || !now.Before(point.ExpiresAt) {
		return false, ErrRunnerConflict
	}
	members, err := checkpointMembers(point)
	if err != nil {
		return false, err
	}
	for _, member := range members {
		if member.TrialID != row.TrialID {
			continue
		}
		original := &model.JobSandbox{ID: member.SandboxID}
		if err := s.Store.Get(ctx, original); err != nil {
			return false, err
		}
		if original.WorkspaceID != row.WorkspaceID || original.Image != row.Image || original.StorageMiB != row.StorageMiB {
			return false, ErrRunnerConflict
		}
		snapshot, err := s.SandboxClient.Resource(CheckpointGVR).Namespace(point.Namespace).Get(ctx, member.SnapshotName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get restore snapshot: %w", err)
		}
		phase, id := checkpointPhase(snapshot)
		if !ownedCheckpoint(point, member, snapshot) || phase != "Succeeded" || id == "" || id != member.SnapshotID || snapshot.GetDeletionTimestamp() != nil {
			return false, ErrRunnerConflict
		}
		// Unstructured values must have int64 numeric fields. Decode with the
		// Kubernetes JSON decoder rather than a floating-point intermediate.
		var sourceObject unstructured.Unstructured
		wrapped := append([]byte(`{"apiVersion":"agents.kruise.io/v1alpha1","kind":"Sandbox","spec":`), member.SandboxSpec...)
		wrapped = append(wrapped, '}')
		if err := sourceObject.UnmarshalJSON(wrapped); err != nil {
			return false, err
		}
		source, _, _ := unstructured.NestedMap(sourceObject.Object, "spec")
		image, _, _ := unstructured.NestedSlice(source, "template", "spec", "containers")
		if len(image) != 1 {
			return false, ErrRunnerConflict
		}
		container, ok := image[0].(map[string]interface{})
		if !ok || container["image"] != row.Image {
			return false, ErrRunnerConflict
		}
		shutdown, _, _ := unstructured.NestedString(desired.Object, "spec", "shutdownTime")
		source["shutdownTime"] = shutdown
		if err := unstructured.SetNestedStringMap(source, sandboxLabels(row), "template", "metadata", "labels"); err != nil {
			return false, err
		}
		annotations, _, _ := unstructured.NestedStringMap(source, "template", "metadata", "annotations")
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[checkpointRestoreAnnotation] = id
		if err := unstructured.SetNestedStringMap(source, annotations, "template", "metadata", "annotations"); err != nil {
			return false, err
		}
		desired.Object["spec"] = source
		return true, nil
	}
	return false, nil
}
