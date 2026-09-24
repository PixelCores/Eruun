package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var SandboxGVR = schema.GroupVersionResource{Group: "agents.kruise.io", Version: "v1alpha1", Resource: "sandboxes"}

const (
	sandboxIDLabel          = "eruun.io/sandbox-id"
	sandboxDigestAnnotation = "eruun.io/sandbox-request-digest"
	sandboxRunnerAnnotation = "eruun.io/sandbox-runner-uid"
)

func sandboxLabels(row *model.JobSandbox) map[string]string {
	return map[string]string{config.LabelManagedBy: "eruun", "eruun.io/task-id": row.TaskID, sandboxIDLabel: row.ID[:48],
		"alibabacloud.com/acs": "true", "alibabacloud.com/compute-class": "agent-sandbox"}
}

func buildSandbox(row *model.JobSandbox, auth *runnerAuthorization, now time.Time) (*unstructured.Unstructured, error) {
	resources := auth.evaluation.Traits.Evaluation.SandboxResources
	if resources == nil {
		return nil, fmt.Errorf("sandbox resource snapshot is missing")
	}
	requests, limits := corev1.ResourceList{}, corev1.ResourceList{}
	for _, item := range []struct {
		name   corev1.ResourceName
		value  string
		target corev1.ResourceList
	}{
		{corev1.ResourceCPU, resources.CPU, requests}, {corev1.ResourceMemory, resources.Memory, requests},
		{corev1.ResourceCPU, resources.CPULimit, limits}, {corev1.ResourceMemory, resources.MemoryLimit, limits},
	} {
		quantity, err := resource.ParseQuantity(item.value)
		if err != nil || quantity.Sign() <= 0 {
			return nil, fmt.Errorf("invalid sandbox resource snapshot")
		}
		item.target[item.name] = quantity
	}
	storage := *resource.NewQuantity(row.StorageMiB*1024*1024, resource.BinarySI)
	requests[corev1.ResourceEphemeralStorage], limits[corev1.ResourceEphemeralStorage] = storage, storage
	deadline := row.Deadline.Add(sandboxRetention)
	seconds := int64(deadline.Sub(now).Seconds())
	if seconds < 1 {
		return nil, fmt.Errorf("sandbox deadline elapsed")
	}
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: sandboxLabels(row)}, Spec: corev1.PodSpec{
		ServiceAccountName: "default", AutomountServiceAccountToken: ptr.To(false), RestartPolicy: corev1.RestartPolicyNever,
		ActiveDeadlineSeconds: &seconds,
		SecurityContext:       &corev1.PodSecurityContext{RunAsUser: ptr.To(int64(1000)), RunAsGroup: ptr.To(int64(1000)), FSGroup: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		Containers: []corev1.Container{{Name: "main", Image: row.Image, Command: []string{"sleep", "infinity"},
			Resources:       corev1.ResourceRequirements{Requests: requests, Limits: limits},
			SecurityContext: &corev1.SecurityContext{RunAsUser: ptr.To(int64(1000)), RunAsGroup: ptr.To(int64(1000)), RunAsNonRoot: ptr.To(true), Privileged: ptr.To(false), AllowPrivilegeEscalation: ptr.To(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		}},
	}}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&template)
	if err != nil {
		return nil, fmt.Errorf("encode sandbox template: %w", err)
	}
	object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "agents.kruise.io/v1alpha1", "kind": "Sandbox",
		"spec": map[string]interface{}{"template": encoded, "persistentContents": []interface{}{"filesystem"},
			"runtimes": []interface{}{map[string]interface{}{"name": "agent-runtime"}}, "shutdownTime": deadline.UTC().Format(time.RFC3339)}}}
	object.SetName(row.SandboxName)
	object.SetNamespace(row.Namespace)
	object.SetLabels(sandboxLabels(row))
	object.SetAnnotations(map[string]string{sandboxDigestAnnotation: row.RequestDigest, sandboxRunnerAnnotation: row.RunnerUID})
	// There is deliberately no Runner ownerReference: failed collection retains
	// the Sandbox independently of Runner deletion, bounded by shutdownTime.
	return object, nil
}

func ownedSandbox(row *model.JobSandbox, object *unstructured.Unstructured) bool {
	return object != nil && object.GetName() == row.SandboxName && object.GetNamespace() == row.Namespace &&
		object.GetUID() != "" && object.GetLabels()[sandboxIDLabel] == row.ID[:48] && object.GetLabels()["eruun.io/task-id"] == row.TaskID &&
		object.GetLabels()[config.LabelManagedBy] == "eruun" && object.GetAnnotations()[sandboxDigestAnnotation] == row.RequestDigest && object.GetAnnotations()[sandboxRunnerAnnotation] == row.RunnerUID
}

func sandboxReadyPodUID(object *unstructured.Unstructured) string {
	if object.GetDeletionTimestamp() != nil {
		return ""
	}
	observed, ok, _ := unstructured.NestedInt64(object.Object, "status", "observedGeneration")
	if !ok || observed < object.GetGeneration() {
		return ""
	}
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	ready := false
	for _, value := range conditions {
		condition, ok := value.(map[string]interface{})
		if ok && condition["type"] == "Ready" {
			ready = condition["status"] == "True"
		}
	}
	if !ready {
		return ""
	}
	uid, _, _ := unstructured.NestedString(object.Object, "status", "podInfo", "podUID")
	return uid
}

func ownedSandboxPod(row *model.JobSandbox, pod *corev1.Pod, uid string) bool {
	if pod == nil || uid == "" || string(pod.UID) != uid || pod.Namespace != row.Namespace || pod.DeletionTimestamp != nil || pod.Labels[sandboxIDLabel] != row.ID[:48] || pod.Labels["eruun.io/task-id"] != row.TaskID {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.Kind != "Sandbox" || owner.APIVersion != "agents.kruise.io/v1alpha1" || owner.Name != row.SandboxName || string(owner.UID) != row.SandboxUID {
		return false
	}
	if pod.Spec.ServiceAccountName != "default" || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		return false
	}
	ready := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	if pod.Status.Phase != corev1.PodRunning || !ready {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "main" && container.Image == row.Image {
			return true
		}
	}
	return false
}

func (s *Service) finishSandbox(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox, state, reason string) error {
	return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
		if !current.ReleaseRequested {
			current.State, current.Reason = state, reason
		}
		current.LeaseToken, current.LeaseUntil = "", nil
		current.ReconcileAt = now.Add(15 * time.Second)
		return nil
	})
}

func (s *Service) lostSandbox(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox, reason string) error {
	return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
		current.State, current.Reason, current.SlotReserved, current.StartReserved = sandboxFailed, reason, false, false
		current.LeaseToken, current.LeaseUntil = "", nil
		current.ReconcileAt = now.Add(sandboxRetention)
		return nil
	})
}

func (s *Service) retainFailedSandbox(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox, reason string) error {
	return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
		stopSandbox(current, now, reason)
		current.State = sandboxFailed
		current.LeaseToken, current.LeaseUntil = "", nil
		current.ReconcileAt = now
		return nil
	})
}

func (s *Service) advanceSandbox(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox) error {
	if row.ReleaseRequested {
		return s.cleanupSandbox(ctx, auth, row)
	}
	object, err := s.SandboxObserver.Sandbox(row.Namespace, row.SandboxName)
	if errors.Is(err, informer.ErrSandboxObservationUnavailable) {
		return bcode.ErrServiceUnavailable
	}
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("observe sandbox: %w", err)
	}
	client := s.SandboxClient.Resource(SandboxGVR).Namespace(row.Namespace)
	// A cached absence, changed UID, initial creation, or readiness candidate
	// always needs an authoritative read. Ordinary Pending remains cache-only.
	phase := ""
	if object != nil {
		phase, _, _ = unstructured.NestedString(object.Object, "status", "phase")
	}
	if row.SandboxUID == "" || err != nil || object.GetUID() != types.UID(row.SandboxUID) || sandboxReadyPodUID(object) != "" || phase == "Failed" || phase == "Succeeded" {
		object, err = client.Get(ctx, row.SandboxName, metav1.GetOptions{})
	}
	if k8serrors.IsNotFound(err) {
		if row.SandboxUID != "" {
			return s.lostSandbox(ctx, auth, row, "sandbox_missing")
		}
		starting, err := repository.ReserveSandboxStart(ctx, s.Store, row.ID)
		if err != nil {
			return err
		}
		if !starting {
			return s.finishSandbox(ctx, auth, row, sandboxPending, "starting_capacity")
		}
		wait, err := repository.ReserveResourceCreation(ctx, s.Store)
		if err != nil {
			return err
		}
		if wait > 0 {
			return s.finishSandbox(ctx, auth, row, sandboxPending, "creation_rate_limited")
		}
		var now time.Time
		if err = s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, at time.Time) error {
			now = at
			if !current.ReleaseRequested {
				current.CreateAttempts++
			}
			return nil
		}); err != nil {
			return err
		}
		if row.ReleaseRequested {
			return s.cleanupSandbox(ctx, auth, row)
		}
		desired, err := buildSandbox(row, auth, now)
		if err != nil {
			return err
		}
		if _, err := s.checkpointRestore(ctx, auth, row, desired); err != nil {
			return err
		}
		object, err = client.Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			// Includes an uncertain create response: never use a guessed UID.
			object, err = client.Get(ctx, row.SandboxName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("confirm sandbox creation: %w", err)
			}
		}
	} else if err != nil {
		return fmt.Errorf("get sandbox: %w", err)
	}
	if !ownedSandbox(row, object) || (row.SandboxUID != "" && row.SandboxUID != string(object.GetUID())) {
		return s.lostSandbox(ctx, auth, row, "sandbox_identity_changed")
	}
	if row.SandboxUID == "" {
		if err := s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, _ time.Time) error {
			if current.SandboxUID != "" && current.SandboxUID != string(object.GetUID()) {
				return ErrRunnerConflict
			}
			current.SandboxUID = string(object.GetUID())
			return nil
		}); err != nil {
			return err
		}
	}
	if row.ReleaseRequested {
		return s.cleanupSandbox(ctx, auth, row)
	}
	phase, _, _ = unstructured.NestedString(object.Object, "status", "phase")
	if phase == "Failed" || phase == "Succeeded" {
		return s.retainFailedSandbox(ctx, auth, row, "sandbox_terminated")
	}
	uid := sandboxReadyPodUID(object)
	if uid == "" {
		return s.finishSandbox(ctx, auth, row, sandboxPending, "sandbox_pending")
	}
	pods, err := s.SandboxObserver.SandboxPods(row.Namespace, row.ID[:48])
	if errors.Is(err, informer.ErrSandboxObservationUnavailable) {
		return bcode.ErrServiceUnavailable
	}
	if err != nil {
		return fmt.Errorf("observe sandbox Pod: %w", err)
	}
	if len(pods) > 1 {
		return s.retainFailedSandbox(ctx, auth, row, "pod_identity_ambiguous")
	}
	if len(pods) == 0 {
		return s.finishSandbox(ctx, auth, row, sandboxPending, "pod_identity_unconfirmed")
	}
	pod, err := s.Kube.CoreV1().Pods(row.Namespace).Get(ctx, pods[0].Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		if row.PodUID != "" && row.PodName == pods[0].Name {
			return s.retainFailedSandbox(ctx, auth, row, "pod_missing")
		}
		return s.finishSandbox(ctx, auth, row, sandboxPending, "pod_identity_unconfirmed")
	}
	if err != nil {
		return fmt.Errorf("confirm sandbox Pod: %w", err)
	}
	if row.PodUID != "" && (row.PodUID != string(pod.UID) || row.PodName != pod.Name) {
		return s.retainFailedSandbox(ctx, auth, row, "pod_identity_changed")
	}
	if !ownedSandboxPod(row, pod, uid) {
		return s.finishSandbox(ctx, auth, row, sandboxPending, "pod_identity_unconfirmed")
	}
	if row.PodUID != "" && row.PodUID != uid {
		return s.retainFailedSandbox(ctx, auth, row, "pod_identity_changed")
	}
	return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
		if current.PodUID != "" && current.PodUID != uid {
			return ErrRunnerConflict
		}
		current.PodName, current.PodUID = pod.Name, uid
		if !current.ReleaseRequested {
			current.State, current.Reason, current.StartReserved = sandboxReady, "", false
		}
		current.LeaseToken, current.LeaseUntil = "", nil
		current.ReconcileAt = now.Add(15 * time.Second)
		return nil
	})
}

func (s *Service) cleanupSandbox(ctx context.Context, auth *runnerAuthorization, row *model.JobSandbox) error {
	client := s.SandboxClient.Resource(SandboxGVR).Namespace(row.Namespace)
	object, err := client.Get(ctx, row.SandboxName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
			// A timed-out create may still arrive. Until its absolute shutdown
			// bound, keep the intent and slot so maintenance can find the orphan.
			if current.SandboxUID != "" || current.CreateAttempts == 0 || !now.Before(current.Deadline.Add(sandboxRetention)) {
				current.State, current.SlotReserved, current.StartReserved, current.Reason = sandboxReleased, false, false, ""
			} else {
				current.State, current.Reason = sandboxPending, "creation_outcome_unknown"
			}
			current.LeaseToken, current.LeaseUntil = "", nil
			current.ReconcileAt = now.Add(15 * time.Second)
			return nil
		})
	}
	if err != nil {
		return fmt.Errorf("get sandbox for release: %w", err)
	}
	if !ownedSandbox(row, object) || (row.SandboxUID != "" && row.SandboxUID != string(object.GetUID())) {
		return s.lostSandbox(ctx, auth, row, "sandbox_identity_changed")
	}
	if row.SandboxUID == "" {
		if err := s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, _ time.Time) error {
			current.SandboxUID = string(object.GetUID())
			return nil
		}); err != nil {
			return err
		}
	}
	now, err := s.Store.(datastore.DatabaseClock).CurrentDatabaseTime(ctx)
	if err != nil {
		return err
	}
	if row.RetainUntil != nil && now.Before(*row.RetainUntil) {
		return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
			// Serialize the external extension with recovery's durable isolation
			// marker. A stale lease cannot patch a newer resourceVersion and then
			// discover only afterwards that its database mutation was fenced.
			if current.Reason == "recovery_isolation" {
				return ErrRunnerConflict
			}
			if current.RetainUntil == nil || !now.Before(*current.RetainUntil) {
				current.State, current.Reason = sandboxPending, "release_pending"
				current.LeaseToken, current.LeaseUntil, current.ReconcileAt = "", nil, now
				return nil
			}
			live, err := client.Get(ctx, current.SandboxName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get retained sandbox: %w", err)
			}
			if !ownedSandbox(current, live) || string(live.GetUID()) != current.SandboxUID {
				return ErrRunnerConflict
			}
			shutdown, _, _ := unstructured.NestedString(live.Object, "spec", "shutdownTime")
			existing, parseErr := time.Parse(time.RFC3339, shutdown)
			if parseErr != nil || !existing.Equal(current.RetainUntil.Truncate(time.Second)) {
				patch, _ := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"uid": current.SandboxUID, "resourceVersion": live.GetResourceVersion()}, "spec": map[string]interface{}{"shutdownTime": current.RetainUntil.UTC().Format(time.RFC3339)}})
				if _, err := client.Patch(ctx, current.SandboxName, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
					return fmt.Errorf("retain sandbox: %w", err)
				}
			}
			current.State, current.StartReserved = sandboxRetained, false
			if current.Reason == "creation_outcome_unknown" {
				current.Reason = "collection_incomplete"
			}
			current.LeaseToken, current.LeaseUntil = "", nil
			current.ReconcileAt = now.Add(15 * time.Second)
			return nil
		})
	}
	// Snapshots continue after CR deletion once running. Preserve the source
	// until the snapshot reaches a terminal phase instead of treating deletion
	// as cancellation. The existing absolute Sandbox shutdown remains bounded.
	if auth != nil && auth.evaluation.Traits.Evaluation.Recovery != nil || auth == nil {
		active, err := s.sandboxCheckpointActive(ctx, row)
		if err != nil {
			return err
		}
		if active {
			return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
				current.State, current.Reason = sandboxPending, "checkpoint_running"
				current.LeaseToken, current.LeaseUntil, current.ReconcileAt = "", nil, now.Add(15*time.Second)
				return nil
			})
		}
	}
	uid := types.UID(row.SandboxUID)
	if err := client.Delete(ctx, row.SandboxName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete sandbox: %w", err)
	}
	_, err = client.Get(ctx, row.SandboxName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("confirm sandbox deletion: %w", err)
	}
	return s.mutateSandbox(ctx, auth, row, func(_ datastore.DataStore, current *model.JobSandbox, now time.Time) error {
		if k8serrors.IsNotFound(err) {
			current.State, current.SlotReserved, current.StartReserved, current.Reason = sandboxReleased, false, false, ""
		} else {
			current.State, current.Reason = sandboxPending, "release_pending"
		}
		current.LeaseToken, current.LeaseUntil = "", nil
		current.ReconcileAt = now.Add(15 * time.Second)
		return nil
	})
}
