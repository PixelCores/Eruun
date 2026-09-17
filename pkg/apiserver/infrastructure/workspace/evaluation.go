package workspace

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	corev1 "k8s.io/api/core/v1"
	networkv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
)

const EvaluationRunnerName = "eruun-evaluation-runner"
const EvaluationRunnerLabel = "eruun.io/evaluation-runner"

type evaluationRunnerKey struct{}
type evaluationRunnerAccess struct{ taskID, image string }

// WithEvaluationRunner is used only after loading and validating a persisted
// evaluation task. It grants one trusted Runner image access to its bounded SA.
func WithEvaluationRunner(ctx context.Context, taskID, image string) context.Context {
	return context.WithValue(ctx, evaluationRunnerKey{}, evaluationRunnerAccess{taskID: taskID, image: image})
}

func prepareEvaluationJob(obj map[string]interface{}, access evaluationRunnerAccess) error {
	meta := mapAt(obj, "metadata")
	if access.taskID == "" || access.image == "" || meta["name"] != "eruun-job-"+access.taskID {
		return bcode.ErrForbidden
	}
	template := mapAt(mapAt(obj, "spec"), "template")
	pod := mapAt(template, "spec")
	if pod == nil || pod["serviceAccountName"] != EvaluationRunnerName {
		return bcode.ErrForbidden
	}
	containers, _ := pod["containers"].([]interface{})
	if len(containers) != 1 || lenValue(pod["initContainers"]) != 0 || lenValue(pod["ephemeralContainers"]) != 0 {
		return bcode.ErrForbidden
	}
	container, _ := containers[0].(map[string]interface{})
	if container["name"] != "runner" || container["image"] != access.image {
		return bcode.ErrForbidden
	}
	command, _ := container["command"].([]interface{})
	if len(command) != 2 || command[0] != "python" || command[1] != "/opt/eruun/runner.py" || lenValue(container["args"]) != 0 {
		return bcode.ErrForbidden
	}
	// Only the explicit SA/token exception is granted; all workspace container,
	// volume, host and security checks still run.
	pod["serviceAccountName"] = "default"
	pod["automountServiceAccountToken"] = false
	if err := securePodMap(pod); err != nil {
		return err
	}
	pod["serviceAccountName"] = EvaluationRunnerName
	pod["automountServiceAccountToken"] = true
	return nil
}

func PrepareEvaluationTask(task *model.JobTask, w *model.Workspace, cfg spec.WorkspaceConfig, image string) error {
	if task == nil || w == nil || task.JobType != string(config.JobEval) || task.AppID != "" || task.WorkspaceID != w.ID || task.Namespace != w.Namespace {
		return bcode.ErrForbidden
	}
	raw, err := json.Marshal(task.JobInfo)
	if err != nil {
		return err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return err
	}
	if namespace, ok := mapAt(obj, "metadata")["namespace"].(string); ok && namespace != "" && namespace != w.Namespace {
		return bcode.ErrForbidden
	}
	if err := prepareEvaluationJob(obj, evaluationRunnerAccess{taskID: task.TaskID, image: image}); err != nil {
		return err
	}
	raw, err = json.Marshal(obj)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, task.JobInfo)
}

// EnsureEvaluationRunner adds namespace-scoped framework permissions without
// extending the user workload transport or the unprivileged trial SA.
func (m *Manager) EnsureEvaluationRunner(ctx context.Context, w *model.Workspace, egress ...spec.JobRunnerEgress) error {
	if m == nil || m.Client == nil || w == nil {
		return fmt.Errorf("evaluation workspace dependencies are incomplete")
	}
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) }, func() error {
		ns, err := m.Client.CoreV1().Namespaces().Get(ctx, w.Namespace, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if ns.Labels[OwnerLabel] != w.ID || ns.DeletionTimestamp != nil {
			return bcode.ErrForbidden
		}
		meta := metav1.ObjectMeta{Name: EvaluationRunnerName, Namespace: w.Namespace, Labels: map[string]string{OwnerLabel: w.ID}}
		sa := &corev1.ServiceAccount{ObjectMeta: meta, AutomountServiceAccountToken: ptr.To(false)}
		if old, e := m.Client.CoreV1().ServiceAccounts(w.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
			_, err = m.Client.CoreV1().ServiceAccounts(w.Namespace).Create(ctx, sa, metav1.CreateOptions{})
		} else if e != nil {
			return e
		} else {
			if old.Labels[OwnerLabel] != w.ID {
				return bcode.ErrForbidden
			}
			sa.ResourceVersion = old.ResourceVersion
			_, err = m.Client.CoreV1().ServiceAccounts(w.Namespace).Update(ctx, sa, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure evaluation ServiceAccount: %w", err)
		}
		role := &rbacv1.Role{ObjectMeta: meta, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch", "create", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"pods/exec"}, Verbs: []string{"get", "create"}},
		}}
		if old, e := m.Client.RbacV1().Roles(w.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
			_, err = m.Client.RbacV1().Roles(w.Namespace).Create(ctx, role, metav1.CreateOptions{})
		} else if e != nil {
			return e
		} else {
			if old.Labels[OwnerLabel] != w.ID {
				return bcode.ErrForbidden
			}
			role.ResourceVersion = old.ResourceVersion
			_, err = m.Client.RbacV1().Roles(w.Namespace).Update(ctx, role, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure evaluation Role: %w", err)
		}
		binding := &rbacv1.RoleBinding{ObjectMeta: meta, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: EvaluationRunnerName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: EvaluationRunnerName, Namespace: w.Namespace}}}
		if old, e := m.Client.RbacV1().RoleBindings(w.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
			_, err = m.Client.RbacV1().RoleBindings(w.Namespace).Create(ctx, binding, metav1.CreateOptions{})
		} else if e != nil {
			return e
		} else {
			if old.Labels[OwnerLabel] != w.ID {
				return bcode.ErrForbidden
			}
			binding.ResourceVersion = old.ResourceVersion
			_, err = m.Client.RbacV1().RoleBindings(w.Namespace).Update(ctx, binding, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure evaluation RoleBinding: %w", err)
		}
		policy := &networkv1.NetworkPolicy{ObjectMeta: meta, Spec: networkv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{EvaluationRunnerLabel: "true"}}, PolicyTypes: []networkv1.PolicyType{networkv1.PolicyTypeEgress}}}
		for _, rule := range egress {
			policy.Spec.Egress = append(policy.Spec.Egress, networkv1.NetworkPolicyEgressRule{To: []networkv1.NetworkPolicyPeer{{IPBlock: &networkv1.IPBlock{CIDR: rule.CIDR}}}, Ports: []networkv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(rule.Port))}}})
		}
		if old, e := m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Get(ctx, EvaluationRunnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
			_, err = m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Create(ctx, policy, metav1.CreateOptions{})
		} else if e != nil {
			return e
		} else {
			if old.Labels[OwnerLabel] != w.ID {
				return bcode.ErrForbidden
			}
			policy.ResourceVersion = old.ResourceVersion
			_, err = m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Update(ctx, policy, metav1.UpdateOptions{})
		}
		return err
	})
}
