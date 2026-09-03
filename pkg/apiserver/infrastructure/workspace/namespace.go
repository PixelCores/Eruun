package workspace

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	corev1 "k8s.io/api/core/v1"
	networkv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
)

const OwnerLabel = "eruun.io/workspace-id"
const runnerName = "eruun-runner"
const baselineName = "eruun-workspace"

type Manager struct {
	Client     kubernetes.Interface
	RESTConfig *rest.Config
	Config     spec.WorkspaceConfig
}

func (m *Manager) Ensure(ctx context.Context, w *model.Workspace) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) }, func() error { return m.ensure(ctx, w) })
}
func (m *Manager) ensure(ctx context.Context, w *model.Workspace) error {
	if m == nil || m.Client == nil || w == nil || w.ID == "" || w.Namespace == "" {
		return fmt.Errorf("workspace namespace dependencies are incomplete")
	}
	namespaces := m.Client.CoreV1().Namespaces()
	ns, err := namespaces.Get(ctx, w.Namespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		ns, err = namespaces.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: w.Namespace, Labels: map[string]string{OwnerLabel: w.ID, "pod-security.kubernetes.io/enforce": "restricted", "pod-security.kubernetes.io/enforce-version": "v1.34"}}}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			ns, err = namespaces.Get(ctx, w.Namespace, metav1.GetOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("ensure workspace namespace: %w", err)
	}
	if ns.Labels[OwnerLabel] != w.ID || ns.DeletionTimestamp != nil {
		return bcode.ErrAccountConflict
	}
	if ns.Labels["pod-security.kubernetes.io/enforce"] != "restricted" || ns.Labels["pod-security.kubernetes.io/enforce-version"] != "v1.34" {
		ns = ns.DeepCopy()
		ns.Labels["pod-security.kubernetes.io/enforce"] = "restricted"
		ns.Labels["pod-security.kubernetes.io/enforce-version"] = "v1.34"
		if _, err = namespaces.Update(ctx, ns, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("enforce workspace Pod Security: %w", err)
		}
	}
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: w.Namespace, Labels: map[string]string{OwnerLabel: w.ID}}
	}
	for _, name := range []string{runnerName, "default"} {
		sa := &corev1.ServiceAccount{ObjectMeta: meta(name), AutomountServiceAccountToken: ptr.To(false)}
		existing, e := m.Client.CoreV1().ServiceAccounts(w.Namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(e) {
			_, e = m.Client.CoreV1().ServiceAccounts(w.Namespace).Create(ctx, sa, metav1.CreateOptions{})
		} else if e == nil {
			existing.AutomountServiceAccountToken = ptr.To(false)
			_, e = m.Client.CoreV1().ServiceAccounts(w.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
		}
		if e != nil {
			return fmt.Errorf("ensure workspace service account: %w", e)
		}
	}
	role := &rbacv1.Role{ObjectMeta: meta(runnerName), Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"pods", "pods/log", "pods/exec", "services", "configmaps", "secrets", "persistentvolumeclaims", "events"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		{APIGroups: []string{"apps"}, Resources: []string{"deployments", "deployments/scale", "statefulsets", "statefulsets/scale", "replicasets"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		{APIGroups: []string{"batch"}, Resources: []string{"jobs", "cronjobs"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"ingresses"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
	}}
	if old, e := m.Client.RbacV1().Roles(w.Namespace).Get(ctx, runnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
		_, err = m.Client.RbacV1().Roles(w.Namespace).Create(ctx, role, metav1.CreateOptions{})
	} else if e != nil {
		return e
	} else {
		role.ResourceVersion = old.ResourceVersion
		_, err = m.Client.RbacV1().Roles(w.Namespace).Update(ctx, role, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure workspace role: %w", err)
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: meta(runnerName), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: runnerName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: runnerName, Namespace: w.Namespace}}}
	if old, e := m.Client.RbacV1().RoleBindings(w.Namespace).Get(ctx, runnerName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
		_, err = m.Client.RbacV1().RoleBindings(w.Namespace).Create(ctx, binding, metav1.CreateOptions{})
	} else if e != nil {
		return e
	} else {
		binding.ResourceVersion = old.ResourceVersion
		_, err = m.Client.RbacV1().RoleBindings(w.Namespace).Update(ctx, binding, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure workspace role binding: %w", err)
	}
	quota := &corev1.ResourceQuota{ObjectMeta: meta(baselineName), Spec: corev1.ResourceQuotaSpec{Hard: m.Config.Quota}}
	if old, e := m.Client.CoreV1().ResourceQuotas(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
		_, err = m.Client.CoreV1().ResourceQuotas(w.Namespace).Create(ctx, quota, metav1.CreateOptions{})
	} else if e != nil {
		return e
	} else {
		quota.ResourceVersion = old.ResourceVersion
		_, err = m.Client.CoreV1().ResourceQuotas(w.Namespace).Update(ctx, quota, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure workspace quota: %w", err)
	}
	limits := &corev1.LimitRange{ObjectMeta: meta(baselineName), Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, DefaultRequest: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}, Default: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi")}}}}}
	if old, e := m.Client.CoreV1().LimitRanges(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
		_, err = m.Client.CoreV1().LimitRanges(w.Namespace).Create(ctx, limits, metav1.CreateOptions{})
	} else if e != nil {
		return e
	} else {
		limits.ResourceVersion = old.ResourceVersion
		_, err = m.Client.CoreV1().LimitRanges(w.Namespace).Update(ctx, limits, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure workspace limit range: %w", err)
	}
	policy := m.networkPolicy(w)
	if old, e := m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Get(ctx, baselineName, metav1.GetOptions{}); apierrors.IsNotFound(e) {
		_, err = m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Create(ctx, policy, metav1.CreateOptions{})
	} else if e != nil {
		return e
	} else {
		policy.ResourceVersion = old.ResourceVersion
		_, err = m.Client.NetworkingV1().NetworkPolicies(w.Namespace).Update(ctx, policy, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure workspace network policy: %w", err)
	}
	return nil
}

func (m *Manager) networkPolicy(w *model.Workspace) *networkv1.NetworkPolicy {
	empty := &metav1.LabelSelector{}
	same := networkv1.NetworkPolicyPeer{PodSelector: empty}
	except4 := []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4", "240.0.0.0/4"}
	except6 := []string{"::/96", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "2001::/32", "2002::/16", "fc00::/7", "fe80::/10", "ff00::/8"}
	for _, cidr := range m.Config.ClusterCIDRs {
		if strings.Contains(cidr, ":") {
			except6 = append(except6, cidr)
		} else {
			except4 = append(except4, cidr)
		}
	}
	ports := []networkv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(80))}, {Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(443))}}
	dns := networkv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": m.Config.DNSNamespace}}, PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"k8s-app": "kube-dns"}}}
	policy := &networkv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: baselineName, Namespace: w.Namespace, Labels: map[string]string{OwnerLabel: w.ID}}, Spec: networkv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}, PolicyTypes: []networkv1.PolicyType{networkv1.PolicyTypeIngress, networkv1.PolicyTypeEgress}, Ingress: []networkv1.NetworkPolicyIngressRule{{From: []networkv1.NetworkPolicyPeer{same}}}, Egress: []networkv1.NetworkPolicyEgressRule{{To: []networkv1.NetworkPolicyPeer{same}}, {To: []networkv1.NetworkPolicyPeer{dns}, Ports: []networkv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(intstr.FromInt32(53))}, {Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(53))}}}, {To: []networkv1.NetworkPolicyPeer{{IPBlock: &networkv1.IPBlock{CIDR: "0.0.0.0/0", Except: except4}}, {IPBlock: &networkv1.IPBlock{CIDR: "::/0", Except: except6}}}, Ports: ports}}}}
	if m.Config.IngressNamespace != "" {
		policy.Spec.Ingress = append(policy.Spec.Ingress, networkv1.NetworkPolicyIngressRule{From: []networkv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": m.Config.IngressNamespace}}}}})
	}
	return policy
}

func (m *Manager) TenantClient(w *model.Workspace) (kubernetes.Interface, *rest.Config, error) {
	if m.RESTConfig == nil {
		return nil, nil, fmt.Errorf("workspace execution requires Kubernetes REST config")
	}
	cfg := rest.CopyConfig(m.RESTConfig)
	cfg.ContentType = "application/json"
	cfg.AcceptContentTypes = "application/json"
	cfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + w.Namespace + ":" + runnerName}
	previous := cfg.WrapTransport
	cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		if previous != nil {
			next = previous(next)
		}
		return &tenantTransport{next: next, namespace: w.Namespace, config: m.Config}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create workspace Kubernetes client: %w", err)
	}
	return client, cfg, nil
}

func (m *Manager) DeleteEmpty(ctx context.Context, w *model.Workspace) error {
	ns, err := m.Client.CoreV1().Namespaces().Get(ctx, w.Namespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if ns.Labels[OwnerLabel] != w.ID {
		return bcode.ErrAccountConflict
	}
	// Discovery covers custom namespaced resources too; incomplete discovery is
	// an error, never evidence that a namespace is empty.
	lists, err := m.Client.Discovery().ServerPreferredNamespacedResources()
	if err != nil {
		return fmt.Errorf("discover namespace resources: %w", err)
	}
	for _, list := range lists {
		for _, r := range list.APIResources {
			if !r.Namespaced || !hasVerb(r.Verbs, "list") || strings.Contains(r.Name, "/") {
				continue
			}
			if err = m.checkEmptyResource(ctx, w, list.GroupVersion, r.Name); err != nil {
				return err
			}
		}
	}
	uid := ns.UID
	return m.Client.CoreV1().Namespaces().Delete(ctx, w.Namespace, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}
func hasVerb(verbs metav1.Verbs, verb string) bool {
	for _, v := range verbs {
		if v == verb {
			return true
		}
	}
	return false
}
