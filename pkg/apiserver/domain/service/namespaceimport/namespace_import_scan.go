package namespaceimport

import (
	"context"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

type namespaceScanStep struct {
	kindKey string
	scan    func(ctx context.Context) ([]*importResource, []string, error)
}

func (s *namespaceImportServiceImpl) scanNamespaceResourcesTableDriven(ctx context.Context, namespace string, includeKinds map[string]struct{}) ([]*importResource, []string, error) {
	resources := make([]*importResource, 0)
	warnings := make([]string, 0)

	for _, step := range s.buildNamespaceScanSteps(namespace) {
		if _, ok := includeKinds[step.kindKey]; !ok {
			continue
		}
		scanned, stepWarnings, err := step.scan(ctx)
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, scanned...)
		warnings = append(warnings, stepWarnings...)
	}

	clusterResources, clusterWarnings, err := s.scanAssociatedClusterRBACResources(ctx, namespace, includeKinds)
	if err != nil {
		return nil, nil, err
	}
	resources = append(resources, clusterResources...)
	warnings = append(warnings, clusterWarnings...)

	volumeResources, volumeWarnings, err := s.scanAssociatedPersistentVolumes(ctx, namespace, includeKinds)
	if err != nil {
		return nil, nil, err
	}
	resources = append(resources, volumeResources...)
	warnings = append(warnings, volumeWarnings...)

	sort.SliceStable(resources, func(i, j int) bool {
		if resources[i].kind != resources[j].kind {
			return resources[i].kind < resources[j].kind
		}
		return resources[i].name < resources[j].name
	})

	return resources, warnings, nil
}

func (s *namespaceImportServiceImpl) buildNamespaceScanSteps(namespace string) []namespaceScanStep {
	return []namespaceScanStep{
		{kindKey: importKindDeployments, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanDeployments(ctx, namespace)
		}},
		{kindKey: importKindStatefulSets, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanStatefulSets(ctx, namespace)
		}},
		{kindKey: importKindDaemonSets, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanDaemonSets(ctx, namespace)
		}},
		{kindKey: importKindJobs, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanJobs(ctx, namespace) }},
		{kindKey: importKindCronJobs, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanCronJobs(ctx, namespace) }},
		{kindKey: importKindConfigMaps, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanConfigMaps(ctx, namespace)
		}},
		{kindKey: importKindSecrets, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanSecrets(ctx, namespace) }},
		{kindKey: importKindPersistentVolumeClaims, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanPVCs(ctx, namespace) }},
		{kindKey: importKindServices, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanServices(ctx, namespace) }},
		{kindKey: importKindIngresses, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanIngresses(ctx, namespace) }},
		{kindKey: importKindServiceAccounts, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanServiceAccounts(ctx, namespace)
		}},
		{kindKey: importKindRoles, scan: func(ctx context.Context) ([]*importResource, []string, error) { return s.scanRoles(ctx, namespace) }},
		{kindKey: importKindRoleBindings, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanRoleBindings(ctx, namespace)
		}},
		{kindKey: importKindPodDisruptionBudgets, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanPodDisruptionBudgets(ctx, namespace)
		}},
		{kindKey: importKindNetworkPolicies, scan: func(ctx context.Context) ([]*importResource, []string, error) {
			return s.scanNetworkPolicies(ctx, namespace)
		}},
	}
}

func (s *namespaceImportServiceImpl) scanDeployments(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list deployments: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "Deployment", "apps/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindDeployments, "Deployment", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanStatefulSets(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list statefulsets: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "StatefulSet", "apps/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindStatefulSets, "StatefulSet", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanDaemonSets(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list daemonsets: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "DaemonSet", "apps/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindDaemonSets, "DaemonSet", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanJobs(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list jobs: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if isCronJobOwnedJob(item) {
			continue
		}
		obj, err := toUnstructured(item, "Job", "batch/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindJobs, "Job", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanCronJobs(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.BatchV1().CronJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list cronjobs: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "CronJob", "batch/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindCronJobs, "CronJob", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanConfigMaps(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list configmaps: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if item.Name == "kube-root-ca.crt" {
			continue
		}
		obj, err := toUnstructured(item, "ConfigMap", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindConfigMaps, "ConfigMap", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanSecrets(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list secrets: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if isSystemSecret(*item) {
			continue
		}
		obj, err := toUnstructured(item, "Secret", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindSecrets, "Secret", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanPVCs(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list persistentvolumeclaims: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "PersistentVolumeClaim", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindPersistentVolumeClaims, "PersistentVolumeClaim", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanServices(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list services: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "Service", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindServices, "Service", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanIngresses(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.NetworkingV1().Ingresses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list ingresses: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "Ingress", "networking.k8s.io/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindIngresses, "Ingress", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanServiceAccounts(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.CoreV1().ServiceAccounts(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list serviceaccounts: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		if strings.TrimSpace(item.Name) == "default" {
			continue
		}
		obj, err := toUnstructured(item, "ServiceAccount", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindServiceAccounts, "ServiceAccount", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanRoles(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.RbacV1().Roles(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list roles: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "Role", "rbac.authorization.k8s.io/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindRoles, "Role", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanRoleBindings(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.RbacV1().RoleBindings(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list rolebindings: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "RoleBinding", "rbac.authorization.k8s.io/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindRoleBindings, "RoleBinding", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanPodDisruptionBudgets(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.PolicyV1().PodDisruptionBudgets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list poddisruptionbudgets: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "PodDisruptionBudget", "policy/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindPodDisruptionBudgets, "PodDisruptionBudget", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanNetworkPolicies(ctx context.Context, namespace string) ([]*importResource, []string, error) {
	list, err := s.KubeClient.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list networkpolicies: %w", err)
	}
	resources := make([]*importResource, 0, len(list.Items))
	for i := range list.Items {
		item := &list.Items[i]
		obj, err := toUnstructured(item, "NetworkPolicy", "networking.k8s.io/v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindNetworkPolicies, "NetworkPolicy", item.Namespace, item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func (s *namespaceImportServiceImpl) scanAssociatedClusterRBACResources(ctx context.Context, namespace string, includeKinds map[string]struct{}) ([]*importResource, []string, error) {
	_, needClusterRoles := includeKinds[importKindClusterRoles]
	_, needClusterRoleBindings := includeKinds[importKindClusterRoleBindings]
	if !needClusterRoles && !needClusterRoleBindings {
		return nil, nil, nil
	}

	associatedClusterRoles, associatedClusterRoleBindings, warnings, err := s.collectAssociatedClusterRBAC(ctx, namespace)
	if err != nil {
		return nil, nil, err
	}

	resources := make([]*importResource, 0)
	if needClusterRoles {
		list, err := s.KubeClient.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("list clusterroles: %w", err)
		}
		for i := range list.Items {
			item := &list.Items[i]
			if _, ok := associatedClusterRoles[item.Name]; !ok {
				continue
			}
			obj, err := toUnstructured(item, "ClusterRole", "rbac.authorization.k8s.io/v1")
			if err != nil {
				return nil, nil, err
			}
			resources = append(resources, newImportResource(importKindClusterRoles, "ClusterRole", "", item.Name, item.Labels, obj))
		}
	}

	if needClusterRoleBindings {
		list, err := s.KubeClient.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("list clusterrolebindings: %w", err)
		}
		for i := range list.Items {
			item := &list.Items[i]
			if _, ok := associatedClusterRoleBindings[item.Name]; !ok {
				continue
			}
			obj, err := toUnstructured(item, "ClusterRoleBinding", "rbac.authorization.k8s.io/v1")
			if err != nil {
				return nil, nil, err
			}
			resources = append(resources, newImportResource(importKindClusterRoleBindings, "ClusterRoleBinding", "", item.Name, item.Labels, obj))
		}
	}

	return resources, warnings, nil
}

func (s *namespaceImportServiceImpl) scanAssociatedPersistentVolumes(
	ctx context.Context,
	namespace string,
	includeKinds map[string]struct{},
) ([]*importResource, []string, error) {
	if _, included := includeKinds[importKindPersistentVolumes]; !included {
		return nil, nil, nil
	}
	list, err := s.KubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("list persistentvolumes: %w", err)
	}
	resources := make([]*importResource, 0)
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.ClaimRef == nil || strings.TrimSpace(item.Spec.ClaimRef.Namespace) != namespace {
			continue
		}
		obj, err := toUnstructured(item, "PersistentVolume", "v1")
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, newImportResource(importKindPersistentVolumes, "PersistentVolume", "", item.Name, item.Labels, obj))
	}
	return resources, nil, nil
}

func newImportResource(kindKey, kind, namespace, name string, labels map[string]string, obj *unstructured.Unstructured) *importResource {
	return &importResource{
		kindKey:   kindKey,
		kind:      kind,
		namespace: namespace,
		name:      name,
		labels:    utils.CopyStringMap(labels),
		object:    obj,
	}
}
