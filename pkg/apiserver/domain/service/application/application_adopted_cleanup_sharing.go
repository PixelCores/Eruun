package application

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

// adoptedCleanupSharingState is rebuilt from live resources for every cleanup
// plan. Import-time exclusivity is not sufficient: another workload may start
// using a Service, Secret, ConfigMap, ServiceAccount, or RBAC object after the
// application was adopted.
type adoptedCleanupSharingState struct {
	namespace string

	snapshotUIDs            map[string]struct{}
	managedSnapshotUIDs     map[string]struct{}
	adoptedServiceAccounts  map[string]struct{}
	externalConfigMaps      map[string]struct{}
	externalSecrets         map[string]struct{}
	externalPVCs            map[string]struct{}
	externalServiceAccounts map[string]struct{}
	externalPodLabels       []labels.Set
	sharedServices          map[string]struct{}
	sharedRoles             map[string]struct{}
	hpaTargets              map[string]struct{}
}

func (c *applicationsServiceImpl) scanAdoptedCleanupSharing(
	ctx context.Context,
	snapshot *importcontract.Snapshot,
) (*adoptedCleanupSharingState, error) {
	if c.KubeClient == nil {
		return nil, fmt.Errorf("kube client is nil")
	}
	if snapshot == nil {
		return nil, fmt.Errorf("adoption snapshot is nil")
	}
	state := &adoptedCleanupSharingState{
		namespace:               strings.TrimSpace(snapshot.Namespace),
		snapshotUIDs:            make(map[string]struct{}),
		managedSnapshotUIDs:     make(map[string]struct{}),
		adoptedServiceAccounts:  make(map[string]struct{}),
		externalConfigMaps:      make(map[string]struct{}),
		externalSecrets:         make(map[string]struct{}),
		externalPVCs:            make(map[string]struct{}),
		externalServiceAccounts: make(map[string]struct{}),
		sharedServices:          make(map[string]struct{}),
		sharedRoles:             make(map[string]struct{}),
		hpaTargets:              make(map[string]struct{}),
	}
	rootUIDs := make(map[string]struct{})
	for _, resource := range snapshot.Resources {
		uid := strings.TrimSpace(resource.Source.UID)
		if uid != "" {
			state.snapshotUIDs[uid] = struct{}{}
			if strings.EqualFold(resource.Ownership, importcontract.OwnershipExclusive) &&
				strings.EqualFold(resource.Disposition, importcontract.DispositionManaged) {
				state.managedSnapshotUIDs[uid] = struct{}{}
			}
		}
		if strings.EqualFold(resource.Source.Kind, "ServiceAccount") &&
			resource.Ownership == importcontract.OwnershipExclusive &&
			resource.Disposition == importcontract.DispositionManaged {
			state.adoptedServiceAccounts[strings.TrimSpace(resource.Source.Name)] = struct{}{}
		}
		if strings.EqualFold(resource.DependencyRole, "workload") &&
			(strings.EqualFold(resource.Source.Kind, "Deployment") ||
				strings.EqualFold(resource.Source.Kind, "StatefulSet")) &&
			uid != "" {
			rootUIDs[uid] = struct{}{}
		}
	}

	deployments, err := c.KubeClient.AppsV1().Deployments(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup Deployment references: %w", err)
	}
	for index := range deployments.Items {
		workload := &deployments.Items[index]
		state.addExternalPodTemplate(string(workload.UID), rootUIDs, workload.Spec.Template)
	}
	adoptedReplicaSetUIDs := make(map[string]struct{})
	replicaSets, err := c.KubeClient.AppsV1().ReplicaSets(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup ReplicaSet references: %w", err)
	}
	for index := range replicaSets.Items {
		replicaSet := &replicaSets.Items[index]
		owner := metav1.GetControllerOf(replicaSet)
		if owner != nil && strings.EqualFold(owner.Kind, "Deployment") {
			if _, adoptedRoot := rootUIDs[strings.TrimSpace(string(owner.UID))]; adoptedRoot {
				adoptedReplicaSetUIDs[strings.TrimSpace(string(replicaSet.UID))] = struct{}{}
				continue
			}
		}
		state.addExternalPodTemplate(string(replicaSet.UID), rootUIDs, replicaSet.Spec.Template)
	}
	statefulSets, err := c.KubeClient.AppsV1().StatefulSets(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup StatefulSet references: %w", err)
	}
	for index := range statefulSets.Items {
		workload := &statefulSets.Items[index]
		state.addExternalPodTemplate(string(workload.UID), rootUIDs, workload.Spec.Template)
	}
	daemonSets, err := c.KubeClient.AppsV1().DaemonSets(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup DaemonSet references: %w", err)
	}
	for index := range daemonSets.Items {
		state.addExternalPodTemplate(string(daemonSets.Items[index].UID), rootUIDs, daemonSets.Items[index].Spec.Template)
	}
	jobs, err := c.KubeClient.BatchV1().Jobs(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup Job references: %w", err)
	}
	for index := range jobs.Items {
		state.addExternalPodTemplate(string(jobs.Items[index].UID), rootUIDs, jobs.Items[index].Spec.Template)
	}
	cronJobs, err := c.KubeClient.BatchV1().CronJobs(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup CronJob references: %w", err)
	}
	for index := range cronJobs.Items {
		state.addExternalPodTemplate(string(cronJobs.Items[index].UID), rootUIDs, cronJobs.Items[index].Spec.JobTemplate.Spec.Template)
	}
	pods, err := c.KubeClient.CoreV1().Pods(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup Pod references: %w", err)
	}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if cleanupPodBelongsToAdoptedRoot(pod, rootUIDs, adoptedReplicaSetUIDs) {
			continue
		}
		if len(pod.Labels) > 0 {
			state.externalPodLabels = append(state.externalPodLabels, labels.Set(pod.Labels))
		}
		state.addPodSpecReferences(pod.Spec)
	}
	ingresses, err := c.KubeClient.NetworkingV1().Ingresses(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup Ingress references: %w", err)
	}
	for index := range ingresses.Items {
		ingress := &ingresses.Items[index]
		if _, adopted := state.snapshotUIDs[string(ingress.UID)]; adopted {
			continue
		}
		for _, serviceName := range cleanupIngressServiceNames(ingress) {
			state.sharedServices[serviceName] = struct{}{}
		}
		for _, tls := range ingress.Spec.TLS {
			state.addName(state.externalSecrets, tls.SecretName)
		}
	}
	services, err := c.KubeClient.CoreV1().Services(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup Service selectors: %w", err)
	}
	for index := range services.Items {
		service := &services.Items[index]
		if cleanupServiceSelectsAny(service, state.externalPodLabels) {
			state.sharedServices[service.Name] = struct{}{}
		}
	}
	// An adopted Ingress can become shared after import when its backend
	// Service starts selecting an external workload. Preserve the TLS Secret
	// together with that retained Ingress.
	for index := range ingresses.Items {
		ingress := &ingresses.Items[index]
		if _, adopted := state.snapshotUIDs[string(ingress.UID)]; !adopted {
			continue
		}
		shared := false
		for _, serviceName := range cleanupIngressServiceNames(ingress) {
			if _, found := state.sharedServices[serviceName]; found {
				shared = true
				break
			}
		}
		if !shared {
			continue
		}
		for _, tls := range ingress.Spec.TLS {
			state.addName(state.externalSecrets, tls.SecretName)
		}
	}

	roleBindings, err := c.KubeClient.RbacV1().RoleBindings(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup RoleBinding references: %w", err)
	}
	clusterRoleBindings, err := c.KubeClient.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup ClusterRoleBinding references: %w", err)
	}
	for index := range clusterRoleBindings.Items {
		binding := &clusterRoleBindings.Items[index]
		state.addExternalServiceAccountSubjects(binding.Subjects, "")
	}
	for index := range roleBindings.Items {
		binding := &roleBindings.Items[index]
		if _, managed := state.managedSnapshotUIDs[string(binding.UID)]; managed {
			continue
		}
		state.addExternalServiceAccountSubjects(binding.Subjects, binding.Namespace)
		if strings.EqualFold(binding.RoleRef.Kind, "Role") {
			state.sharedRoles[binding.RoleRef.Name] = struct{}{}
		}
	}
	// A shared RoleBinding can connect several adopted ServiceAccounts. Iterate
	// to a fixed point so all subjects and referenced Roles are preserved.
	for {
		changed := false
		for index := range roleBindings.Items {
			binding := &roleBindings.Items[index]
			if _, managed := state.managedSnapshotUIDs[string(binding.UID)]; !managed ||
				!state.roleBindingHasExternalSubjects(binding) {
				continue
			}
			if strings.EqualFold(binding.RoleRef.Kind, "Role") {
				state.sharedRoles[binding.RoleRef.Name] = struct{}{}
			}
			if state.addExternalServiceAccountSubjects(binding.Subjects, binding.Namespace) {
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	if err := c.addExternalServiceAccountSecretReferences(ctx, state); err != nil {
		return nil, err
	}

	hpas, err := c.KubeClient.AutoscalingV2().HorizontalPodAutoscalers(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("scan cleanup HPA targets: %w", err)
	}
	for index := range hpas.Items {
		target := hpas.Items[index].Spec.ScaleTargetRef
		state.hpaTargets[cleanupKindNameKey(target.Kind, target.Name)] = struct{}{}
	}
	return state, nil
}

func (c *applicationsServiceImpl) addExternalServiceAccountSecretReferences(
	ctx context.Context,
	state *adoptedCleanupSharingState,
) error {
	if state == nil || len(state.externalServiceAccounts) == 0 {
		return nil
	}
	serviceAccounts, err := c.KubeClient.CoreV1().ServiceAccounts(state.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("scan cleanup ServiceAccount secret references: %w", err)
	}
	for index := range serviceAccounts.Items {
		serviceAccount := &serviceAccounts.Items[index]
		if _, external := state.externalServiceAccounts[serviceAccount.Name]; !external {
			continue
		}
		for _, reference := range serviceAccount.Secrets {
			state.addName(state.externalSecrets, reference.Name)
		}
		for _, reference := range serviceAccount.ImagePullSecrets {
			state.addName(state.externalSecrets, reference.Name)
		}
	}
	return nil
}

func cleanupPodBelongsToAdoptedRoot(
	pod *corev1.Pod,
	rootUIDs map[string]struct{},
	adoptedReplicaSetUIDs map[string]struct{},
) bool {
	if pod == nil {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil {
		return false
	}
	uid := strings.TrimSpace(string(owner.UID))
	switch {
	case strings.EqualFold(owner.Kind, "StatefulSet"):
		_, found := rootUIDs[uid]
		return found
	case strings.EqualFold(owner.Kind, "ReplicaSet"):
		_, found := adoptedReplicaSetUIDs[uid]
		return found
	default:
		return false
	}
}

func (s *adoptedCleanupSharingState) addExternalPodTemplate(
	uid string,
	rootUIDs map[string]struct{},
	template corev1.PodTemplateSpec,
) {
	if _, adoptedRoot := rootUIDs[strings.TrimSpace(uid)]; adoptedRoot {
		return
	}
	if len(template.Labels) > 0 {
		s.externalPodLabels = append(s.externalPodLabels, labels.Set(template.Labels))
	}
	s.addPodSpecReferences(template.Spec)
}

func (s *adoptedCleanupSharingState) addPodSpecReferences(spec corev1.PodSpec) {
	if serviceAccount := strings.TrimSpace(spec.ServiceAccountName); serviceAccount != "" {
		s.externalServiceAccounts[serviceAccount] = struct{}{}
	}
	for _, reference := range spec.ImagePullSecrets {
		s.addName(s.externalSecrets, reference.Name)
	}
	for _, container := range spec.InitContainers {
		s.addContainerReferences(container.EnvFrom, container.Env)
	}
	for _, container := range spec.Containers {
		s.addContainerReferences(container.EnvFrom, container.Env)
	}
	for _, container := range spec.EphemeralContainers {
		s.addContainerReferences(container.EnvFrom, container.Env)
	}
	for _, volume := range spec.Volumes {
		if volume.ConfigMap != nil {
			s.addName(s.externalConfigMaps, volume.ConfigMap.Name)
		}
		if volume.Secret != nil {
			s.addName(s.externalSecrets, volume.Secret.SecretName)
		}
		if volume.PersistentVolumeClaim != nil {
			s.addName(s.externalPVCs, volume.PersistentVolumeClaim.ClaimName)
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ConfigMap != nil {
					s.addName(s.externalConfigMaps, source.ConfigMap.Name)
				}
				if source.Secret != nil {
					s.addName(s.externalSecrets, source.Secret.Name)
				}
			}
		}
		if volume.CSI != nil && volume.CSI.NodePublishSecretRef != nil {
			s.addName(s.externalSecrets, volume.CSI.NodePublishSecretRef.Name)
		}
		if volume.AzureFile != nil {
			s.addName(s.externalSecrets, volume.AzureFile.SecretName)
		}
		if volume.RBD != nil && volume.RBD.SecretRef != nil {
			s.addName(s.externalSecrets, volume.RBD.SecretRef.Name)
		}
		if volume.CephFS != nil && volume.CephFS.SecretRef != nil {
			s.addName(s.externalSecrets, volume.CephFS.SecretRef.Name)
		}
		if volume.FlexVolume != nil && volume.FlexVolume.SecretRef != nil {
			s.addName(s.externalSecrets, volume.FlexVolume.SecretRef.Name)
		}
		if volume.Cinder != nil && volume.Cinder.SecretRef != nil {
			s.addName(s.externalSecrets, volume.Cinder.SecretRef.Name)
		}
		if volume.ScaleIO != nil && volume.ScaleIO.SecretRef != nil {
			s.addName(s.externalSecrets, volume.ScaleIO.SecretRef.Name)
		}
		if volume.StorageOS != nil && volume.StorageOS.SecretRef != nil {
			s.addName(s.externalSecrets, volume.StorageOS.SecretRef.Name)
		}
	}
}

func (s *adoptedCleanupSharingState) addContainerReferences(
	envFrom []corev1.EnvFromSource,
	env []corev1.EnvVar,
) {
	for _, source := range envFrom {
		if source.ConfigMapRef != nil {
			s.addName(s.externalConfigMaps, source.ConfigMapRef.Name)
		}
		if source.SecretRef != nil {
			s.addName(s.externalSecrets, source.SecretRef.Name)
		}
	}
	for _, variable := range env {
		if variable.ValueFrom == nil {
			continue
		}
		if variable.ValueFrom.ConfigMapKeyRef != nil {
			s.addName(s.externalConfigMaps, variable.ValueFrom.ConfigMapKeyRef.Name)
		}
		if variable.ValueFrom.SecretKeyRef != nil {
			s.addName(s.externalSecrets, variable.ValueFrom.SecretKeyRef.Name)
		}
	}
}

func (s *adoptedCleanupSharingState) addName(target map[string]struct{}, value string) {
	if name := strings.TrimSpace(value); name != "" {
		target[name] = struct{}{}
	}
}

func (s *adoptedCleanupSharingState) addExternalServiceAccountSubjects(
	subjects []rbacv1.Subject,
	defaultNamespace string,
) bool {
	changed := false
	for _, subject := range subjects {
		if !strings.EqualFold(subject.Kind, rbacv1.ServiceAccountKind) {
			continue
		}
		namespace := strings.TrimSpace(subject.Namespace)
		if namespace == "" {
			namespace = strings.TrimSpace(defaultNamespace)
		}
		if namespace != s.namespace {
			continue
		}
		name := strings.TrimSpace(subject.Name)
		if name == "" {
			continue
		}
		if _, exists := s.externalServiceAccounts[name]; !exists {
			s.externalServiceAccounts[name] = struct{}{}
			changed = true
		}
	}
	return changed
}

func (s *adoptedCleanupSharingState) blockReason(
	saved importcontract.ResourceSnapshot,
) string {
	if _, found := s.hpaTargets[cleanupKindNameKey(saved.Source.Kind, saved.Source.Name)]; found {
		return "live HorizontalPodAutoscaler targets this adopted workload"
	}
	return ""
}

func (s *adoptedCleanupSharingState) sharedReason(
	saved importcontract.ResourceSnapshot,
	live metav1.Object,
) string {
	name := strings.TrimSpace(saved.Source.Name)
	switch strings.ToLower(strings.TrimSpace(saved.Source.Kind)) {
	case "configmap":
		if _, found := s.externalConfigMaps[name]; found {
			return "referenced by a workload outside the adopted root set"
		}
	case "secret":
		if _, found := s.externalSecrets[name]; found {
			return "referenced by a workload outside the adopted root set"
		}
	case "persistentvolumeclaim":
		if _, found := s.externalPVCs[name]; found {
			return "referenced by a workload outside the adopted root set"
		}
	case "serviceaccount":
		if _, found := s.externalServiceAccounts[name]; found {
			return "referenced outside the adopted root set"
		}
	case "service":
		if _, found := s.sharedServices[name]; found {
			return "selects an external workload or is referenced by an external Ingress"
		}
	case "ingress":
		ingress, ok := live.(*networkingv1.Ingress)
		if ok {
			for _, serviceName := range cleanupIngressServiceNames(ingress) {
				if _, found := s.sharedServices[serviceName]; found {
					return "routes to a Service that is now shared"
				}
			}
		}
	case "role":
		if _, found := s.sharedRoles[name]; found {
			return "referenced by an external or shared RoleBinding"
		}
	case "rolebinding":
		if binding, ok := live.(*rbacv1.RoleBinding); ok && s.roleBindingHasExternalSubjects(binding) {
			return "grants access to an external or shared subject"
		}
	case "poddisruptionbudget":
		if pdb, ok := live.(*policyv1.PodDisruptionBudget); ok &&
			cleanupLabelSelectorMatchesAny(pdb.Spec.Selector, s.externalPodLabels) {
			return "selects a workload outside the adopted root set"
		}
	case "networkpolicy":
		if policy, ok := live.(*networkingv1.NetworkPolicy); ok &&
			cleanupLabelSelectorMatchesAny(&policy.Spec.PodSelector, s.externalPodLabels) {
			return "selects a workload outside the adopted root set"
		}
	}
	return ""
}

func (s *adoptedCleanupSharingState) roleBindingHasExternalSubjects(binding *rbacv1.RoleBinding) bool {
	if binding == nil {
		return false
	}
	for _, subject := range binding.Subjects {
		if !strings.EqualFold(subject.Kind, "ServiceAccount") {
			return true
		}
		namespace := strings.TrimSpace(subject.Namespace)
		if namespace == "" {
			namespace = binding.Namespace
		}
		if namespace != s.namespace {
			return true
		}
		name := strings.TrimSpace(subject.Name)
		if _, adopted := s.adoptedServiceAccounts[name]; !adopted {
			return true
		}
		if _, external := s.externalServiceAccounts[name]; external {
			return true
		}
	}
	return false
}

func cleanupServiceSelectsAny(service *corev1.Service, podLabels []labels.Set) bool {
	if service == nil || len(service.Spec.Selector) == 0 {
		return false
	}
	for _, candidate := range podLabels {
		matches := true
		for key, value := range service.Spec.Selector {
			if candidate[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func cleanupLabelSelectorMatchesAny(selector *metav1.LabelSelector, podLabels []labels.Set) bool {
	if selector == nil {
		return false
	}
	compiled, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		// An invalid live selector is unsafe to reason about; preserving the
		// resource is more conservative than deleting it.
		return true
	}
	for _, candidate := range podLabels {
		if compiled.Matches(candidate) {
			return true
		}
	}
	return false
}

func cleanupIngressServiceNames(ingress *networkingv1.Ingress) []string {
	if ingress == nil {
		return nil
	}
	seen := make(map[string]struct{})
	add := func(backend networkingv1.IngressBackend) {
		if backend.Service != nil {
			if name := strings.TrimSpace(backend.Service.Name); name != "" {
				seen[name] = struct{}{}
			}
		}
	}
	if ingress.Spec.DefaultBackend != nil {
		add(*ingress.Spec.DefaultBackend)
	}
	for _, rule := range ingress.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			add(path.Backend)
		}
	}
	result := make([]string, 0, len(seen))
	for name := range seen {
		result = append(result, name)
	}
	return result
}

func cleanupKindNameKey(kind, name string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + "/" + strings.TrimSpace(name)
}
