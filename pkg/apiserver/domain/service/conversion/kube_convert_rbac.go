package conversion

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

type decodedRBACResources struct {
	serviceAccounts     map[string]*corev1.ServiceAccount
	roles               map[string]*rbacv1.Role
	clusterRoles        map[string]*rbacv1.ClusterRole
	roleBindings        []*rbacv1.RoleBinding
	clusterRoleBindings []*rbacv1.ClusterRoleBinding
}

func convertRBACPolicies(
	serviceAccounts []*unstructured.Unstructured,
	roles []*unstructured.Unstructured,
	roleBindings []*unstructured.Unstructured,
	clusterRoles []*unstructured.Unstructured,
	clusterRoleBindings []*unstructured.Unstructured,
) (map[string][]spec.RBACPolicySpec, []string, error) {
	decoded, warnings, err := decodeRBACResources(serviceAccounts, roles, roleBindings, clusterRoles, clusterRoleBindings)
	if err != nil {
		return nil, warnings, err
	}

	policies := make(map[string][]spec.RBACPolicySpec)
	policies, roleBindingWarnings := convertRoleBindingPolicies(decoded, policies)
	warnings = append(warnings, roleBindingWarnings...)

	policies, clusterRoleBindingWarnings := convertClusterRoleBindingPolicies(decoded, policies)
	warnings = append(warnings, clusterRoleBindingWarnings...)

	return policies, warnings, nil
}

func decodeRBACResources(
	serviceAccounts []*unstructured.Unstructured,
	roles []*unstructured.Unstructured,
	roleBindings []*unstructured.Unstructured,
	clusterRoles []*unstructured.Unstructured,
	clusterRoleBindings []*unstructured.Unstructured,
) (*decodedRBACResources, []string, error) {
	decoded := &decodedRBACResources{
		serviceAccounts:     make(map[string]*corev1.ServiceAccount),
		roles:               make(map[string]*rbacv1.Role),
		clusterRoles:        make(map[string]*rbacv1.ClusterRole),
		roleBindings:        make([]*rbacv1.RoleBinding, 0, len(roleBindings)),
		clusterRoleBindings: make([]*rbacv1.ClusterRoleBinding, 0, len(clusterRoleBindings)),
	}

	var warnings []string

	stageWarnings, err := decodeServiceAccounts(serviceAccounts, decoded.serviceAccounts)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = decodeRoles(roles, decoded.roles)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = decodeClusterRoles(clusterRoles, decoded.clusterRoles)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = decodeRoleBindings(roleBindings, &decoded.roleBindings)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	stageWarnings, err = decodeClusterRoleBindings(clusterRoleBindings, &decoded.clusterRoleBindings)
	warnings = append(warnings, stageWarnings...)
	if err != nil {
		return nil, warnings, err
	}

	return decoded, warnings, nil
}

func decodeServiceAccounts(objects []*unstructured.Unstructured, output map[string]*corev1.ServiceAccount) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		var sa corev1.ServiceAccount
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &sa); err != nil {
			return warnings, err
		}
		if strings.TrimSpace(sa.Name) == "" {
			warnings = append(warnings, "serviceaccount missing metadata.name; skipped")
			continue
		}
		output[buildNamespacedKey(sa.Namespace, sa.Name)] = &sa
	}
	return warnings, nil
}

func decodeRoles(objects []*unstructured.Unstructured, output map[string]*rbacv1.Role) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		var role rbacv1.Role
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &role); err != nil {
			return warnings, err
		}
		if strings.TrimSpace(role.Name) == "" {
			warnings = append(warnings, "role missing metadata.name; skipped")
			continue
		}
		output[buildNamespacedKey(role.Namespace, role.Name)] = &role
	}
	return warnings, nil
}

func decodeClusterRoles(objects []*unstructured.Unstructured, output map[string]*rbacv1.ClusterRole) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		var role rbacv1.ClusterRole
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &role); err != nil {
			return warnings, err
		}
		if strings.TrimSpace(role.Name) == "" {
			warnings = append(warnings, "clusterrole missing metadata.name; skipped")
			continue
		}
		output[role.Name] = &role
	}
	return warnings, nil
}

func decodeRoleBindings(objects []*unstructured.Unstructured, output *[]*rbacv1.RoleBinding) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		var binding rbacv1.RoleBinding
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &binding); err != nil {
			return warnings, err
		}
		if strings.TrimSpace(binding.Name) == "" {
			warnings = append(warnings, "rolebinding missing metadata.name; skipped")
			continue
		}
		*output = append(*output, &binding)
	}
	return warnings, nil
}

func decodeClusterRoleBindings(objects []*unstructured.Unstructured, output *[]*rbacv1.ClusterRoleBinding) ([]string, error) {
	var warnings []string
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		var binding rbacv1.ClusterRoleBinding
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &binding); err != nil {
			return warnings, err
		}
		if strings.TrimSpace(binding.Name) == "" {
			warnings = append(warnings, "clusterrolebinding missing metadata.name; skipped")
			continue
		}
		*output = append(*output, &binding)
	}
	return warnings, nil
}

func convertRoleBindingPolicies(resources *decodedRBACResources, policies map[string][]spec.RBACPolicySpec) (map[string][]spec.RBACPolicySpec, []string) {
	var warnings []string
	for _, binding := range resources.roleBindings {
		bindingWarnings := appendRoleBindingPolicies(resources, binding, policies)
		warnings = append(warnings, bindingWarnings...)
	}
	return policies, warnings
}

func appendRoleBindingPolicies(resources *decodedRBACResources, binding *rbacv1.RoleBinding, policies map[string][]spec.RBACPolicySpec) []string {
	if binding == nil {
		return nil
	}
	roleKind := strings.TrimSpace(binding.RoleRef.Kind)
	roleName := strings.TrimSpace(binding.RoleRef.Name)
	if roleKind == "" || roleName == "" {
		return []string{fmt.Sprintf("rolebinding %s missing roleRef; skipped", binding.Name)}
	}

	rules, roleLabels, resolveWarnings, ok := resolveRoleBindingRules(resources, binding, roleKind, roleName)
	if !ok {
		return resolveWarnings
	}

	specRules := convertRBACRules(rules)
	if len(specRules) == 0 {
		return append(resolveWarnings, fmt.Sprintf("rolebinding %s has empty rules; skipped", binding.Name))
	}

	warnings := resolveWarnings
	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind || strings.TrimSpace(subject.Name) == "" {
			continue
		}
		ns := strings.TrimSpace(subject.Namespace)
		if ns == "" {
			ns = binding.Namespace
		}
		policy := spec.RBACPolicySpec{
			ServiceAccount: subject.Name,
			Namespace:      ns,
			ClusterScope:   false,
			RoleName:       roleName,
			BindingName:    binding.Name,
			RoleLabels:     utils.CopyStringMap(roleLabels),
			BindingLabels:  utils.CopyStringMap(binding.Labels),
			Rules:          specRules,
		}
		fillServiceAccountMetadata(&policy, resources.serviceAccounts, ns, subject.Name)
		finalizePolicyShareLabels(&policy)
		key := buildNamespacedKey(ns, subject.Name)
		policies[key] = append(policies[key], policy)
	}

	return warnings
}

func resolveRoleBindingRules(
	resources *decodedRBACResources,
	binding *rbacv1.RoleBinding,
	roleKind string,
	roleName string,
) ([]rbacv1.PolicyRule, map[string]string, []string, bool) {
	switch roleKind {
	case "Role":
		role := resources.roles[buildNamespacedKey(binding.Namespace, roleName)]
		if role == nil {
			return nil, nil, []string{fmt.Sprintf("rolebinding %s references missing role %s; skipped", binding.Name, roleName)}, false
		}
		return role.Rules, role.Labels, nil, true
	case "ClusterRole":
		role := resources.clusterRoles[roleName]
		if role == nil {
			return nil, nil, []string{fmt.Sprintf("rolebinding %s references missing clusterrole %s; skipped", binding.Name, roleName)}, false
		}
		return role.Rules, role.Labels, []string{fmt.Sprintf("rolebinding %s references clusterrole %s; mapped to namespaced role", binding.Name, roleName)}, true
	default:
		return nil, nil, []string{fmt.Sprintf("rolebinding %s uses unsupported roleRef kind %s; skipped", binding.Name, roleKind)}, false
	}
}

func convertClusterRoleBindingPolicies(resources *decodedRBACResources, policies map[string][]spec.RBACPolicySpec) (map[string][]spec.RBACPolicySpec, []string) {
	var warnings []string
	for _, binding := range resources.clusterRoleBindings {
		bindingWarnings := appendClusterRoleBindingPolicies(resources, binding, policies)
		warnings = append(warnings, bindingWarnings...)
	}
	return policies, warnings
}

func appendClusterRoleBindingPolicies(resources *decodedRBACResources, binding *rbacv1.ClusterRoleBinding, policies map[string][]spec.RBACPolicySpec) []string {
	if binding == nil {
		return nil
	}
	roleKind := strings.TrimSpace(binding.RoleRef.Kind)
	roleName := strings.TrimSpace(binding.RoleRef.Name)
	if roleKind != "ClusterRole" || roleName == "" {
		return []string{fmt.Sprintf("clusterrolebinding %s has invalid roleRef; skipped", binding.Name)}
	}

	role := resources.clusterRoles[roleName]
	if role == nil {
		return []string{fmt.Sprintf("clusterrolebinding %s references missing clusterrole %s; skipped", binding.Name, roleName)}
	}

	specRules := convertRBACRules(role.Rules)
	if len(specRules) == 0 {
		return []string{fmt.Sprintf("clusterrolebinding %s has empty rules; skipped", binding.Name)}
	}

	for _, subject := range binding.Subjects {
		if subject.Kind != rbacv1.ServiceAccountKind || strings.TrimSpace(subject.Name) == "" {
			continue
		}
		ns := strings.TrimSpace(subject.Namespace)
		policy := spec.RBACPolicySpec{
			ServiceAccount: subject.Name,
			Namespace:      ns,
			ClusterScope:   true,
			RoleName:       roleName,
			BindingName:    binding.Name,
			RoleLabels:     utils.CopyStringMap(role.Labels),
			BindingLabels:  utils.CopyStringMap(binding.Labels),
			Rules:          specRules,
		}
		fillServiceAccountMetadata(&policy, resources.serviceAccounts, ns, subject.Name)
		finalizePolicyShareLabels(&policy)
		key := buildNamespacedKey(ns, subject.Name)
		policies[key] = append(policies[key], policy)
	}

	return nil
}

func fillServiceAccountMetadata(
	policy *spec.RBACPolicySpec,
	serviceAccounts map[string]*corev1.ServiceAccount,
	namespace string,
	name string,
) {
	if policy == nil {
		return
	}
	serviceAccount := serviceAccounts[buildNamespacedKey(namespace, name)]
	if serviceAccount == nil {
		return
	}
	policy.ServiceAccountLabels = utils.CopyStringMap(serviceAccount.Labels)
	policy.ServiceAccountAnnotations = utils.CopyStringMap(serviceAccount.Annotations)
	policy.ServiceAccountAutomountSAT = serviceAccount.AutomountServiceAccountToken
}

func finalizePolicyShareLabels(policy *spec.RBACPolicySpec) {
	if policy == nil {
		return
	}
	policy.ServiceAccountLabels = ensureShareLabels(policy.ServiceAccountLabels, policy.ServiceAccount)
	policy.RoleLabels = ensureShareLabels(policy.RoleLabels, policy.RoleName)
	policy.BindingLabels = ensureShareLabels(policy.BindingLabels, policy.BindingName)
}

func convertRBACRules(rules []rbacv1.PolicyRule) []spec.RBACRuleSpec {
	if len(rules) == 0 {
		return nil
	}
	result := make([]spec.RBACRuleSpec, 0, len(rules))
	for _, rule := range rules {
		result = append(result, spec.RBACRuleSpec{
			APIGroups:       append([]string(nil), rule.APIGroups...),
			Resources:       append([]string(nil), rule.Resources...),
			ResourceNames:   append([]string(nil), rule.ResourceNames...),
			NonResourceURLs: append([]string(nil), rule.NonResourceURLs...),
			Verbs:           append([]string(nil), rule.Verbs...),
		})
	}
	return result
}
