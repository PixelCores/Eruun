package spec

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

// ValidateNodeSelectorEntry shares the label rules with targetWorkEnv validation.
func ValidateNodeSelectorEntry(key, value string) (keyError, valueError string) {
	if causes := k8svalidation.IsQualifiedName(key); len(causes) > 0 {
		keyError = "key must be a valid label key: " + strings.Join(causes, "; ")
	}
	if value == "" {
		valueError = "value must not be empty"
	} else if causes := k8svalidation.IsValidLabelValue(value); len(causes) > 0 {
		valueError = "value must be a valid label value: " + strings.Join(causes, "; ")
	}
	return
}

func ValidateSchedulingProfileName(name string) error {
	if causes := k8svalidation.IsDNS1123Label(name); len(causes) > 0 {
		return fmt.Errorf("name: %s", strings.Join(causes, "; "))
	}
	return nil
}

func ValidateNodePool(name string, selector map[string]string, tolerations []corev1.Toleration) error {
	if err := ValidateSchedulingProfileName(name); err != nil {
		return err
	}
	if len(selector) == 0 {
		return fmt.Errorf("nodeSelector: at least one label is required")
	}
	keys := make([]string, 0, len(selector))
	for key := range selector {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		keyErr, valueErr := ValidateNodeSelectorEntry(key, selector[key])
		if keyErr != "" {
			return fmt.Errorf("nodeSelector[%q]: %s", key, keyErr)
		}
		if valueErr != "" {
			return fmt.Errorf("nodeSelector[%q]: %s", key, valueErr)
		}
	}
	for i, t := range tolerations {
		field := fmt.Sprintf("tolerations[%d]", i)
		switch t.Operator {
		case corev1.TolerationOpExists:
			if t.Value != "" {
				return fmt.Errorf("%s.value: must be empty for Exists", field)
			}
		case corev1.TolerationOpEqual:
			if t.Key == "" {
				return fmt.Errorf("%s.key: must not be empty for Equal", field)
			}
		default:
			return fmt.Errorf("%s.operator: must be Equal or Exists", field)
		}
		if t.Key != "" {
			if causes := k8svalidation.IsQualifiedName(t.Key); len(causes) > 0 {
				return fmt.Errorf("%s.key: %s", field, strings.Join(causes, "; "))
			}
		}
		switch t.Effect {
		case "", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
		default:
			return fmt.Errorf("%s.effect: must be empty, NoSchedule, PreferNoSchedule or NoExecute", field)
		}
		if t.TolerationSeconds != nil && t.Effect != corev1.TaintEffectNoExecute {
			return fmt.Errorf("%s.tolerationSeconds: only allowed with NoExecute", field)
		}
	}
	return nil
}

func ValidateRBACProfile(name string, rules []RBACRuleSpec) error {
	if err := ValidateSchedulingProfileName(name); err != nil {
		return err
	}
	if len(rules) == 0 {
		return fmt.Errorf("rules: at least one rule is required")
	}
	for i, rule := range rules {
		field := fmt.Sprintf("rules[%d]", i)
		if len(rule.Verbs) == 0 {
			return fmt.Errorf("%s.verbs: at least one verb is required", field)
		}
		if len(rule.Resources) == 0 {
			return fmt.Errorf("%s.resources: at least one resource is required", field)
		}
		if len(rule.NonResourceURLs) > 0 {
			return fmt.Errorf("%s.nonResourceURLs: not allowed", field)
		}
		for _, group := range rule.APIGroups {
			if group == "rbac.authorization.k8s.io" || group == "*" {
				return fmt.Errorf("%s.apiGroups: %q is not allowed", field, group)
			}
		}
		for _, verb := range rule.Verbs {
			switch verb {
			case "bind", "escalate", "impersonate", "*":
				return fmt.Errorf("%s.verbs: %q is not allowed", field, verb)
			}
		}
		for _, resource := range rule.Resources {
			if resource == "*" || resource == "serviceaccounts/token" {
				return fmt.Errorf("%s.resources: %q is not allowed", field, resource)
			}
		}
	}
	return nil
}
