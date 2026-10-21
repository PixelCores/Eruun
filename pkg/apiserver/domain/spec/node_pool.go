package spec

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

// validateNodeSelectorEntry validates the label rules used by node pools.
func validateNodeSelectorEntry(key, value string) (keyError, valueError string) {
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

func ValidateNodePoolName(name string) error {
	if causes := k8svalidation.IsDNS1123Label(name); len(causes) > 0 {
		return fmt.Errorf("name: %s", strings.Join(causes, "; "))
	}
	return nil
}

func ValidateNodePool(name string, selector map[string]string, tolerations []corev1.Toleration) error {
	if err := ValidateNodePoolName(name); err != nil {
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
		keyErr, valueErr := validateNodeSelectorEntry(key, selector[key])
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
