package spec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestNodePoolValidation(t *testing.T) {
	seconds := int64(0)
	for _, tc := range []struct {
		name        string
		selector    map[string]string
		tolerations []corev1.Toleration
		field       string
	}{
		{name: "valid", selector: map[string]string{"eruun.io/pool": "cpu"}, tolerations: []corev1.Toleration{
			{Operator: corev1.TolerationOpExists},
			{Key: "pool", Operator: corev1.TolerationOpEqual, Value: "cpu", Effect: corev1.TaintEffectNoSchedule},
			{Key: "pool", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectPreferNoSchedule},
			{Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		}},
		{name: "empty selector", field: "nodeSelector"},
		{name: "invalid key", selector: map[string]string{"bad key": "cpu"}, field: `nodeSelector["bad key"]`},
		{name: "empty value", selector: map[string]string{"pool": ""}, field: `nodeSelector["pool"]`},
		{name: "invalid value", selector: map[string]string{"pool": "bad value"}, field: `nodeSelector["pool"]`},
		{name: "empty operator", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{}}, field: "tolerations[0].operator"},
		{name: "invalid operator", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Operator: "Other"}}, field: "tolerations[0].operator"},
		{name: "exists value", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists, Value: "cpu"}}, field: "tolerations[0].value"},
		{name: "equal empty key", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpEqual}}, field: "tolerations[0].key"},
		{name: "invalid tolerance key", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Key: "bad key", Operator: corev1.TolerationOpExists}}, field: "tolerations[0].key"},
		{name: "invalid effect", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists, Effect: "Other"}}, field: "tolerations[0].effect"},
		{name: "seconds without NoExecute", selector: map[string]string{"pool": "cpu"}, tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists, TolerationSeconds: &seconds}}, field: "tolerations[0].tolerationSeconds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNodePool("pool", tc.selector, tc.tolerations)
			if tc.field == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.field)
			}
		})
	}
}

func TestSchedulingProfileNames(t *testing.T) {
	for _, name := range []string{"a", "gpu-a100", strings.Repeat("a", 63)} {
		require.NoError(t, ValidateSchedulingProfileName(name))
	}
	for _, name := range []string{"", "Upper", "has.dot", "-bad", "bad-", strings.Repeat("a", 64), " padded "} {
		require.ErrorContains(t, ValidateNodePool(name, map[string]string{"pool": "cpu"}, nil), "name")
		require.ErrorContains(t, ValidateRBACProfile(name, []RBACRuleSpec{{Resources: []string{"pods"}, Verbs: []string{"get"}}}), "name")
	}
}

func TestRBACProfileValidation(t *testing.T) {
	valid := RBACRuleSpec{APIGroups: []string{""}, Resources: []string{"pods", "pods/log"}, ResourceNames: []string{"web"}, Verbs: []string{"get", "list", "watch"}}
	require.NoError(t, ValidateRBACProfile("reader", []RBACRuleSpec{valid}))
	require.ErrorContains(t, ValidateRBACProfile("reader", nil), "rules")
	cases := []struct {
		name, field string
		rule        RBACRuleSpec
	}{
		{"no verbs", "verbs", RBACRuleSpec{Resources: valid.Resources}},
		{"no resources", "resources", RBACRuleSpec{Verbs: valid.Verbs}},
		{"non resource URL", "nonResourceURLs", RBACRuleSpec{Resources: valid.Resources, Verbs: valid.Verbs, NonResourceURLs: []string{"/healthz"}}},
	}
	for _, group := range []string{"rbac.authorization.k8s.io", "*"} {
		rule := valid
		rule.APIGroups = []string{group}
		cases = append(cases, struct {
			name, field string
			rule        RBACRuleSpec
		}{"group " + group, "apiGroups", rule})
	}
	for _, verb := range []string{"bind", "escalate", "impersonate", "*"} {
		rule := valid
		rule.Verbs = []string{verb}
		cases = append(cases, struct {
			name, field string
			rule        RBACRuleSpec
		}{"verb " + verb, "verbs", rule})
	}
	for _, resource := range []string{"*", "serviceaccounts/token"} {
		rule := valid
		rule.Resources = []string{resource}
		cases = append(cases, struct {
			name, field string
			rule        RBACRuleSpec
		}{"resource " + resource, "resources", rule})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, ValidateRBACProfile("reader", []RBACRuleSpec{valid, tc.rule}), "rules[1]."+tc.field)
		})
	}
}
