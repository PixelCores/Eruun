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
	}
}
