package v1

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/stretchr/testify/require"
)

func TestNormalizeWorkflowSteps(t *testing.T) {
	steps := []CreateWorkflowStepRequest{{
		Name: "MixedCase", StepType: config.WorkflowStepType("  SEQUENTIAL  "),
		Components: []string{"  api  "},
		Properties: WorkflowProperties{Path: " /var/log ", Container: " main ", Policies: []string{" cleanup "}},
		Approval:   &WorkflowStepApproval{NotifyURL: " https://example.com ", Method: " post "},
		SubSteps:   []CreateWorkflowSubStepRequest{{Name: "SubStep", Components: []string{" worker "}}},
	}}
	NormalizeWorkflowSteps(steps)
	require.Equal(t, "mixedcase", steps[0].Name)
	require.Equal(t, config.WorkflowStepType("sequential"), steps[0].StepType)
	require.Equal(t, []string{"api"}, steps[0].Components)
	require.Equal(t, "main", steps[0].Properties.Container)
	require.Equal(t, "/var/log", steps[0].Properties.Path)
	require.Equal(t, []string{"cleanup"}, steps[0].Properties.Policies)
	require.Equal(t, "POST", steps[0].Approval.Method)
	require.Equal(t, "substep", steps[0].SubSteps[0].Name)
	require.Equal(t, []string{"worker"}, steps[0].SubSteps[0].Components)
}
