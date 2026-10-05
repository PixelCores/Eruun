package application

import (
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/stretchr/testify/require"
)

func TestVersionUpdateWorkflowDeploymentComponents(t *testing.T) {
	tests := []struct {
		name  string
		steps []*model.WorkflowStep
		want  map[string]struct{}
	}{
		{
			name: "empty workflow",
			want: map[string]struct{}{},
		},
		{
			name:  "nil and blank steps",
			steps: []*model.WorkflowStep{nil, {}, {Name: " \t"}},
			want:  map[string]struct{}{},
		},
		{
			name: "normalize and deduplicate policy and fallback names",
			steps: []*model.WorkflowStep{
				{
					Name:         "group",
					WorkflowType: config.JobDeploy,
					Properties: []model.Policies{
						{Policies: []string{" API ", "api", "", " \t"}},
						{Policies: []string{" Worker "}},
					},
				},
				{Name: " WORKER "},
				{Name: " Database "},
			},
			want: map[string]struct{}{"api": {}, "worker": {}, "database": {}},
		},
		{
			name: "only omitted and deploy job types cover components",
			steps: []*model.WorkflowStep{
				{Name: "omitted"},
				{Name: "whitespace", WorkflowType: " \t"},
				{Name: "deploy", WorkflowType: " deploy "},
				{Name: "cleanup", WorkflowType: config.JobCleanupResources},
				{Name: "reset", WorkflowType: config.JobDatabaseReset},
				{Name: "archive", WorkflowType: config.JobLogArchiveUpload},
				{Name: "unsupported", WorkflowType: "DEPLOY"},
			},
			want: map[string]struct{}{"omitted": {}, "whitespace": {}, "deploy": {}},
		},
		{
			name: "approval excludes the entire step",
			steps: []*model.WorkflowStep{
				{
					Name:         "approval",
					StepType:     " APPROVAL ",
					WorkflowType: config.JobDeploy,
					Properties:   []model.Policies{{Policies: []string{"parent"}}},
					SubSteps:     []*model.WorkflowSubStep{{Name: "child", WorkflowType: config.JobDeploy}},
				},
				{Name: "approval-name", StepType: config.WorkflowStepTypeApproval},
			},
			want: map[string]struct{}{},
		},
		{
			name: "substeps determine coverage independently of parent job type",
			steps: []*model.WorkflowStep{
				{
					Name:         "parent",
					WorkflowType: config.JobCleanupResources,
					Properties:   []model.Policies{{Policies: []string{"parent-policy"}}},
					SubSteps: []*model.WorkflowSubStep{
						nil,
						{},
						{Name: " API "},
						{
							Name:         "child-group",
							WorkflowType: " deploy ",
							Properties:   []model.Policies{{Policies: []string{"api", " WORKER ", " "}}},
						},
						{Name: "cleanup", WorkflowType: config.JobCleanupResources},
						{Name: "unsupported", WorkflowType: "unknown"},
					},
				},
				{Name: "nil-substep-parent", SubSteps: []*model.WorkflowSubStep{nil}},
			},
			want: map[string]struct{}{"api": {}, "worker": {}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionUpdateWorkflowDeploymentComponents(model.WorkflowSteps{Steps: tt.steps})
			require.Equal(t, tt.want, got)
		})
	}
}
