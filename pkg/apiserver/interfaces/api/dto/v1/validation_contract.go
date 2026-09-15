package v1

import (
	"strconv"
	"strings"
)

type ValidationPathScope string

const (
	ValidationPathApplication ValidationPathScope = "application"
	ValidationPathWorkflow    ValidationPathScope = "workflow"
)

func FinalizeValidationErrors(errors []ValidationError, scope ValidationPathScope) []ValidationError {
	if len(errors) == 0 {
		return []ValidationError{}
	}
	for i := range errors {
		errors[i].Path = validationJSONPointer(errors[i].Field, scope)
	}
	return errors
}

func NewApplicationExecutionPlan(spec CreateApplicationsRequest) *ExecutionPlan {
	actions := make([]PlanAction, 0, len(spec.Components)+len(spec.Workflow))
	for _, component := range spec.Components {
		actions = append(actions, PlanAction{
			Order:      len(actions) + 1,
			Action:     "apply",
			Target:     component.Name,
			TargetType: "component",
			JobType:    component.ComponentType,
		})
	}
	appendWorkflowPlanActions(&actions, spec.Workflow)
	return &ExecutionPlan{Actions: actions}
}

func NewWorkflowExecutionPlan(steps []CreateWorkflowStepRequest) *ExecutionPlan {
	actions := make([]PlanAction, 0, len(steps))
	appendWorkflowPlanActions(&actions, steps)
	return &ExecutionPlan{Actions: actions}
}

func appendWorkflowPlanActions(actions *[]PlanAction, steps []CreateWorkflowStepRequest) {
	for _, step := range steps {
		action := "execute"
		if strings.EqualFold(string(step.StepType), "approval") {
			action = "await-approval"
		}
		*actions = append(*actions, PlanAction{
			Order:      len(*actions) + 1,
			Action:     action,
			Target:     step.Name,
			TargetType: "workflowStep",
			JobType:    step.WorkflowType,
			Components: append([]string(nil), step.Components...),
			Mode:       step.Mode,
		})
		for _, subStep := range step.SubSteps {
			*actions = append(*actions, PlanAction{
				Order:      len(*actions) + 1,
				Action:     "execute",
				Target:     subStep.Name,
				TargetType: "workflowSubStep",
				JobType:    subStep.WorkflowType,
				Components: append([]string(nil), subStep.Components...),
			})
		}
	}
}

func validationJSONPointer(field string, scope ValidationPathScope) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return ""
	}
	segments := validationPathSegments(field)
	if len(segments) == 0 {
		return ""
	}
	if scope == ValidationPathApplication {
		if segments[0] == "component" {
			segments[0] = "components"
		}
	}
	for i := range segments {
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(segments[i], "~", "~0"), "/", "~1")
	}
	return "/" + strings.Join(segments, "/")
}

func validationPathSegments(field string) []string {
	var segments []string
	for len(field) > 0 {
		dot := strings.IndexByte(field, '.')
		bracket := strings.IndexByte(field, '[')
		switch {
		case bracket >= 0 && (dot < 0 || bracket < dot):
			if bracket > 0 {
				segments = append(segments, field[:bracket])
			}
			end := strings.IndexByte(field[bracket:], ']')
			if end < 0 {
				segments = append(segments, field[bracket+1:])
				return segments
			}
			end += bracket
			segment := field[bracket+1 : end]
			if unquoted, err := strconv.Unquote(segment); err == nil {
				segment = unquoted
			}
			if segment != "" {
				segments = append(segments, segment)
			}
			field = strings.TrimPrefix(field[end+1:], ".")
		case dot >= 0:
			segments = append(segments, field[:dot])
			field = field[dot+1:]
		default:
			segments = append(segments, field)
			field = ""
		}
	}
	return segments
}
