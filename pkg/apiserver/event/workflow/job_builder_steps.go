package workflow

import (
	"context"
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func buildWorkflowStepExecutionGroups(
	ctx context.Context,
	workflowSteps *model.WorkflowSteps,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) [][]StepExecution {
	if workflowSteps == nil || len(workflowSteps.Steps) == 0 {
		return nil
	}
	stepGroups := make([][]StepExecution, len(workflowSteps.Steps))
	for stepIndex, step := range workflowSteps.Steps {
		stepGroups[stepIndex] = buildWorkflowStepExecutions(ctx, stepIndex, step, componentMap, task, defaultJobTimeoutSeconds)
	}
	return stepGroups
}

func buildWorkflowStepExecutions(
	ctx context.Context,
	stepIndex int,
	step *model.WorkflowStep,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) []StepExecution {
	mode := normalizedStepMode(step.Mode)
	if config.ParseWorkflowStepType(string(step.StepType)) == config.WorkflowStepTypeApproval {
		return []StepExecution{{
			Name:     step.Name,
			Mode:     mode,
			StepType: config.WorkflowStepTypeApproval,
			Approval: convertApprovalExecution(step.Approval),
		}}
	}
	if len(step.SubSteps) > 0 {
		return buildSubStepExecutions(ctx, stepIndex, step, mode, componentMap, task, defaultJobTimeoutSeconds)
	}

	componentNames := step.ComponentNames()
	if len(componentNames) == 0 {
		return nil
	}
	if step.WorkflowType == config.JobDatabaseReset {
		return buildDatabaseResetStepExecution(ctx, step.Name, componentNames, step.Properties, componentMap, task, defaultJobTimeoutSeconds, workflowStepExecutionKey(stepIndex, 0))
	}
	if mode.IsParallel() && len(componentNames) > 1 {
		return buildParallelComponentExecution(ctx, step.Name, "parallel-group", mode, componentNames, step.WorkflowType, step.Properties, componentMap, task, defaultJobTimeoutSeconds, func(componentIndex int) string {
			return workflowStepExecutionKey(stepIndex, componentIndex)
		})
	}
	return buildSequentialComponentExecutions(ctx, componentNames, config.WorkflowModeStepByStep, step.WorkflowType, step.Properties, componentMap, task, defaultJobTimeoutSeconds, func(componentIndex int) string {
		return workflowStepExecutionKey(stepIndex, componentIndex)
	}, func(name string) string {
		return name
	})
}

func buildSubStepExecutions(
	ctx context.Context,
	stepIndex int,
	step *model.WorkflowStep,
	mode config.WorkflowMode,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) []StepExecution {
	if mode.IsParallel() {
		return buildParallelSubStepExecution(ctx, stepIndex, step, mode, componentMap, task, defaultJobTimeoutSeconds)
	}

	var executions []StepExecution
	for subStepIndex, sub := range step.SubSteps {
		componentNames := sub.ComponentNames()
		executions = append(executions, buildSequentialSubStepExecution(ctx, componentNames, sub, componentMap, task, defaultJobTimeoutSeconds, func(componentIndex int) string {
			return workflowSubStepExecutionKey(stepIndex, subStepIndex, componentIndex)
		})...)
	}
	return executions
}

func buildParallelSubStepExecution(
	ctx context.Context,
	stepIndex int,
	step *model.WorkflowStep,
	mode config.WorkflowMode,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) []StepExecution {
	buckets := newJobBuckets()
	for subStepIndex, sub := range step.SubSteps {
		componentNames := sub.ComponentNames()
		appendComponentGroup(
			ctx,
			buckets,
			componentNames,
			sub.WorkflowType,
			sub.Properties,
			componentMap,
			task,
			defaultJobTimeoutSeconds,
			func(componentIndex int) string {
				return workflowSubStepExecutionKey(stepIndex, subStepIndex, componentIndex)
			},
		)
	}
	if bucketsEmpty(buckets) {
		return nil
	}
	return []StepExecution{{
		Name:     firstNonEmptyJobStepName(step.Name, "parallel-group"),
		Mode:     mode,
		StepType: config.WorkflowStepTypeComponent,
		Jobs:     buckets,
	}}
}

func buildSequentialSubStepExecution(ctx context.Context, componentNames []string, sub *model.WorkflowSubStep, componentMap map[string]*model.ApplicationComponent, task *model.WorkflowQueue, defaultJobTimeoutSeconds int64, executionKeyForComponent func(componentIndex int) string) []StepExecution {
	buckets := newJobBuckets()
	appendComponentGroup(ctx, buckets, componentNames, sub.WorkflowType, sub.Properties, componentMap, task, defaultJobTimeoutSeconds, executionKeyForComponent)
	if bucketsEmpty(buckets) {
		return nil
	}
	displayName := sub.Name
	if strings.TrimSpace(displayName) == "" && len(componentNames) == 1 {
		displayName = componentNames[0]
	}
	return []StepExecution{{
		Name:     displayName,
		Mode:     config.WorkflowModeStepByStep,
		StepType: config.WorkflowStepTypeComponent,
		Jobs:     buckets,
	}}
}

func buildParallelComponentExecution(
	ctx context.Context,
	stepName string,
	fallbackStepName string,
	mode config.WorkflowMode,
	componentNames []string,
	workflowType config.JobType,
	workflowProperties []model.Policies,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	executionKeyForComponent func(componentIndex int) string,
) []StepExecution {
	buckets := newJobBuckets()
	appendComponentGroup(ctx, buckets, componentNames, workflowType, workflowProperties, componentMap, task, defaultJobTimeoutSeconds, executionKeyForComponent)
	if bucketsEmpty(buckets) {
		return nil
	}
	return []StepExecution{{
		Name:     firstNonEmptyJobStepName(stepName, fallbackStepName),
		Mode:     mode,
		StepType: config.WorkflowStepTypeComponent,
		Jobs:     buckets,
	}}
}

func buildDatabaseResetStepExecution(
	ctx context.Context,
	stepName string,
	componentNames []string,
	workflowProperties []model.Policies,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	executionKey string,
) []StepExecution {
	buckets := buildDatabaseResetJobs(ctx, componentNames, workflowProperties, componentMap, task, defaultJobTimeoutSeconds, executionKey)
	if bucketsEmpty(buckets) {
		return nil
	}
	return []StepExecution{{
		Name:     firstNonEmptyJobStepName(stepName, "database-reset"),
		Mode:     config.WorkflowModeStepByStep,
		StepType: config.WorkflowStepTypeComponent,
		Jobs:     buckets,
	}}
}

func buildSequentialComponentExecutions(
	ctx context.Context,
	componentNames []string,
	mode config.WorkflowMode,
	workflowType config.JobType,
	workflowProperties []model.Policies,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	executionKeyForComponent func(componentIndex int) string,
	displayName func(componentName string) string,
) []StepExecution {
	executions := make([]StepExecution, 0, len(componentNames))
	for componentIndex, name := range componentNames {
		buckets := newJobBuckets()
		appendComponentGroup(
			ctx,
			buckets,
			[]string{name},
			workflowType,
			workflowProperties,
			componentMap,
			task,
			defaultJobTimeoutSeconds,
			func(int) string {
				return executionKeyForComponent(componentIndex)
			},
		)
		if bucketsEmpty(buckets) {
			continue
		}
		executions = append(executions, StepExecution{
			Name:     displayName(name),
			Mode:     mode,
			StepType: config.WorkflowStepTypeComponent,
			Jobs:     buckets,
		})
	}
	return executions
}

func normalizedStepMode(mode config.WorkflowMode) config.WorkflowMode {
	if mode == "" {
		return config.WorkflowModeStepByStep
	}
	return mode
}

func firstNonEmptyJobStepName(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func convertApprovalExecution(approval *model.WorkflowStepApproval) *ApprovalExecution {
	if approval == nil {
		return nil
	}
	headers := approval.Headers
	if len(headers) == 0 {
		headers = nil
	}
	return &ApprovalExecution{
		NotifyURL:      strings.TrimSpace(approval.NotifyURL),
		Message:        strings.TrimSpace(approval.Message),
		Method:         strings.ToUpper(strings.TrimSpace(approval.Method)),
		Headers:        headers,
		TimeoutSeconds: approval.TimeoutSeconds,
	}
}

func workflowStepExecutionKey(stepIndex, componentIndex int) string {
	return fmt.Sprintf("step:%d/component:%d", stepIndex, componentIndex)
}

func workflowSubStepExecutionKey(stepIndex, subStepIndex, componentIndex int) string {
	return fmt.Sprintf("step:%d/substep:%d/component:%d", stepIndex, subStepIndex, componentIndex)
}
