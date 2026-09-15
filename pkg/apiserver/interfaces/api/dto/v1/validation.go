package v1

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
)

// ValidationError represents a single validation error with field path and details
type ValidationError struct {
	// Field is the legacy validation path returned by non-Try APIs.
	Field string `json:"field"`
	// Code is the error code for programmatic handling
	Code string `json:"code"`
	// Message is the human-readable error description
	Message string `json:"message"`
}

// TryValidationError is the structured validation error returned by Try APIs.
type TryValidationError struct {
	// Field retains the internal validation path for in-process consumers.
	Field string `json:"-"`
	// Path is the canonical JSON Pointer to the invalid value.
	Path string `json:"path"`
	// Code is the error code for programmatic handling.
	Code string `json:"code"`
	// Message is the human-readable error description.
	Message string `json:"message"`
}

// TryApplicationRequest is exactly the canonical Application write model.
// Existing-application workflow validation uses the dedicated :appID/workflow/try endpoint.
type TryApplicationRequest = CreateApplicationsRequest

// TryApplicationResponse is the response for the try application validation API
type TryApplicationResponse struct {
	// Valid indicates whether the application configuration passes all validations
	Valid bool `json:"valid"`
	// Errors contains all validation errors found during validation
	Errors []TryValidationError `json:"errors"`
	// NormalizedSpec is accepted by the application write APIs without reshaping.
	NormalizedSpec *CreateApplicationsRequest `json:"normalizedSpec,omitempty"`
	// Plan is the ordered logical execution plan derived from NormalizedSpec.
	Plan *ExecutionPlan `json:"plan,omitempty"`
}

// TryWorkflowRequest is the request for the try workflow validation API
type TryWorkflowRequest struct {
	// WorkflowID is optional - if provided, validates against existing workflow
	WorkflowID string `json:"workflowId,omitempty"`
	// Name is the workflow name
	Name string `json:"name,omitempty"`
	// Alias is the workflow alias
	Alias string `json:"alias,omitempty"`
	// WorkflowType is the top-level workflow task type
	WorkflowType config.WorkflowTaskType `json:"workflowType,omitempty"`
	// Callback is the workflow-level terminal callback to validate with update workflow payloads
	Callback *WorkflowCallback `json:"callback,omitempty"`
	// FailurePolicy controls cleanup behavior when a deploy job fails or times out
	FailurePolicy workflowconfig.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`
	// FailurePolicySet distinguishes an omitted policy from an explicit empty reset.
	FailurePolicySet bool `json:"-"`
	// Workflow contains the workflow steps to validate.
	Workflow []CreateWorkflowStepRequest `json:"workflow" validate:"required,min=1,dive"`
}

// TryWorkflowResponse is the response for the try workflow validation API
type TryWorkflowResponse struct {
	// Valid indicates whether the workflow configuration passes all validations
	Valid bool `json:"valid"`
	// Errors contains all validation errors found during validation
	Errors []TryValidationError `json:"errors"`
	// NormalizedSpec is accepted by the workflow update API without reshaping.
	NormalizedSpec *UpdateApplicationWorkflowRequest `json:"normalizedSpec,omitempty"`
	// Plan is the ordered logical execution plan derived from NormalizedSpec.
	Plan *ExecutionPlan `json:"plan,omitempty"`
}

type ExecutionPlan struct {
	Actions []PlanAction `json:"actions"`
}

type PlanAction struct {
	Order      int            `json:"order"`
	Action     string         `json:"action"`
	Target     string         `json:"target"`
	TargetType string         `json:"targetType"`
	JobType    config.JobType `json:"jobType,omitempty"`
	Components []string       `json:"components,omitempty"`
	Mode       string         `json:"mode,omitempty"`
}

// Validation error codes
const (
	// Naming errors
	ErrCodeInvalidName          = "INVALID_NAME"
	ErrCodeNameTooShort         = "NAME_TOO_SHORT"
	ErrCodeNameTooLong          = "NAME_TOO_LONG"
	ErrCodeInvalidNameFormat    = "INVALID_NAME_FORMAT"
	ErrCodeInvalidComponentName = "INVALID_COMPONENT_NAME"
	ErrCodeInvalidStepName      = "INVALID_STEP_NAME"

	// Component errors
	ErrCodeInvalidComponentType = "INVALID_COMPONENT_TYPE"
	ErrCodeMissingImage         = "MISSING_IMAGE"
	ErrCodeDuplicateComponent   = "DUPLICATE_COMPONENT"

	// Traits errors
	ErrCodeInvalidTraitConfig    = "INVALID_TRAIT_CONFIG"
	ErrCodeMissingRequiredField  = "MISSING_REQUIRED_FIELD"
	ErrCodeInvalidStorageType    = "INVALID_STORAGE_TYPE"
	ErrCodeInvalidStorageSize    = "INVALID_STORAGE_SIZE"
	ErrCodeInvalidProbeType      = "INVALID_PROBE_TYPE"
	ErrCodeInvalidProbeConfig    = "INVALID_PROBE_CONFIG"
	ErrCodeNestedTraitForbidden  = "NESTED_TRAIT_FORBIDDEN"
	ErrCodeMissingRBACRules      = "MISSING_RBAC_RULES"
	ErrCodeMissingRBACVerbs      = "MISSING_RBAC_VERBS"
	ErrCodeMissingIngressRoutes  = "MISSING_INGRESS_ROUTES"
	ErrCodeMissingServiceName    = "MISSING_SERVICE_NAME"
	ErrCodeInvalidEnvFromType    = "INVALID_ENVFROM_TYPE"
	ErrCodeInvalidEnvValueSource = "INVALID_ENV_VALUE_SOURCE"
	ErrCodeInvalidJobSchedule    = "INVALID_JOB_SCHEDULE"
	ErrCodeInvalidJobRunPolicy   = "INVALID_JOB_RUN_POLICY"
	ErrCodeInvalidJobStartTime   = "INVALID_JOB_START_TIME"

	// Workflow errors
	ErrCodeComponentNotFound            = "COMPONENT_NOT_FOUND"
	ErrCodeInvalidWorkflowMode          = "INVALID_WORKFLOW_MODE"
	ErrCodeInvalidWorkflowStepType      = "INVALID_WORKFLOW_STEP_TYPE"
	ErrCodeInvalidWorkflowFailurePolicy = "INVALID_WORKFLOW_FAILURE_POLICY"
	ErrCodeInvalidApprovalConfig        = "INVALID_APPROVAL_CONFIG"
	ErrCodeEmptyWorkflowStep            = "EMPTY_WORKFLOW_STEP"
	ErrCodeDuplicateWorkflowStep        = "DUPLICATE_WORKFLOW_STEP"
	ErrCodeWorkflowStepNoComponent      = "WORKFLOW_STEP_NO_COMPONENT"
)

const ErrCodeInvalidJobFailurePolicy = "INVALID_JOB_FAILURE_POLICY"
