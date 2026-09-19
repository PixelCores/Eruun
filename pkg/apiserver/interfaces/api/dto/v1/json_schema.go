package v1

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const CanonicalJSONSchemaID = "https://eruun.io/schemas/v1/canonical-profile.json"

var (
	canonicalJSONSchemaOnce sync.Once
	canonicalJSONSchemaData []byte
	canonicalJSONSchemaErr  error
)

type canonicalApplicationJSON struct {
	ID              string                      `json:"id,omitempty"`
	Name            string                      `json:"name"`
	Namespace       string                      `json:"namespace,omitempty"`
	Alias           string                      `json:"alias,omitempty"`
	Version         string                      `json:"version,omitempty"`
	Project         string                      `json:"project,omitempty"`
	Description     string                      `json:"description,omitempty"`
	Icon            string                      `json:"icon,omitempty"`
	Components      []CreateComponentRequest    `json:"components"`
	Workflow        []CreateWorkflowStepRequest `json:"workflow,omitempty"`
	Callback        *WorkflowCallback           `json:"callback,omitempty"`
	FailurePolicy   string                      `json:"failurePolicy,omitempty"`
	TemplateEnabled *bool                       `json:"templateEnabled,omitempty"`
}

type canonicalWorkflowJSON struct {
	WorkflowID    string                      `json:"workflowId,omitempty"`
	Name          string                      `json:"name,omitempty"`
	Alias         string                      `json:"alias,omitempty"`
	Callback      *WorkflowCallback           `json:"callback,omitempty"`
	WorkflowType  config.WorkflowTaskType     `json:"workflowType,omitempty"`
	FailurePolicy string                      `json:"failurePolicy,omitempty"`
	Workflow      []CreateWorkflowStepRequest `json:"workflow"`
}

type canonicalWorkflowStepJSON struct {
	SchedulingClass string                         `json:"schedulingClass,omitempty"`
	Name            string                         `json:"name"`
	StepType        config.WorkflowStepType        `json:"stepType,omitempty"`
	JobType         config.JobType                 `json:"jobType,omitempty"`
	Approval        *WorkflowStepApproval          `json:"approval,omitempty"`
	Properties      []WorkflowProperties           `json:"properties,omitempty"`
	Components      []string                       `json:"components,omitempty"`
	Mode            string                         `json:"mode,omitempty"`
	SubSteps        []CreateWorkflowSubStepRequest `json:"subSteps,omitempty"`
}

type canonicalWorkflowSubStepJSON struct {
	SchedulingClass string               `json:"schedulingClass,omitempty"`
	Name            string               `json:"name"`
	JobType         config.JobType       `json:"jobType,omitempty"`
	Properties      []WorkflowProperties `json:"properties,omitempty"`
	Components      []string             `json:"components,omitempty"`
}

type canonicalJobJSON struct {
	WorkspaceID string               `json:"workspaceId,omitempty"`
	Name        string               `json:"name"`
	Type        string               `json:"type"`
	Spec        *spec.CommandJobSpec `json:"spec,omitempty"`
	Traits      spec.JobTraits       `json:"traits,omitempty"`
}

// CanonicalJSONSchema returns the discoverable JSON Schema bundle for the
// canonical Application, Component, Trait, Workflow, and standalone Job request profiles.
func CanonicalJSONSchema() ([]byte, error) {
	canonicalJSONSchemaOnce.Do(func() {
		builder := newSchemaBuilder()
		applicationRef := builder.schemaFor(reflect.TypeOf(canonicalApplicationJSON{}))
		componentRef := builder.schemaFor(reflect.TypeOf(CreateComponentRequest{}))
		traitRef := builder.schemaFor(reflect.TypeOf(Traits{}))
		workflowRef := builder.schemaFor(reflect.TypeOf(canonicalWorkflowJSON{}))
		jobRef := builder.schemaFor(reflect.TypeOf(canonicalJobJSON{}))
		schema := map[string]any{
			"$schema": "https://json-schema.org/draft/2020-12/schema",
			"$id":     CanonicalJSONSchemaID,
			"title":   "Eruun canonical JSON profile",
			"anyOf":   []any{applicationRef, componentRef, traitRef, workflowRef, jobRef},
			"$defs":   builder.definitions,
		}
		canonicalJSONSchemaData, canonicalJSONSchemaErr = json.MarshalIndent(schema, "", "  ")
	})
	return append([]byte(nil), canonicalJSONSchemaData...), canonicalJSONSchemaErr
}

type schemaBuilder struct {
	definitions map[string]any
	names       map[reflect.Type]string
}

func newSchemaBuilder() *schemaBuilder {
	b := &schemaBuilder{definitions: map[string]any{}, names: map[reflect.Type]string{}}
	b.names[reflect.TypeOf(canonicalApplicationJSON{})] = "Application"
	b.names[reflect.TypeOf(canonicalJobJSON{})] = "Job"
	b.names[reflect.TypeOf(CreateComponentRequest{})] = "Component"
	b.names[reflect.TypeOf(Traits{})] = "Trait"
	b.names[reflect.TypeOf(canonicalWorkflowJSON{})] = "Workflow"
	b.names[reflect.TypeOf(canonicalWorkflowStepJSON{})] = "WorkflowStep"
	b.names[reflect.TypeOf(canonicalWorkflowSubStepJSON{})] = "WorkflowSubStep"
	return b
}

func (b *schemaBuilder) schemaFor(t reflect.Type) map[string]any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(CreateWorkflowStepRequest{}) {
		return b.schemaFor(reflect.TypeOf(canonicalWorkflowStepJSON{}))
	}
	if t == reflect.TypeOf(CreateWorkflowSubStepRequest{}) {
		return b.schemaFor(reflect.TypeOf(canonicalWorkflowSubStepJSON{}))
	}
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	if t == reflect.TypeOf(intstr.IntOrString{}) {
		return map[string]any{"oneOf": []any{map[string]any{"type": "integer"}, map[string]any{"type": "string"}}}
	}

	switch t.Kind() {
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": b.schemaFor(t.Elem())}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": b.schemaFor(t.Elem())}
	case reflect.Interface:
		return map[string]any{}
	case reflect.Struct:
		return b.structSchema(t)
	default:
		return map[string]any{}
	}
}

func (b *schemaBuilder) structSchema(t reflect.Type) map[string]any {
	name := b.definitionName(t)
	if _, exists := b.definitions[name]; exists {
		return map[string]any{"$ref": "#/$defs/" + name}
	}
	b.definitions[name] = map[string]any{}
	properties := map[string]any{}
	var required []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonName, options := parseJSONTag(field.Tag.Get("json"))
		if jsonName == "-" || (!field.IsExported() && !field.Anonymous) {
			continue
		}
		if jsonName == "" {
			jsonName = field.Name
		}
		fieldSchema := b.schemaFor(field.Type)
		b.applyFieldConstraints(name, jsonName, fieldSchema)
		requiredField := b.requiredField(name, jsonName, field.Tag.Get("validate"), options)
		if !requiredField && marshalsNull(field.Type, options) {
			fieldSchema = map[string]any{
				"anyOf": []any{fieldSchema, map[string]any{"type": "null"}},
			}
		}
		properties[jsonName] = fieldSchema
		if requiredField {
			required = append(required, jsonName)
		}
	}
	definition := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties":           properties,
	}
	if len(required) > 0 {
		definition["required"] = required
	}
	b.applyDefinitionConstraints(name, definition)
	b.definitions[name] = definition
	return map[string]any{"$ref": "#/$defs/" + name}
}

func (b *schemaBuilder) definitionName(t reflect.Type) string {
	if name := b.names[t]; name != "" {
		return name
	}
	name := t.Name()
	if name == "" {
		name = "Anonymous"
	}
	if _, used := b.definitions[name]; used {
		name = strings.ReplaceAll(t.PkgPath(), "/", ".") + "." + name
	}
	b.names[t] = name
	return name
}

func (b *schemaBuilder) requiredField(definition, field, validation string, options map[string]bool) bool {
	switch definition {
	case "Application":
		return field == "name" || field == "components"
	case "Component":
		return field == "name" || field == "type"
	case "Workflow":
		return field == "workflow"
	case "WorkflowStep", "WorkflowSubStep":
		return field == "name"
	case "Job":
		return field == "name" || field == "type"
	case "EvaluationTraitSpec":
		return field == "env" || field == "agent" || field == "taskPackageId"
	case "CommandJobSpec":
		return field == "image" || field == "command"
	case "JobResultPolicy":
		return field == "retentionDays" || field == "targets"
	case "JobResultTarget":
		return field == "type" || field == "mode"
	}
	return strings.Contains(validation, "required") && !options["omitempty"]
}

func (b *schemaBuilder) applyFieldConstraints(definition, field string, schema map[string]any) {
	switch {
	case definition == "Job" && field == "type":
		schema["enum"] = []string{"command", "job"}
	case definition == "Job" && field == "name":
		schema["minLength"], schema["maxLength"] = 1, 128
	case definition == "EvaluationTraitSpec" && field == "env":
		schema["const"] = "ack"
	case definition == "EvaluationTraitSpec" && field == "agent":
		schema["enum"] = []string{"codex", "claude-code", "terminus-2", "oracle"}
	case definition == "EvaluationTraitSpec" && field == "model":
		schema["maxLength"] = 256
	case definition == "EvaluationTraitSpec" && field == "taskPackageId":
		schema["minLength"], schema["maxLength"] = 36, 36
	case definition == "EvaluationTraitSpec" && field == "attempts":
		schema["minimum"], schema["maximum"], schema["default"] = 1, 10, 1
	case definition == "EvaluationTraitSpec" && field == "concurrency":
		schema["minimum"], schema["maximum"], schema["default"] = 1, 16, 1
	case definition == "EvaluationTraitSpec" && field == "timeoutSeconds":
		schema["minimum"], schema["maximum"], schema["default"] = 60, 86400, 3600
	case definition == "EvaluationTraitSpec" && field == "sandboxResources":
		schema["required"] = []string{"cpu", "memory"}
		schema["description"] = "Resources for each trial sandbox; traits.resources applies only to the Runner."
	case definition == "JobResultPolicy" && field == "retentionDays":
		schema["minimum"], schema["maximum"] = 1, 3650
	case definition == "JobResultPolicy" && field == "targets":
		schema["minItems"], schema["maxItems"] = 1, 2
	case definition == "JobResultTarget" && field == "type":
		schema["enum"] = []string{"database", "minio"}
	case definition == "JobResultTarget" && field == "mode":
		schema["enum"] = []string{"full", "metadata"}
	case (definition == "Application" || definition == "Component" || definition == "Workflow" || definition == "WorkflowStep" || definition == "WorkflowSubStep") && field == "name":
		schema["pattern"] = `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
		schema["minLength"] = 2
		schema["maxLength"] = 63
		if definition == "Application" {
			schema["maxLength"] = datastore.PrimaryKeyMaxLength
		}
	case definition == "Component" && field == "type":
		schema["enum"] = []string{"webservice", "store", "config", "secret", "cloudjob", "job", "scheduledjob"}
	case (definition == "WorkflowStep" || definition == "WorkflowSubStep") && field == "jobType":
		schema["enum"] = []string{"deploy", "cleanup_resources", "database_reset", "log_archive_upload"}
	case (definition == "WorkflowStep" || definition == "WorkflowSubStep") && field == "schedulingClass":
		schema["enum"] = []string{"normal", "background", "high"}
	case definition == "WorkflowStep" && field == "stepType":
		schema["enum"] = []string{"component", "approval"}
	case definition == "WorkflowStep" && field == "mode":
		schema["enum"] = []string{"StepByStep", "DAG"}
	case (definition == "Application" || definition == "Workflow") && field == "failurePolicy":
		schema["enum"] = []string{"cleanup_all", "cleanup_failed"}
	case definition == "Workflow" && field == "workflowType":
		schema["enum"] = []string{
			string(config.WorkflowTaskTypeWorkflow),
			string(config.WorkflowTaskTypeUpdate),
			string(config.WorkflowTaskTypeTesting),
			string(config.WorkflowTaskTypeScanning),
			string(config.WorkflowTaskTypeDelivery),
			string(config.WorkflowTaskTypeDatabaseReset),
			string(config.WorkflowTaskTypeLogArchiveUpload),
		}
	case definition == "Workflow" && field == "workflow":
		schema["minItems"] = 1
	}
}

func (b *schemaBuilder) applyDefinitionConstraints(name string, definition map[string]any) {
	switch name {
	case "Job":
		definition["oneOf"] = []any{
			map[string]any{"required": []string{"spec"}, "properties": map[string]any{
				"type":   map[string]any{"const": "command"},
				"traits": map[string]any{"not": map[string]any{"required": []string{"evaluation"}}},
			}},
			map[string]any{"required": []string{"traits"}, "not": map[string]any{"required": []string{"spec"}}, "properties": map[string]any{
				"type":   map[string]any{"const": "job"},
				"traits": map[string]any{"required": []string{"evaluation"}},
			}},
		}
	case "Component":
		definition["allOf"] = []any{map[string]any{
			"if": map[string]any{"required": []string{"traits"}, "properties": map[string]any{
				"traits": map[string]any{"required": []string{"evaluation"}},
			}},
			"then": map[string]any{"properties": map[string]any{
				"type": map[string]any{"const": "job"}, "image": map[string]any{"maxLength": 0},
			}},
		}}
	case "EvaluationTraitSpec":
		definition["description"] = "LLM evaluation shared by standalone Jobs and Application job components."
		definition["allOf"] = []any{map[string]any{
			"if":   map[string]any{"properties": map[string]any{"agent": map[string]any{"not": map[string]any{"const": "oracle"}}}},
			"then": map[string]any{"required": []string{"model"}, "properties": map[string]any{"model": map[string]any{"minLength": 1}}},
		}}
	case "InitTraitSpec", "SidecarTraitsSpec":
		definition["allOf"] = []any{map[string]any{"properties": map[string]any{
			"traits": map[string]any{"not": map[string]any{"required": []string{"evaluation"}}},
		}}}
	}
}

func marshalsNull(t reflect.Type, options map[string]bool) bool {
	if options["omitempty"] {
		return false
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return true
	default:
		return false
	}
}

func parseJSONTag(tag string) (string, map[string]bool) {
	parts := strings.Split(tag, ",")
	options := map[string]bool{}
	for _, option := range parts[1:] {
		options[option] = true
	}
	return parts[0], options
}
