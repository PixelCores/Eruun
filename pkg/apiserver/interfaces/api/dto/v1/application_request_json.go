package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

var createApplicationsRequestFields = []string{
	"id",
	"name",
	"namespace",
	"alias",
	"version",
	"project",
	"description",
	"icon",
	"component",
	"components",
	"workflow",
	"callback",
	"templateEnabled",
}

type createApplicationWorkflowObject struct {
	Callback      *WorkflowCallback            `json:"callback,omitempty"`
	FailurePolicy config.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`
	Steps         []CreateWorkflowStepRequest  `json:"steps,omitempty"`
}

func (r *CreateApplicationsRequest) UnmarshalJSON(data []byte) error {
	return decodeCreateApplicationsRequest(data, r, nil)
}

func (r *CreateAndExecApplicationRequest) UnmarshalJSON(data []byte) error {
	var base CreateApplicationsRequest
	var workflowID string
	var executeAt int64

	extra := map[string]func(json.RawMessage) error{
		"workflowId": func(raw json.RawMessage) error {
			return decodeStrictJSON(raw, &workflowID)
		},
		"executeAt": func(raw json.RawMessage) error {
			return decodeStrictJSON(raw, &executeAt)
		},
	}
	if err := decodeCreateApplicationsRequest(data, &base, extra); err != nil {
		return err
	}

	r.CreateApplicationsRequest = base
	r.WorkflowID = workflowID
	r.ExecuteAt = executeAt
	return nil
}

func (r *TryApplicationRequest) UnmarshalJSON(data []byte) error {
	var base CreateApplicationsRequest
	var appID string

	extra := map[string]func(json.RawMessage) error{
		"appId": func(raw json.RawMessage) error {
			return decodeStrictJSON(raw, &appID)
		},
	}
	if err := decodeCreateApplicationsRequest(data, &base, extra); err != nil {
		return err
	}

	r.CreateApplicationsRequest = base
	r.AppID = appID
	return nil
}

func (r *DatabaseResetRequest) UnmarshalJSON(data []byte) error {
	var request struct {
		Components []string        `json:"components"`
		InitSQLURL json.RawMessage `json:"initSqlUrl"`
	}
	if err := decodeStrictJSON(data, &request); err != nil {
		return err
	}

	*r = DatabaseResetRequest{Components: request.Components}
	if len(request.InitSQLURL) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(request.InitSQLURL), []byte("null")) {
		return errors.New("initSqlUrl must be a string")
	}
	if err := decodeStrictJSON(request.InitSQLURL, &r.InitSQLURL); err != nil {
		return err
	}
	r.initSQLURLProvided = true
	return nil
}

// InitSQLURLProvided reports whether initSqlUrl was present in the JSON request.
// A non-empty programmatic value is also treated as provided.
func (r DatabaseResetRequest) InitSQLURLProvided() bool {
	return r.initSQLURLProvided || r.InitSQLURL != ""
}

func decodeCreateApplicationsRequest(data []byte, req *CreateApplicationsRequest, extra map[string]func(json.RawMessage) error) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*req = CreateApplicationsRequest{}
	seen := map[string]string{}

	for name, raw := range fields {
		fieldName, ok := matchJSONFieldName(name, createApplicationsRequestFields)
		if !ok {
			fieldName, ok = matchJSONFieldFunc(name, extra)
		}
		if !ok {
			return fmt.Errorf("json: unknown field %q", name)
		}
		canonicalName := canonicalCreateApplicationsRequestField(fieldName)
		if previous, ok := seen[canonicalName]; ok {
			return fmt.Errorf("json: fields %q and %q cannot both be set", previous, name)
		}
		seen[canonicalName] = name

		switch fieldName {
		case "id":
			if err := decodeStrictJSON(raw, &req.ID); err != nil {
				return err
			}
		case "name":
			if err := decodeStrictJSON(raw, &req.Name); err != nil {
				return err
			}
		case "namespace":
			if err := decodeStrictJSON(raw, &req.Namespace); err != nil {
				return err
			}
		case "alias":
			if err := decodeStrictJSON(raw, &req.Alias); err != nil {
				return err
			}
		case "version":
			if err := decodeStrictJSON(raw, &req.Version); err != nil {
				return err
			}
		case "project":
			if err := decodeStrictJSON(raw, &req.Project); err != nil {
				return err
			}
		case "description":
			if err := decodeStrictJSON(raw, &req.Description); err != nil {
				return err
			}
		case "icon":
			if err := decodeStrictJSON(raw, &req.Icon); err != nil {
				return err
			}
		case "component", "components":
			if err := decodeStrictJSON(raw, &req.Component); err != nil {
				return err
			}
		case "workflow":
			if err := decodeCreateApplicationWorkflow(raw, req); err != nil {
				return err
			}
		case "callback":
			if err := decodeStrictJSON(raw, &req.Callback); err != nil {
				return err
			}
		case "templateEnabled":
			if err := decodeStrictJSON(raw, &req.TemplateEnabled); err != nil {
				return err
			}
		default:
			if err := extra[fieldName](raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func canonicalCreateApplicationsRequestField(name string) string {
	switch name {
	case "components":
		return "component"
	default:
		return name
	}
}

func (r *UpdateApplicationWorkflowRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = UpdateApplicationWorkflowRequest{}
	var workflowField string

	for name, raw := range fields {
		switch name {
		case "workflowId":
			if err := decodeStrictJSON(raw, &r.WorkflowID); err != nil {
				return err
			}
		case "name":
			if err := decodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "alias":
			if err := decodeStrictJSON(raw, &r.Alias); err != nil {
				return err
			}
		case "callback":
			if err := decodeStrictJSON(raw, &r.Callback); err != nil {
				return err
			}
		case "workflowType":
			if err := decodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "failurePolicy":
			r.FailurePolicySet = true
			if err := decodeStrictJSON(raw, &r.FailurePolicy); err != nil {
				return err
			}
		case "workflow", "steps":
			if workflowField != "" {
				return fmt.Errorf("json: fields %q and %q cannot both be set", workflowField, name)
			}
			workflowField = name
			if err := decodeStrictJSON(raw, &r.Workflow); err != nil {
				return err
			}
		default:
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	return nil
}

func (r *CreateWorkflowStepRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = CreateWorkflowStepRequest{}
	var jobTypeField string

	for name, raw := range fields {
		switch name {
		case "name":
			if err := decodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "stepType":
			if err := decodeStrictJSON(raw, &r.StepType); err != nil {
				return err
			}
		case "jobType", "workflowType":
			if jobTypeField != "" {
				return fmt.Errorf("json: fields %q and %q cannot both be set", jobTypeField, name)
			}
			jobTypeField = name
			if err := decodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "approval":
			if err := decodeStrictJSON(raw, &r.Approval); err != nil {
				return err
			}
		case "properties":
			properties, propertiesList, fromArray, err := decodeWorkflowStepProperties(raw)
			if err != nil {
				return err
			}
			r.Properties = properties
			r.propertiesList = propertiesList
			r.propertiesFromArray = fromArray
		case "components":
			if err := decodeStrictJSON(raw, &r.Components); err != nil {
				return err
			}
		case "mode":
			if err := decodeStrictJSON(raw, &r.Mode); err != nil {
				return err
			}
		case "subSteps":
			if err := decodeStrictJSON(raw, &r.SubSteps); err != nil {
				return err
			}
		default:
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	return nil
}

func (r *CreateWorkflowSubStepRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = CreateWorkflowSubStepRequest{}
	var jobTypeField string

	for name, raw := range fields {
		switch name {
		case "name":
			if err := decodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "jobType", "workflowType":
			if jobTypeField != "" {
				return fmt.Errorf("json: fields %q and %q cannot both be set", jobTypeField, name)
			}
			jobTypeField = name
			if err := decodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "properties":
			properties, propertiesList, fromArray, err := decodeWorkflowStepProperties(raw)
			if err != nil {
				return err
			}
			r.Properties = properties
			r.propertiesList = propertiesList
			r.propertiesFromArray = fromArray
		case "components":
			if err := decodeStrictJSON(raw, &r.Components); err != nil {
				return err
			}
		default:
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	return nil
}

func decodeWorkflowStepProperties(raw json.RawMessage) (WorkflowProperties, []WorkflowProperties, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return WorkflowProperties{}, nil, false, nil
	}
	if trimmed[0] != '[' {
		var properties WorkflowProperties
		if err := decodeStrictJSON(raw, &properties); err != nil {
			return WorkflowProperties{}, nil, false, err
		}
		return properties, nil, false, nil
	}

	var propertiesList []WorkflowProperties
	if err := decodeStrictJSON(raw, &propertiesList); err != nil {
		return WorkflowProperties{}, nil, true, err
	}
	if len(propertiesList) == 0 {
		return WorkflowProperties{}, nil, true, nil
	}
	return propertiesList[0], propertiesList, true, nil
}

func matchJSONFieldName(name string, candidates []string) (string, bool) {
	for _, candidate := range candidates {
		if name == candidate {
			return candidate, true
		}
	}
	return "", false
}

func matchJSONFieldFunc(name string, funcs map[string]func(json.RawMessage) error) (string, bool) {
	if len(funcs) == 0 {
		return "", false
	}
	if _, ok := funcs[name]; ok {
		return name, true
	}
	return "", false
}

func decodeCreateApplicationWorkflow(raw json.RawMessage, req *CreateApplicationsRequest) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}

	switch trimmed[0] {
	case '[':
		return decodeStrictJSON(raw, &req.WorkflowSteps)
	case '{':
		var workflow createApplicationWorkflowObject
		if err := decodeStrictJSON(raw, &workflow); err != nil {
			return err
		}
		req.WorkflowSteps = workflow.Steps
		req.WorkflowCallback = workflow.Callback
		req.WorkflowFailurePolicy = workflow.FailurePolicy
		return nil
	default:
		return fmt.Errorf("workflow must be an array or object")
	}
}

func decodeStrictJSON(data []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body contains multiple JSON values")
		}
		return err
	}
	return nil
}
