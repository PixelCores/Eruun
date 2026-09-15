package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"components",
	"workflow",
	"callback",
	"failurePolicy",
	"templateEnabled",
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

	for name, raw := range fields {
		fieldName, ok := matchJSONFieldName(name, createApplicationsRequestFields)
		if !ok {
			fieldName, ok = matchJSONFieldFunc(name, extra)
		}
		if !ok {
			return fmt.Errorf("json: unknown field %q", name)
		}
		if err := decodeCreateApplicationField(fieldName, raw, req, extra); err != nil {
			return err
		}
	}
	return nil
}

func decodeCreateApplicationField(fieldName string, raw json.RawMessage, req *CreateApplicationsRequest, extra map[string]func(json.RawMessage) error) error {
	switch fieldName {
	case "id":
		return decodeStrictJSON(raw, &req.ID)
	case "name":
		return decodeStrictJSON(raw, &req.Name)
	case "namespace":
		return decodeStrictJSON(raw, &req.Namespace)
	case "alias":
		return decodeStrictJSON(raw, &req.Alias)
	case "version":
		return decodeStrictJSON(raw, &req.Version)
	case "project":
		return decodeStrictJSON(raw, &req.Project)
	case "description":
		return decodeStrictJSON(raw, &req.Description)
	case "icon":
		return decodeStrictJSON(raw, &req.Icon)
	case "components":
		return decodeStrictJSON(raw, &req.Components)
	case "workflow":
		return decodeWorkflowArray(raw, &req.Workflow)
	case "callback":
		return decodeStrictJSON(raw, &req.Callback)
	case "failurePolicy":
		return decodeStrictJSON(raw, &req.FailurePolicy)
	case "templateEnabled":
		return decodeStrictJSON(raw, &req.TemplateEnabled)
	default:
		return extra[fieldName](raw)
	}
}

func (r *UpdateApplicationWorkflowRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = UpdateApplicationWorkflowRequest{}

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
		case "workflow":
			if err := decodeWorkflowArray(raw, &r.Workflow); err != nil {
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
		case "schedulingClass":
			if err := decodeStrictJSON(raw, &r.SchedulingClass); err != nil {
				return err
			}
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

func (r CreateWorkflowStepRequest) MarshalJSON() ([]byte, error) {
	type canonicalStep struct {
		SchedulingClass string                         `json:"schedulingClass,omitempty"`
		Name            string                         `json:"name"`
		StepType        string                         `json:"stepType,omitempty"`
		JobType         string                         `json:"jobType,omitempty"`
		Approval        *WorkflowStepApproval          `json:"approval,omitempty"`
		Properties      []WorkflowProperties           `json:"properties,omitempty"`
		Components      []string                       `json:"components,omitempty"`
		Mode            string                         `json:"mode,omitempty"`
		SubSteps        []CreateWorkflowSubStepRequest `json:"subSteps,omitempty"`
	}
	return json.Marshal(canonicalStep{
		SchedulingClass: r.SchedulingClass,
		Name:            r.Name,
		StepType:        string(r.StepType),
		JobType:         string(r.WorkflowType),
		Approval:        r.Approval,
		Properties:      canonicalWorkflowProperties(r.Properties, r.propertiesList, r.propertiesFromArray),
		Components:      r.Components,
		Mode:            r.Mode,
		SubSteps:        r.SubSteps,
	})
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
		case "schedulingClass":
			if err := decodeStrictJSON(raw, &r.SchedulingClass); err != nil {
				return err
			}
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

func (r CreateWorkflowSubStepRequest) MarshalJSON() ([]byte, error) {
	type canonicalSubStep struct {
		SchedulingClass string               `json:"schedulingClass,omitempty"`
		Name            string               `json:"name"`
		JobType         string               `json:"jobType,omitempty"`
		Properties      []WorkflowProperties `json:"properties,omitempty"`
		Components      []string             `json:"components,omitempty"`
	}
	return json.Marshal(canonicalSubStep{
		SchedulingClass: r.SchedulingClass,
		Name:            r.Name,
		JobType:         string(r.WorkflowType),
		Properties:      canonicalWorkflowProperties(r.Properties, r.propertiesList, r.propertiesFromArray),
		Components:      r.Components,
	})
}

func canonicalWorkflowProperties(single WorkflowProperties, list []WorkflowProperties, fromArray bool) []WorkflowProperties {
	if fromArray {
		return list
	}
	if len(single.Policies) == 0 && single.Path == "" && single.Container == "" {
		return nil
	}
	return []WorkflowProperties{single}
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

func decodeWorkflowArray(raw json.RawMessage, workflow *[]CreateWorkflowStepRequest) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errors.New("workflow must be an array")
	}
	if trimmed[0] != '[' {
		return errors.New("workflow must be an array")
	}
	return decodeStrictJSON(raw, workflow)
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
