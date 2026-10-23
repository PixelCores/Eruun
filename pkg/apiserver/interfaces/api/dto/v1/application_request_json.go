package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// The request decoders below match field names exactly (encoding/json alone
// is case-insensitive) and reject unknown fields even through json.Unmarshal.

func (r *CreateApplicationsRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = CreateApplicationsRequest{}
	for name, raw := range fields {
		known, err := decodeCreateApplicationField(name, raw, r)
		if err != nil {
			return err
		}
		if !known {
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	return nil
}

func (r *CreateAndExecApplicationRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*r = CreateAndExecApplicationRequest{}
	for name, raw := range fields {
		var err error
		switch name {
		case "workflowId":
			err = DecodeStrictJSON(raw, &r.WorkflowID)
		case "executeAt":
			err = DecodeStrictJSON(raw, &r.ExecuteAt)
		default:
			var known bool
			known, err = decodeCreateApplicationField(name, raw, &r.CreateApplicationsRequest)
			if err == nil && !known {
				err = fmt.Errorf("json: unknown field %q", name)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DatabaseResetRequest) UnmarshalJSON(data []byte) error {
	var request struct {
		Components []string        `json:"components"`
		InitSQLURL json.RawMessage `json:"initSqlUrl"`
	}
	if err := DecodeStrictJSON(data, &request); err != nil {
		return err
	}

	*r = DatabaseResetRequest{Components: request.Components}
	if len(request.InitSQLURL) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(request.InitSQLURL), []byte("null")) {
		return errors.New("initSqlUrl must be a string")
	}
	if err := DecodeStrictJSON(request.InitSQLURL, &r.InitSQLURL); err != nil {
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

func decodeCreateApplicationField(name string, raw json.RawMessage, req *CreateApplicationsRequest) (bool, error) {
	switch name {
	case "id":
		return true, DecodeStrictJSON(raw, &req.ID)
	case "name":
		return true, DecodeStrictJSON(raw, &req.Name)
	case "namespace":
		return true, DecodeStrictJSON(raw, &req.Namespace)
	case "alias":
		return true, DecodeStrictJSON(raw, &req.Alias)
	case "version":
		return true, DecodeStrictJSON(raw, &req.Version)
	case "project":
		return true, DecodeStrictJSON(raw, &req.Project)
	case "description":
		return true, DecodeStrictJSON(raw, &req.Description)
	case "icon":
		return true, DecodeStrictJSON(raw, &req.Icon)
	case "components":
		return true, DecodeStrictJSON(raw, &req.Components)
	case "workflow":
		return true, decodeWorkflowArray(raw, &req.Workflow)
	case "callback":
		return true, DecodeStrictJSON(raw, &req.Callback)
	case "failurePolicy":
		return true, DecodeStrictJSON(raw, &req.FailurePolicy)
	case "templateEnabled":
		return true, DecodeStrictJSON(raw, &req.TemplateEnabled)
	default:
		return false, nil
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
			if err := DecodeStrictJSON(raw, &r.WorkflowID); err != nil {
				return err
			}
		case "name":
			if err := DecodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "alias":
			if err := DecodeStrictJSON(raw, &r.Alias); err != nil {
				return err
			}
		case "callback":
			if err := DecodeStrictJSON(raw, &r.Callback); err != nil {
				return err
			}
		case "workflowType":
			if err := DecodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "failurePolicy":
			r.FailurePolicySet = true
			if err := DecodeStrictJSON(raw, &r.FailurePolicy); err != nil {
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

	for name, raw := range fields {
		switch name {
		case "schedulingClass":
			if err := DecodeStrictJSON(raw, &r.SchedulingClass); err != nil {
				return err
			}
		case "name":
			if err := DecodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "stepType":
			if err := DecodeStrictJSON(raw, &r.StepType); err != nil {
				return err
			}
		case "jobType":
			if err := DecodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "approval":
			if err := DecodeStrictJSON(raw, &r.Approval); err != nil {
				return err
			}
		case "properties":
			if err := decodeWorkflowStepProperties(raw, &r.Properties); err != nil {
				return err
			}
		case "components":
			if err := DecodeStrictJSON(raw, &r.Components); err != nil {
				return err
			}
		case "mode":
			if err := DecodeStrictJSON(raw, &r.Mode); err != nil {
				return err
			}
		case "subSteps":
			if err := DecodeStrictJSON(raw, &r.SubSteps); err != nil {
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

	for name, raw := range fields {
		switch name {
		case "schedulingClass":
			if err := DecodeStrictJSON(raw, &r.SchedulingClass); err != nil {
				return err
			}
		case "name":
			if err := DecodeStrictJSON(raw, &r.Name); err != nil {
				return err
			}
		case "jobType":
			if err := DecodeStrictJSON(raw, &r.WorkflowType); err != nil {
				return err
			}
		case "properties":
			if err := decodeWorkflowStepProperties(raw, &r.Properties); err != nil {
				return err
			}
		case "components":
			if err := DecodeStrictJSON(raw, &r.Components); err != nil {
				return err
			}
		default:
			return fmt.Errorf("json: unknown field %q", name)
		}
	}
	return nil
}

func decodeWorkflowStepProperties(raw json.RawMessage, properties *[]WorkflowProperties) error {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return errors.New("workflow step properties must be an array")
	}
	return DecodeStrictJSON(raw, properties)
}

func decodeWorkflowArray(raw json.RawMessage, workflow *[]CreateWorkflowStepRequest) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return errors.New("workflow must be an array")
	}
	return DecodeStrictJSON(raw, workflow)
}

// DecodeStrictJSON decodes exactly one JSON value into target and rejects
// unknown fields; HTTP binding and gRPC request decoding share it.
func DecodeStrictJSON(data []byte, target interface{}) error {
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
