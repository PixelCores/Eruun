package contract

import "encoding/json"

const (
	TaskEnvelopeVersion       = 1
	PreExecutionFailureReason = "resource import job failed before execution"
	ExecutionFailureReason    = "resource import job failed"
)

// TaskEnvelope is the durable input contract for a one-time resource import
// job. The task type determines whether Request contains scan or manage input;
// Namespace is repeated outside the opaque request so the worker can establish
// workspace isolation before decoding user input.
type TaskEnvelope struct {
	Version   int             `json:"version"`
	Namespace string          `json:"namespace"`
	Request   json.RawMessage `json:"request"`
}
