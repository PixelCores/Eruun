package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"

	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"k8s.io/apimachinery/pkg/api/resource"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	HarborVersion            = "0.22.0"
	JobArchiveTimeoutSeconds = 300
	// One ten-minute control-plane interruption plus the existing six-minute
	// collection and delivery budget. This does not extend the trial runtime.
	EvaluationCollectionGraceSeconds       = 960
	DefaultRunnerWorkStorageMiB      int64 = 20480
)

// JobSpec describes one standalone command or a Job with an evaluation trait.
type JobSpec struct {
	Name   string          `json:"name"`
	Type   string          `json:"type"`
	Spec   json.RawMessage `json:"spec,omitempty"`
	Traits JobTraits       `json:"traits,omitempty"`
}

type CommandJobSpec struct {
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	Args           []string `json:"args,omitempty"`
	TimeoutSeconds int64    `json:"timeoutSeconds,omitempty"`
}

// EvaluationTraitSpec describes an LLM evaluation independently of its execution entry point.
// The platform owns the pinned framework and Runner image.
type EvaluationTraitSpec struct {
	Env              string              `json:"env"`
	Model            string              `json:"model,omitempty"`
	Agent            string              `json:"agent"`
	TaskPackageID    string              `json:"taskPackageId"`
	Attempts         int                 `json:"attempts,omitempty"`
	Concurrency      int                 `json:"concurrency,omitempty"`
	TimeoutSeconds   int64               `json:"timeoutSeconds,omitempty"`
	SandboxResources *ResourceTraitsSpec `json:"sandboxResources,omitempty"`
	ResultPolicy     *JobResultPolicy    `json:"resultPolicy,omitempty"`
}

func (e *EvaluationTraitSpec) Normalize() error {
	if e == nil {
		return fmt.Errorf("traits.eval is required")
	}
	if e.Env != "ack" {
		return fmt.Errorf("evaluation env must be ack")
	}
	if len(e.TaskPackageID) != 36 {
		return fmt.Errorf("taskPackageId must reference an uploaded task package")
	}
	switch e.Agent {
	case "terminus-2", "codex", "claude-code", "oracle":
	default:
		return fmt.Errorf("unsupported evaluation agent")
	}
	if ((e.Agent != "oracle" || e.Model != "") && strings.TrimSpace(e.Model) == "") || len(e.Model) > 256 || strings.ContainsAny(e.Model, "\r\n") {
		return fmt.Errorf("model must identify the model for the selected agent")
	}
	if e.Attempts == 0 {
		e.Attempts = 1
	}
	if e.Concurrency == 0 {
		e.Concurrency = 1
	}
	if e.Attempts < 1 || e.Attempts > 10 || e.Concurrency < 1 || e.Concurrency > 16 {
		return fmt.Errorf("attempts must be 1..10 and concurrency 1..16")
	}
	if e.TimeoutSeconds == 0 {
		e.TimeoutSeconds = 3600
	}
	if e.TimeoutSeconds < 60 || e.TimeoutSeconds > workflowconfig.MaxEvaluationTimeoutSeconds {
		return fmt.Errorf("evaluation timeoutSeconds must be 60..%d", workflowconfig.MaxEvaluationTimeoutSeconds)
	}
	if err := validateJobResources(e.SandboxResources); err != nil {
		return fmt.Errorf("sandboxResources: %w", err)
	}
	normalizeJobResources(&e.SandboxResources)
	if e.ResultPolicy != nil {
		return e.ResultPolicy.Validate()
	}
	return nil
}

type JobResultPolicy struct {
	RetentionDays int               `json:"retentionDays"`
	Targets       []JobResultTarget `json:"targets"`
}

type JobResultTarget struct {
	Type string `json:"type"`
	Mode string `json:"mode"`
}

func DefaultJobResultPolicy() JobResultPolicy {
	return JobResultPolicy{RetentionDays: 90, Targets: []JobResultTarget{{Type: "database", Mode: "full"}}}
}

func (p JobResultPolicy) Validate() error {
	if p.RetentionDays < 1 || p.RetentionDays > 3650 {
		return fmt.Errorf("retentionDays must be between 1 and 3650")
	}
	if len(p.Targets) < 1 || len(p.Targets) > 2 {
		return fmt.Errorf("choose one or both database and minio targets")
	}
	seen := map[string]bool{}
	metadata := false
	for _, target := range p.Targets {
		if seen[target.Type] {
			return fmt.Errorf("duplicate result target %q", target.Type)
		}
		seen[target.Type] = true
		switch target.Type {
		case "minio":
			if target.Mode != "full" {
				return fmt.Errorf("minio requires full mode")
			}
		case "database":
			if target.Mode != "full" && target.Mode != "metadata" {
				return fmt.Errorf("database mode must be full or metadata")
			}
			metadata = target.Mode == "metadata"
		default:
			return fmt.Errorf("unsupported result target %q", target.Type)
		}
	}
	if metadata && !seen["minio"] {
		return fmt.Errorf("database metadata mode requires minio full mode")
	}
	return nil
}

// DecodeJobJSON rejects unknown fields and concatenated JSON values at every typed boundary.
func DecodeJobJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}

func (j *JobSpec) Normalize() error {
	if j == nil || strings.TrimSpace(j.Name) == "" || len(j.Name) > 128 {
		return fmt.Errorf("name must contain 1 to 128 bytes")
	}
	switch j.Type {
	case "command":
		if j.Traits.Evaluation != nil {
			return fmt.Errorf("evaluation requires type job")
		}
		if len(j.Spec) == 0 || bytes.Equal(bytes.TrimSpace(j.Spec), []byte("null")) {
			return fmt.Errorf("spec is required")
		}
		if err := validateJobTraits(j.Traits); err != nil {
			return err
		}
		var command CommandJobSpec
		if err := DecodeJobJSON(j.Spec, &command); err != nil {
			return fmt.Errorf("command spec: %w", err)
		}
		if !ExplicitJobImage(command.Image) || len(command.Command) == 0 || command.Command[0] == "" {
			return fmt.Errorf("command requires an explicitly tagged image and command")
		}
		if command.TimeoutSeconds == 0 {
			command.TimeoutSeconds = 3600
		}
		if command.TimeoutSeconds < 1 || command.TimeoutSeconds > 86400 {
			return fmt.Errorf("timeoutSeconds must be between 1 and 86400")
		}
		j.Spec, _ = json.Marshal(command)
		normalizeJobResources(&j.Traits.Resources)
	case "job":
		if len(j.Spec) != 0 {
			return fmt.Errorf("evaluation inputs belong in traits.eval; spec is not supported")
		}
		return NormalizeEvaluationTraits(&j.Traits)
	default:
		return fmt.Errorf("type must be command or job")
	}
	return nil
}

// NormalizeEvaluationTraits is shared by standalone and Application evaluation Jobs.
func NormalizeEvaluationTraits(t *JobTraits) error {
	if t == nil || t.Evaluation == nil {
		return fmt.Errorf("traits.eval is required")
	}
	if len(t.Storage)+len(t.EnvFrom) > 0 || t.SecurityPolicy != nil {
		return fmt.Errorf("evaluation supports resources and credential envs only")
	}
	if err := validateEvaluationEnvs(t.Envs); err != nil {
		return err
	}
	if err := validateJobResources(t.Resources); err != nil {
		return err
	}
	if err := t.Evaluation.Normalize(); err != nil {
		return err
	}
	normalizeJobResources(&t.Resources)
	return nil
}

func normalizeJobResources(resources **ResourceTraitsSpec) {
	if *resources == nil {
		*resources = &ResourceTraitsSpec{CPU: "1", Memory: "2Gi", CPULimit: "2", MemoryLimit: "4Gi"}
	}
	if (*resources).CPULimit == "" {
		(*resources).CPULimit = (*resources).CPU
	}
	if (*resources).MemoryLimit == "" {
		(*resources).MemoryLimit = (*resources).Memory
	}
}

func ExplicitJobImage(image string) bool {
	if image == "" || strings.ContainsAny(image, " \t\r\n") {
		return false
	}
	if at := strings.LastIndex(image, "@sha256:"); at > 0 {
		return len(image[at+8:]) == 64
	}
	last := image[strings.LastIndex(image, "/")+1:]
	colon := strings.LastIndex(last, ":")
	return colon > 0 && colon < len(last)-1 && last[colon+1:] != "latest"
}

// evaluationCredentialEnvs is the closed set of environment variables an
// evaluation Job may declare. Because it is closed, platform-injected names
// (ERUUN_JOB_CONFIG and the POD_* field refs the builder appends) stay
// unreachable from user input without a separate prefix guard.
var evaluationCredentialEnvs = map[string]bool{
	"OPENAI_API_KEY": true, "ANTHROPIC_API_KEY": true, "GEMINI_API_KEY": true,
	"GOOGLE_API_KEY": true, "OPENROUTER_API_KEY": true, "AZURE_API_KEY": true,
}

// validateEvaluationEnvs restricts evaluation env vars to model credentials
// sourced from a Secret, so a plaintext key can never reach the Runner spec.
func validateEvaluationEnvs(envs []SimplifiedEnvSpec) error {
	seen := map[string]bool{}
	for _, env := range envs {
		if !evaluationCredentialEnvs[env.Name] || seen[env.Name] {
			return fmt.Errorf("unsupported or duplicate credential environment name")
		}
		seen[env.Name] = true
		source := env.ValueFrom
		if source.Secret == nil || source.Static != nil || source.Config != nil || source.Field != nil {
			return fmt.Errorf("evaluation credentials must come from exactly one Secret reference")
		}
		if len(validation.IsDNS1123Subdomain(source.Secret.Name)) != 0 || len(validation.IsConfigMapKey(source.Secret.Key)) != 0 {
			return fmt.Errorf("invalid credential Secret reference")
		}
	}
	return nil
}

func validateJobTraits(t JobTraits) error {
	for _, storage := range t.Storage {
		if storage.TmpCreate || storage.Size != "" || storage.StorageClass != "" {
			return fmt.Errorf("Job storage must reference an existing resource")
		}
	}
	return validateJobResources(t.Resources)
}

func validateJobResources(resources *ResourceTraitsSpec) error {
	if resources != nil {
		if resources.CPU == "" || resources.Memory == "" {
			return fmt.Errorf("resources require cpu and memory requests")
		}
		for _, pair := range [][2]string{{resources.CPU, resources.CPULimit}, {resources.Memory, resources.MemoryLimit}} {
			request, err := resource.ParseQuantity(pair[0])
			if err != nil || request.Sign() <= 0 {
				return fmt.Errorf("invalid resource request")
			}
			if pair[1] != "" {
				limit, err := resource.ParseQuantity(pair[1])
				if err != nil || limit.Cmp(request) < 0 {
					return fmt.Errorf("resource limit must be at least its request")
				}
			}
		}
		if resources.GPU != "" {
			return fmt.Errorf("GPU Job resources are not supported in the first version")
		}
	}
	return nil
}

// JobsRuntimeConfig is administrator-owned configuration loaded from a mounted Secret.
type JobsRuntimeConfig struct {
	RunnerImage          string            `json:"runnerImage"`
	RunnerWorkStorageMiB int64             `json:"runnerWorkStorageMiB,omitempty"`
	APIURL               string            `json:"apiURL"`
	MinIO                *MinIOConfig      `json:"minio,omitempty"`
	RunnerEgress         []JobRunnerEgress `json:"runnerEgress"`
}

type JobRunnerEgress struct {
	CIDR string `json:"cidr"`
	Port int32  `json:"port"`
}

type MinIOConfig struct {
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Secure    bool   `json:"secure"`
	Region    string `json:"region,omitempty"`
}

func LoadJobsRuntimeConfig(path string) (*JobsRuntimeConfig, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read jobs config: %w", err)
	}
	var cfg JobsRuntimeConfig
	if err = DecodeJobJSON(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode jobs config: %w", err)
	}
	if !ExplicitJobImage(cfg.RunnerImage) {
		return nil, fmt.Errorf("jobs runnerImage requires an explicit tag or digest")
	}
	if cfg.RunnerWorkStorageMiB == 0 {
		cfg.RunnerWorkStorageMiB = DefaultRunnerWorkStorageMiB
	}
	if cfg.RunnerWorkStorageMiB < 1024 || cfg.RunnerWorkStorageMiB > 1048576 {
		return nil, fmt.Errorf("jobs runnerWorkStorageMiB must be between 1024 and 1048576")
	}
	u, err := url.Parse(cfg.APIURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("jobs apiURL must be an HTTP(S) origin")
	}
	cfg.APIURL = strings.TrimSuffix(cfg.APIURL, "/")
	if len(cfg.RunnerEgress) == 0 {
		return nil, fmt.Errorf("jobs runnerEgress must allow the Kubernetes API and result API addresses")
	}
	for _, rule := range cfg.RunnerEgress {
		_, network, err := net.ParseCIDR(rule.CIDR)
		if err != nil || rule.Port < 1 || rule.Port > 65535 {
			return nil, fmt.Errorf("invalid jobs runnerEgress CIDR or port")
		}
		ones, bits := network.Mask.Size()
		if ones != bits {
			return nil, fmt.Errorf("jobs runnerEgress must specify individual /32 or /128 API addresses")
		}
	}
	if cfg.MinIO != nil && (cfg.MinIO.Endpoint == "" || cfg.MinIO.Bucket == "" || cfg.MinIO.AccessKey == "" || cfg.MinIO.SecretKey == "") {
		return nil, fmt.Errorf("jobs minio configuration is incomplete")
	}
	return &cfg, nil
}
