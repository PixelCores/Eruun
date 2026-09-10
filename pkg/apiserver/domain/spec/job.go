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

	"k8s.io/apimachinery/pkg/api/resource"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	HarborVersion                    = "0.22.0"
	JobArchiveTimeoutSeconds         = 300
	EvaluationCollectionGraceSeconds = 360
)

// JobSpec describes one standalone execution. Type-specific inputs are not Traits.
type JobSpec struct {
	Name         string           `json:"name"`
	Type         string           `json:"type"`
	Spec         json.RawMessage  `json:"spec"`
	Traits       Traits           `json:"traits,omitempty"`
	ResultPolicy *JobResultPolicy `json:"resultPolicy,omitempty"`
}

type CommandJobSpec struct {
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	Args           []string `json:"args,omitempty"`
	TimeoutSeconds int64    `json:"timeoutSeconds,omitempty"`
}

type AgentEvaluationSpec struct {
	Framework        string          `json:"framework"`
	FrameworkVersion string          `json:"frameworkVersion"`
	DatasetID        string          `json:"datasetId"`
	Agent            EvaluationAgent `json:"agent"`
	Options          HarborOptions   `json:"options,omitempty"`
	TimeoutSeconds   int64           `json:"timeoutSeconds,omitempty"`
}

type EvaluationAgent struct {
	Name        string            `json:"name"`
	Model       string            `json:"model,omitempty"`
	Credentials []EnvVarSecretRef `json:"credentials,omitempty"`
}

type EnvVarSecretRef struct {
	Name         string             `json:"name"`
	SecretKeyRef SecretSelectorSpec `json:"secretKeyRef"`
}

type HarborOptions struct {
	Attempts    int `json:"attempts,omitempty"`
	Concurrency int `json:"concurrency,omitempty"`
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
	if len(j.Spec) == 0 || bytes.Equal(bytes.TrimSpace(j.Spec), []byte("null")) {
		return fmt.Errorf("spec is required")
	}
	if err := validateJobTraits(j.Traits, j.Type == "agent_evaluation"); err != nil {
		return err
	}
	switch j.Type {
	case "command":
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
		if j.ResultPolicy != nil {
			return fmt.Errorf("resultPolicy applies to agent_evaluation")
		}
		j.Spec, _ = json.Marshal(command)
	case "agent_evaluation":
		var evaluation AgentEvaluationSpec
		if err := DecodeJobJSON(j.Spec, &evaluation); err != nil {
			return fmt.Errorf("agent_evaluation spec: %w", err)
		}
		if evaluation.Framework != "harbor" || evaluation.FrameworkVersion != HarborVersion {
			return fmt.Errorf("supported framework is harbor %s", HarborVersion)
		}
		if len(evaluation.DatasetID) != 36 {
			return fmt.Errorf("datasetId must reference an uploaded task package")
		}
		// Built-in adapters, never arbitrary Python import paths or user commands.
		switch evaluation.Agent.Name {
		case "terminus-2", "codex", "claude-code", "oracle":
		default:
			return fmt.Errorf("unsupported Harbor agent")
		}
		if evaluation.Agent.Name != "oracle" && (strings.TrimSpace(evaluation.Agent.Model) == "" || len(evaluation.Agent.Model) > 256) {
			return fmt.Errorf("model is required for this agent")
		}
		seen := map[string]bool{}
		for _, credential := range evaluation.Agent.Credentials {
			switch credential.Name {
			case "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY", "OPENROUTER_API_KEY", "AZURE_API_KEY":
			default:
				return fmt.Errorf("unsupported credential environment name")
			}
			if len(validation.IsEnvVarName(credential.Name)) != 0 || strings.HasPrefix(credential.Name, "ERUUN_") || strings.HasPrefix(credential.Name, "POD_") || seen[credential.Name] {
				return fmt.Errorf("invalid or duplicate credential environment name")
			}
			if len(validation.IsDNS1123Subdomain(credential.SecretKeyRef.Name)) != 0 || len(validation.IsConfigMapKey(credential.SecretKeyRef.Key)) != 0 {
				return fmt.Errorf("invalid credential Secret reference")
			}
			seen[credential.Name] = true
		}
		if evaluation.Options.Attempts == 0 {
			evaluation.Options.Attempts = 1
		}
		if evaluation.Options.Concurrency == 0 {
			evaluation.Options.Concurrency = 1
		}
		if evaluation.Options.Attempts < 1 || evaluation.Options.Attempts > 10 || evaluation.Options.Concurrency < 1 || evaluation.Options.Concurrency > 16 {
			return fmt.Errorf("attempts must be 1..10 and concurrency 1..16")
		}
		if evaluation.TimeoutSeconds == 0 {
			evaluation.TimeoutSeconds = 3600
		}
		if evaluation.TimeoutSeconds < 60 || evaluation.TimeoutSeconds > 86400 {
			return fmt.Errorf("evaluation timeoutSeconds must be 60..86400")
		}
		if j.ResultPolicy != nil {
			if err := j.ResultPolicy.Validate(); err != nil {
				return err
			}
		}
		j.Spec, _ = json.Marshal(evaluation)
	default:
		return fmt.Errorf("type must be command or agent_evaluation")
	}
	if j.Traits.Resources == nil {
		j.Traits.Resources = &ResourceTraitsSpec{CPU: "1", Memory: "2Gi", CPULimit: "2", MemoryLimit: "4Gi"}
	}
	if j.Traits.Resources.CPULimit == "" {
		j.Traits.Resources.CPULimit = j.Traits.Resources.CPU
	}
	if j.Traits.Resources.MemoryLimit == "" {
		j.Traits.Resources.MemoryLimit = j.Traits.Resources.Memory
	}
	return nil
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

func validateJobTraits(t Traits, evaluation bool) error {
	if len(t.Init)+len(t.Sidecar)+len(t.Ingress)+len(t.Service)+len(t.RBAC)+len(t.Probes)+len(t.TargetWorkEnv) > 0 || t.Share != nil || t.Rollout != nil {
		return fmt.Errorf("unsupported standalone Job trait")
	}
	if evaluation && (len(t.Storage)+len(t.Envs)+len(t.EnvFrom) > 0 || t.SecurityPolicy != nil) {
		return fmt.Errorf("evaluation supports resources only; use agent.credentials for Secrets")
	}
	for _, storage := range t.Storage {
		if storage.TmpCreate || storage.Size != "" || storage.StorageClass != "" {
			return fmt.Errorf("Job storage must reference an existing resource")
		}
	}
	if t.Resources != nil {
		if t.Resources.CPU == "" || t.Resources.Memory == "" {
			return fmt.Errorf("resources require cpu and memory requests")
		}
		for _, pair := range [][2]string{{t.Resources.CPU, t.Resources.CPULimit}, {t.Resources.Memory, t.Resources.MemoryLimit}} {
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
		if t.Resources.GPU != "" {
			return fmt.Errorf("GPU Job resources are not supported in the first version")
		}
	}
	return nil
}

// JobsRuntimeConfig is administrator-owned configuration loaded from a mounted Secret.
type JobsRuntimeConfig struct {
	RunnerImage  string            `json:"runnerImage"`
	APIURL       string            `json:"apiURL"`
	MinIO        *MinIOConfig      `json:"minio,omitempty"`
	RunnerEgress []JobRunnerEgress `json:"runnerEgress"`
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
