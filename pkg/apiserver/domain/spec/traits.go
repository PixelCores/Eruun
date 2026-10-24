package spec

import (
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// This package defines canonical value objects shared by DTO and Domain.
// It avoids duplicating identical semantic structures across layers.

// Traits is the aggregate of all attachable traits for a component.
type Traits struct {
	Evaluation *EvaluationTraitSpec `json:"eval,omitempty"`
	Init       []InitTraitSpec      `json:"init,omitempty"`
	Storage    []StorageTraitSpec   `json:"storage,omitempty"`
	Sidecar    []SidecarTraitsSpec  `json:"sidecar,omitempty"`
	// Gateway declares the component's traffic across the workspace boundary.
	Gateway   *GatewayTraitSpec   `json:"gateway,omitempty"`
	Service   []ServiceTraitSpec  `json:"service,omitempty"`
	EnvFrom   []EnvFromSourceSpec `json:"envFrom,omitempty"`
	Envs      []SimplifiedEnvSpec `json:"envs,omitempty"`
	Probes    []ProbeTraitsSpec   `json:"probes,omitempty"`
	NodePool  string              `json:"nodePool,omitempty"`
	Resources *ResourceTraitsSpec `json:"resources,omitempty"`
	// SecurityPolicy defines container-level security settings (securityContext).
	SecurityPolicy *SecurityPolicySpec `json:"securityPolicy,omitempty"`
	Share          *ShareTraitSpec     `json:"share,omitempty"`
	// Rollout controls workload-level update behavior for long-running workloads.
	Rollout *RolloutTraitSpec `json:"rollout,omitempty"`
}

// JobTraits is the restricted projection used by evaluation normalization and
// Runner rendering. Public Job steps use the same Traits as Application components.
type JobTraits struct {
	Evaluation     *EvaluationTraitSpec `json:"eval,omitempty"`
	Storage        []StorageTraitSpec   `json:"storage,omitempty"`
	EnvFrom        []EnvFromSourceSpec  `json:"envFrom,omitempty"`
	Envs           []SimplifiedEnvSpec  `json:"envs,omitempty"`
	Resources      *ResourceTraitsSpec  `json:"resources,omitempty"`
	SecurityPolicy *SecurityPolicySpec  `json:"securityPolicy,omitempty"`
}

// InitTraitSpec describes an init container with its own nested traits.
type InitTraitSpec struct {
	Name       string     `json:"name"`
	Image      string     `json:"image"`
	Traits     Traits     `json:"traits,omitempty"`
	Properties Properties `json:"properties,omitempty"`
}

// StorageTraitSpec describes storage characteristics for mounting into containers.
type StorageTraitSpec struct {
	Name        string `json:"name,omitempty"`
	Type        string `json:"type"`
	MountPath   string `json:"mountPath"`
	SubPath     string `json:"subPath,omitempty"`
	SubPathExpr string `json:"subPathExpr,omitempty"`
	ReadOnly    bool   `json:"readOnly,omitempty"`
	SourceName  string `json:"sourceName,omitempty"` // For ConfigMap/Secret volume sources

	// For "persistent" type
	TmpCreate    bool   `json:"tmpCreate,omitempty"`    // If true, use a StatefulSet volumeClaimTemplate.
	Size         string `json:"size,omitempty"`         // Used when creating a missing standalone or template PVC.
	ClaimName    string `json:"claimName,omitempty"`    // Standalone PVC name. If empty, defaults to Name.
	StorageClass string `json:"storageClass,omitempty"` // Used when creating a missing standalone or template PVC.
}

// SidecarTraitsSpec describes a sidecar container that may attach additional traits.
type SidecarTraitsSpec struct {
	Name    string            `json:"name"`
	Image   string            `json:"image"`
	Command []string          `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Traits  Traits            `json:"traits,omitempty"`
}

// EnvFromSourceSpec corresponds to a single corev1.EnvFromSource.
type EnvFromSourceSpec struct {
	Type       string `json:"type"`       // "secret" or "configMap"
	SourceName string `json:"sourceName"` // The name of the secret or configMap
}

// SimplifiedEnvSpec is the user-friendly, simplified way to define environment variables.
type SimplifiedEnvSpec struct {
	Name      string      `json:"name"`
	ValueFrom ValueSource `json:"valueFrom"`
}

// ValueSource defines the source for an environment variable's value.
// Only one of its fields may be set.
type ValueSource struct {
	Static *string                `json:"static,omitempty"`
	Secret *SecretSelectorSpec    `json:"secret,omitempty"`
	Config *ConfigMapSelectorSpec `json:"config,omitempty"`
	Field  *string                `json:"field,omitempty"`
}

// SecretSelectorSpec selects a key from a Secret.
type SecretSelectorSpec struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// ConfigMapSelectorSpec selects a key from a ConfigMap.
type ConfigMapSelectorSpec struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Properties describes container-level properties shared by traits.
type Properties struct {
	Ports                      []Ports                               `json:"ports"`
	Env                        map[string]string                     `json:"env"`
	Conf                       map[string]string                     `json:"conf"`
	Secret                     map[string]string                     `json:"secret"`
	Command                    []string                              `json:"command"`
	Labels                     map[string]string                     `json:"labels"`
	Schedule                   string                                `json:"schedule,omitempty"`
	StartTime                  int64                                 `json:"startTime,omitempty"`
	RunPolicy                  string                                `json:"runPolicy,omitempty"`
	JobRetryPolicy             *workflowconfig.JobRetryPolicy        `json:"jobRetryPolicy,omitempty"`
	FailurePolicy              *workflowconfig.WorkflowFailurePolicy `json:"failurePolicy,omitempty"`
	SuccessfulJobsHistoryLimit *int32                                `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     *int32                                `json:"failedJobsHistoryLimit,omitempty"`
}

type Ports struct {
	Port int32 `json:"port"`
}

// ProbeTraitsSpec defines a health check probe for a container.
type ProbeTraitsSpec struct {
	Type                string          `json:"type"` // "liveness", "readiness", or "startup"
	InitialDelaySeconds int32           `json:"initialDelaySeconds,omitempty"`
	PeriodSeconds       int32           `json:"periodSeconds,omitempty"`
	TimeoutSeconds      int32           `json:"timeoutSeconds,omitempty"`
	FailureThreshold    int32           `json:"failureThreshold,omitempty"`
	SuccessThreshold    int32           `json:"successThreshold,omitempty"`
	Exec                *ExecProbe      `json:"exec,omitempty"`
	HTTPGet             *HTTPGetProbe   `json:"httpGet,omitempty"`
	TCPSocket           *TCPSocketProbe `json:"tcpSocket,omitempty"`
}

// ExecProbe describes a command-line probe.
type ExecProbe struct {
	Command []string `json:"command"`
}

// HTTPGetProbe describes an HTTP probe.
type HTTPGetProbe struct {
	Path   string `json:"path"`
	Port   int    `json:"port"`
	Host   string `json:"host,omitempty"`
	Scheme string `json:"scheme,omitempty"`
}

// TCPSocketProbe describes a TCP socket probe.
type TCPSocketProbe struct {
	Port int    `json:"port"`
	Host string `json:"host,omitempty"`
}

// ResourceTraitsSpec defines CPU/Memory/GPU resources for a container.
// It is modeled as a trait so it can be attached to main, init, or sidecar containers (via nested traits).
// CPU/Memory are Kubernetes requests. CPULimit/MemoryLimit are Kubernetes limits
// and fall back to the request values when omitted for backward compatibility.
type ResourceTraitsSpec struct {
	CPU         string `json:"cpu,omitempty"`
	Memory      string `json:"memory,omitempty"`
	GPU         string `json:"gpu,omitempty"`
	CPULimit    string `json:"cpuLimit,omitempty"`
	MemoryLimit string `json:"memoryLimit,omitempty"`
}

// SecurityPolicySpec mirrors corev1.SecurityContext for container-level security controls.
type SecurityPolicySpec = corev1.SecurityContext

// ServiceTraitSpec describes a Service generated for the component.
type ServiceTraitSpec struct {
	Name string `json:"name,omitempty"`
	// Type is one of internal/node/public/external; empty means internal.
	Type         string                 `json:"type,omitempty"`
	ExternalName string                 `json:"externalName,omitempty"`
	Headless     bool                   `json:"headless,omitempty"`
	Selector     map[string]string      `json:"selector,omitempty"`
	Labels       map[string]string      `json:"labels,omitempty"`
	Ports        []ServicePortTraitSpec `json:"ports,omitempty"`
}

// ServicePortTraitSpec describes one service port entry.
type ServicePortTraitSpec struct {
	Name       string `json:"name,omitempty"`
	Port       int32  `json:"port"`
	TargetPort int32  `json:"targetPort,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
}

// ShareTraitSpec controls how shared resources are handled in a namespace.
type ShareTraitSpec struct {
	Strategy string `json:"strategy,omitempty"`
}

// RolloutTraitSpec controls how a long-running workload replaces existing pods.
type RolloutTraitSpec struct {
	Type          string                    `json:"type,omitempty"`
	RollingUpdate *RolloutRollingUpdateSpec `json:"rollingUpdate,omitempty"`
}

// RolloutRollingUpdateSpec holds strategy-specific rolling update parameters.
type RolloutRollingUpdateSpec struct {
	MaxSurge       *intstr.IntOrString `json:"maxSurge,omitempty"`
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
	Partition      *int32              `json:"partition,omitempty"`
}
