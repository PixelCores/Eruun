package v1

import (
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

type ApplicationComponent struct {
	ID              int                       `json:"id"`
	AppID           string                    `json:"appId"`
	ResourceAppName string                    `json:"-"`
	Name            string                    `json:"name"`
	Namespace       string                    `json:"namespace"`
	Image           string                    `json:"image,omitempty"`
	Replicas        int32                     `json:"replicas"`
	ComponentType   config.JobType            `json:"type"`
	Properties      Properties                `json:"properties"`
	Traits          Traits                    `json:"traits"`
	Sidecars        []spec.SidecarTraitsSpec  `json:"sidecars,omitempty"`
	Services        []ComponentServiceInfo    `json:"services,omitempty"`
	Gateway         *ComponentGatewayInfo     `json:"gateway,omitempty"`
	ResourceConfigs []ComponentResourceConfig `json:"resourceConfigs,omitempty"`
	Credentials     []ComponentCredentialInfo `json:"credentials,omitempty"`
	Status          string                    `json:"status,omitempty"`
	LastAbnormal    string                    `json:"lastAbnormal,omitempty"`
	ExternalLinks   []ExternalLink            `json:"externalLinks,omitempty"`
	CreateTime      time.Time                 `json:"createTime"`
	UpdateTime      time.Time                 `json:"updateTime"`
}

type ComponentServiceInfo struct {
	Name         string                     `json:"name"`
	Namespace    string                     `json:"namespace,omitempty"`
	Type         string                     `json:"type,omitempty"`
	Headless     bool                       `json:"headless,omitempty"`
	ExternalName string                     `json:"externalName,omitempty"`
	Ports        []ComponentServicePortInfo `json:"ports,omitempty"`
}

type ComponentServicePortInfo struct {
	Name       string `json:"name,omitempty"`
	Port       int32  `json:"port"`
	TargetPort int32  `json:"targetPort,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
}

// ComponentGatewayInfo is the effective gateway route of a component under the
// current platform gateway configuration.
type ComponentGatewayInfo struct {
	Hosts  []string                    `json:"hosts"`
	Routes []ComponentGatewayRouteInfo `json:"routes"`
}

// ComponentGatewayRouteInfo is one route with its resolved Service and port;
// both are empty when the backend cannot be resolved.
type ComponentGatewayRouteInfo struct {
	Path    string `json:"path"`
	Match   string `json:"match"`
	Rewrite string `json:"rewrite,omitempty"`
	Timeout string `json:"timeout,omitempty"`
	Service string `json:"service,omitempty"`
	Port    int32  `json:"port,omitempty"`
}

type ComponentResourceConfig struct {
	Scope       string `json:"scope"`
	Name        string `json:"name,omitempty"`
	CPU         string `json:"cpu,omitempty"`
	Memory      string `json:"memory,omitempty"`
	CPULimit    string `json:"cpuLimit,omitempty"`
	MemoryLimit string `json:"memoryLimit,omitempty"`
	GPU         string `json:"gpu,omitempty"`
}

type ComponentCredentialInfo struct {
	Source     string `json:"source"`
	EnvName    string `json:"envName,omitempty"`
	SecretName string `json:"secretName"`
	Key        string `json:"key,omitempty"`
	Value      string `json:"value,omitempty"`
	Resolved   bool   `json:"resolved"`
}

type ExternalLink struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
