package config

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ServiceAccessType defines user-facing service exposure type.
type ServiceAccessType string

const (
	ServiceAccessInternal ServiceAccessType = "internal"
	ServiceAccessNode     ServiceAccessType = "node"
	ServiceAccessPublic   ServiceAccessType = "public"
	ServiceAccessExternal ServiceAccessType = "external"
)

// NormalizeServiceAccessType normalizes user input and keeps backward compatibility
// with native Kubernetes values (ClusterIP/NodePort/LoadBalancer/ExternalName).
func NormalizeServiceAccessType(raw string) (ServiceAccessType, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "", string(ServiceAccessInternal), strings.ToLower(string(corev1.ServiceTypeClusterIP)):
		return ServiceAccessInternal, true
	case string(ServiceAccessNode), strings.ToLower(string(corev1.ServiceTypeNodePort)):
		return ServiceAccessNode, true
	case string(ServiceAccessPublic), strings.ToLower(string(corev1.ServiceTypeLoadBalancer)):
		return ServiceAccessPublic, true
	case string(ServiceAccessExternal), strings.ToLower(string(corev1.ServiceTypeExternalName)):
		return ServiceAccessExternal, true
	default:
		return ServiceAccessInternal, false
	}
}

// ToKubeServiceType converts the user-facing service type to Kubernetes service type.
func ToKubeServiceType(accessType ServiceAccessType) corev1.ServiceType {
	switch accessType {
	case ServiceAccessNode:
		return corev1.ServiceTypeNodePort
	case ServiceAccessPublic:
		return corev1.ServiceTypeLoadBalancer
	case ServiceAccessExternal:
		return corev1.ServiceTypeExternalName
	default:
		return corev1.ServiceTypeClusterIP
	}
}

// ServiceAccessTypeFromKube converts Kubernetes service type to user-facing value.
func ServiceAccessTypeFromKube(serviceType corev1.ServiceType) ServiceAccessType {
	switch serviceType {
	case corev1.ServiceTypeNodePort:
		return ServiceAccessNode
	case corev1.ServiceTypeLoadBalancer:
		return ServiceAccessPublic
	case corev1.ServiceTypeExternalName:
		return ServiceAccessExternal
	default:
		return ServiceAccessInternal
	}
}
