package spec

import (
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

// NormalizeServiceAccessType accepts only Eruun's exposure vocabulary; an empty
// value means internal.
func NormalizeServiceAccessType(raw string) (ServiceAccessType, bool) {
	switch ServiceAccessType(raw) {
	case "", ServiceAccessInternal:
		return ServiceAccessInternal, true
	case ServiceAccessNode, ServiceAccessPublic, ServiceAccessExternal:
		return ServiceAccessType(raw), true
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
