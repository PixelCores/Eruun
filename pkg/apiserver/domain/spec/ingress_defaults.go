package spec

import "strings"

// ResolveIngressBackend returns backend defaults without changing the input spec.
// defaultServiceName is the generated component Service name. Callers retain
// their own validation of ambiguous service references.
func ResolveIngressBackend(backend IngressRoute, defaultServiceName string, services []ServiceTraitSpec, ports []Ports) IngressRoute {
	if strings.TrimSpace(backend.ServiceName) != "" && backend.ServicePort > 0 {
		return backend
	}
	serviceName, servicePort := defaultServiceName, int32(0)
	var selected *ServiceTraitSpec
	for i := range services {
		if selected == nil {
			selected = &services[i]
		}
		accessType, _ := NormalizeServiceAccessType(services[i].Type)
		if accessType != ServiceAccessExternal {
			selected = &services[i]
			break
		}
	}
	if selected != nil {
		if name := strings.TrimSpace(selected.Name); name != "" {
			serviceName = name
		}
		servicePort = firstIngressServicePort(selected.Ports)
	} else {
		servicePort = firstIngressPropertyPort(ports)
	}
	if strings.TrimSpace(backend.ServiceName) == "" {
		backend.ServiceName = serviceName
	}
	if backend.ServicePort > 0 {
		return backend
	}

	name := strings.TrimSpace(backend.ServiceName)
	for _, service := range services {
		candidate := strings.TrimSpace(service.Name)
		if candidate == "" {
			candidate = defaultServiceName
		}
		if candidate == name {
			if port := firstIngressServicePort(service.Ports); port > 0 {
				backend.ServicePort = port
				return backend
			}
		}
	}
	if name == defaultServiceName {
		if port := firstIngressPropertyPort(ports); port > 0 {
			backend.ServicePort = port
			return backend
		}
	}
	if servicePort > 0 {
		backend.ServicePort = servicePort
	}
	return backend
}

func firstIngressServicePort(ports []ServicePortTraitSpec) int32 {
	for _, port := range ports {
		if port.Port > 0 {
			return port.Port
		}
	}
	return 0
}

func firstIngressPropertyPort(ports []Ports) int32 {
	for _, port := range ports {
		if port.Port > 0 {
			return port.Port
		}
	}
	return 0
}

// ApplyIngressRewriteAnnotations updates a caller-owned annotation map. Callers
// must copy the saved annotations before processing routes in declaration order.
func ApplyIngressRewriteAnnotations(annotations map[string]string, rewrite *RewritePolicy) map[string]string {
	if rewrite == nil {
		return annotations
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if rewrite.Replacement != "" {
		if _, exists := annotations["nginx.ingress.kubernetes.io/rewrite-target"]; !exists {
			annotations["nginx.ingress.kubernetes.io/rewrite-target"] = rewrite.Replacement
		}
	}
	rewriteType := strings.ToLower(rewrite.Type)
	if rewriteType == "regex" || rewriteType == "regexreplace" {
		annotations["nginx.ingress.kubernetes.io/use-regex"] = "true"
	}
	return annotations
}

// IngressPathType resolves the route, ingress, and rewrite defaults in that order.
// Path type values are normalized for whitespace and case at this shared boundary.
func IngressPathType(routePathType, defaultPathType string, annotations map[string]string) string {
	if pathType, ok := ingressPathType(routePathType); ok {
		return pathType
	}
	if pathType, ok := ingressPathType(defaultPathType); ok {
		return pathType
	}
	if strings.EqualFold(annotations["nginx.ingress.kubernetes.io/use-regex"], "true") {
		return "ImplementationSpecific"
	}
	return "Prefix"
}

func ingressPathType(value string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "prefix":
		return "Prefix", true
	case "exact":
		return "Exact", true
	case "implementationspecific", "implementation-specific":
		return "ImplementationSpecific", true
	default:
		return "", false
	}
}
