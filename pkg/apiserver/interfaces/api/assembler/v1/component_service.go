package v1

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func buildComponentServices(component *apisv1.ApplicationComponent) []apisv1.ComponentServiceInfo {
	if !componentDeploysService(component) {
		return nil
	}
	namespace := pickComponentNamespace(component.Namespace)
	if len(component.Traits.Service) > 0 {
		services := make([]apisv1.ComponentServiceInfo, 0, len(component.Traits.Service))
		for _, trait := range component.Traits.Service {
			name := strings.TrimSpace(trait.Name)
			if name == "" {
				name = naming.ServiceName(component.Name, componentResourceAppName(component))
			}
			accessType, _ := spec.NormalizeServiceAccessType(trait.Type)
			services = append(services, apisv1.ComponentServiceInfo{
				Name:         name,
				Namespace:    namespace,
				Type:         string(accessType),
				Headless:     trait.Headless,
				ExternalName: strings.TrimSpace(trait.ExternalName),
				Ports:        buildServicePortInfos(component.Name, trait.Ports),
			})
		}
		return services
	}
	if len(component.Properties.Ports) == 0 {
		return nil
	}
	return []apisv1.ComponentServiceInfo{
		{
			Name:      naming.ServiceName(component.Name, componentResourceAppName(component)),
			Namespace: namespace,
			Type:      string(spec.ServiceAccessInternal),
			Ports:     buildPropertyPortInfos(component.Name, component.Properties.Ports),
		},
	}
}

func buildServicePortInfos(componentName string, ports []spec.ServicePortTraitSpec) []apisv1.ComponentServicePortInfo {
	if len(ports) == 0 {
		return nil
	}
	result := make([]apisv1.ComponentServicePortInfo, 0, len(ports))
	for _, port := range ports {
		if port.Port <= 0 {
			continue
		}
		targetPort := port.TargetPort
		if targetPort <= 0 {
			targetPort = port.Port
		}
		protocol := strings.ToUpper(strings.TrimSpace(port.Protocol))
		if protocol == "" {
			protocol = "TCP"
		}
		result = append(result, apisv1.ComponentServicePortInfo{
			Name:       pickServicePortName(componentName, port.Name, port.Port),
			Port:       port.Port,
			TargetPort: targetPort,
			Protocol:   protocol,
		})
	}
	return result
}

func buildPropertyPortInfos(componentName string, ports []model.Ports) []apisv1.ComponentServicePortInfo {
	if len(ports) == 0 {
		return nil
	}
	result := make([]apisv1.ComponentServicePortInfo, 0, len(ports))
	for _, port := range ports {
		if port.Port <= 0 {
			continue
		}
		result = append(result, apisv1.ComponentServicePortInfo{
			Name:       pickServicePortName(componentName, "", port.Port),
			Port:       port.Port,
			TargetPort: port.Port,
			Protocol:   "TCP",
		})
	}
	return result
}

func pickServicePortName(componentName, explicitName string, port int32) string {
	if name := strings.TrimSpace(explicitName); name != "" {
		return name
	}
	base := utils.ToRFC1123Name(componentName)
	name := fmt.Sprintf("%s-%d", base, port)
	if len(name) > 15 {
		return fmt.Sprintf("p-%d", port)
	}
	return name
}

func buildComponentIngresses(component *apisv1.ApplicationComponent) []apisv1.ComponentIngressInfo {
	if !componentDeploysIngress(component) || len(component.Traits.Ingress) == 0 {
		return nil
	}
	defaultServiceName, defaultServicePort := resolveComponentIngressDefaultService(component)
	ingresses := make([]apisv1.ComponentIngressInfo, 0, len(component.Traits.Ingress))
	for i, trait := range component.Traits.Ingress {
		annotations, routes := buildIngressTraitDetails(trait, defaultServiceName, defaultServicePort)
		ingress := apisv1.ComponentIngressInfo{
			Name:             buildIngressResourceName(component.Name, componentResourceAppName(component), trait.Name, i),
			Namespace:        pickIngressNamespace(component.Namespace, trait.Namespace),
			IngressClassName: strings.TrimSpace(trait.IngressClassName),
			Annotations:      annotations,
			TLS:              append([]spec.IngressTLSConfig(nil), trait.TLS...),
			Routes:           routes,
		}
		ingresses = append(ingresses, ingress)
	}
	return ingresses
}

func buildIngressTraitDetails(trait spec.IngressTraitsSpec, defaultServiceName string, defaultServicePort int32) (map[string]string, []apisv1.ComponentIngressRouteInfo) {
	annotations := copyStringMap(trait.Annotations)
	if len(trait.Routes) == 0 {
		return annotations, nil
	}
	result := make([]apisv1.ComponentIngressRouteInfo, 0, len(trait.Routes))
	for _, route := range trait.Routes {
		annotations = applyIngressRewriteAnnotations(annotations, route.Rewrite)
		serviceName := strings.TrimSpace(route.Backend.ServiceName)
		if serviceName == "" {
			serviceName = defaultServiceName
		}
		servicePort := route.Backend.ServicePort
		if servicePort <= 0 {
			servicePort = defaultServicePort
		}
		if servicePort <= 0 {
			servicePort = 80
		}
		path := strings.TrimSpace(route.Path)
		if path == "" {
			path = "/"
		}
		pathType := determineIngressRoutePathType(route.PathType, trait.DefaultPathType, annotations)
		for _, host := range resolveIngressRouteHosts(route.Host, trait.Hosts) {
			result = append(result, apisv1.ComponentIngressRouteInfo{
				Host:        host,
				Path:        path,
				PathType:    pathType,
				ServiceName: serviceName,
				ServicePort: servicePort,
				Weight:      route.Backend.Weight,
				Headers:     copyStringMap(route.Backend.Headers),
				Rewrite:     route.Rewrite,
			})
		}
	}
	return annotations, result
}

func resolveIngressRouteHosts(routeHost string, ingressHosts []string) []string {
	if host := strings.TrimSpace(routeHost); host != "" {
		return []string{host}
	}
	hosts := make([]string, 0, len(ingressHosts))
	for _, host := range ingressHosts {
		if host = strings.TrimSpace(host); host != "" {
			hosts = append(hosts, host)
		}
	}
	if len(hosts) > 0 {
		return hosts
	}
	return []string{""}
}

func resolveComponentIngressDefaultService(component *apisv1.ApplicationComponent) (string, int32) {
	if component == nil {
		return "", 0
	}
	if idx, ok := selectServiceTraitForLink(component); ok {
		trait := component.Traits.Service[idx]
		name := strings.TrimSpace(trait.Name)
		if name == "" {
			name = naming.ServiceName(component.Name, componentResourceAppName(component))
		}
		return name, firstServiceTraitPort(trait)
	}
	name := naming.ServiceName(component.Name, componentResourceAppName(component))
	for _, port := range component.Properties.Ports {
		if port.Port > 0 {
			return name, port.Port
		}
	}
	return name, 0
}

func componentDeploysService(component *apisv1.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	switch component.ComponentType {
	case config.InstantJob, config.ScheduledJob, config.CloudJob:
		return false
	default:
		return true
	}
}

func componentDeploysIngress(component *apisv1.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	switch component.ComponentType {
	case config.ServerJob, config.StoreJob, config.InstantJob, config.ScheduledJob:
		return true
	default:
		return false
	}
}

func firstServiceTraitPort(serviceTrait spec.ServiceTraitSpec) int32 {
	for _, port := range serviceTrait.Ports {
		if port.Port > 0 {
			return port.Port
		}
	}
	return 0
}

func pickIngressName(componentName, explicitName string, idx int) string {
	if name := strings.TrimSpace(explicitName); name != "" {
		return name
	}
	base := strings.ToLower(strings.TrimSpace(componentName))
	if base == "" {
		base = "component"
	}
	suffix := "ingress"
	if idx > 0 {
		suffix = fmt.Sprintf("ingress-%d", idx+1)
	}
	return fmt.Sprintf("%s-%s", base, suffix)
}

func buildIngressResourceName(componentName, resourceAppName, explicitName string, idx int) string {
	return naming.IngressName(pickIngressName(componentName, explicitName, idx), resourceAppName)
}

func pickIngressNamespace(componentNamespace, _ string) string {
	return pickComponentNamespace(componentNamespace)
}

func applyIngressRewriteAnnotations(annotations map[string]string, rewrite *spec.RewritePolicy) map[string]string {
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

func determineIngressRoutePathType(routePathType, defaultPathType string, annotations map[string]string) string {
	if pathType, ok := normalizeIngressPathType(routePathType); ok {
		return pathType
	}
	if pathType, ok := normalizeIngressPathType(defaultPathType); ok {
		return pathType
	}
	if value, ok := annotations["nginx.ingress.kubernetes.io/use-regex"]; ok && strings.EqualFold(value, "true") {
		return "ImplementationSpecific"
	}
	return "Prefix"
}

func normalizeIngressPathType(value string) (string, bool) {
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

func selectServiceTraitForLink(component *apisv1.ApplicationComponent) (int, bool) {
	if component == nil || len(component.Traits.Service) == 0 {
		return -1, false
	}

	fallbackIndex := -1
	for i := range component.Traits.Service {
		if fallbackIndex == -1 {
			fallbackIndex = i
		}
		serviceType, _ := spec.NormalizeServiceAccessType(component.Traits.Service[i].Type)
		if serviceType != spec.ServiceAccessExternal {
			return i, true
		}
	}

	if fallbackIndex >= 0 {
		return fallbackIndex, true
	}
	return -1, false
}
