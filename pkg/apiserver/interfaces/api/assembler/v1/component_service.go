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
	ingresses := make([]apisv1.ComponentIngressInfo, 0, len(component.Traits.Ingress))
	for i, trait := range component.Traits.Ingress {
		annotations, routes := buildIngressTraitDetails(component, trait)
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

func buildIngressTraitDetails(component *apisv1.ApplicationComponent, trait spec.IngressTraitsSpec) (map[string]string, []apisv1.ComponentIngressRouteInfo) {
	annotations := copyStringMap(trait.Annotations)
	if len(trait.Routes) == 0 {
		return annotations, nil
	}
	defaultServiceName := naming.ServiceName(component.Name, componentResourceAppName(component))
	result := make([]apisv1.ComponentIngressRouteInfo, 0, len(trait.Routes))
	for _, route := range trait.Routes {
		annotations = spec.ApplyIngressRewriteAnnotations(annotations, route.Rewrite)
		backend := route.Backend
		backend.ServiceName = strings.TrimSpace(backend.ServiceName)
		backend = spec.ResolveIngressBackend(backend, defaultServiceName, component.Traits.Service, component.Properties.Ports)
		if backend.ServicePort <= 0 {
			backend.ServicePort = 80
		}
		path := strings.TrimSpace(route.Path)
		if path == "" {
			path = "/"
		}
		pathType := spec.IngressPathType(route.PathType, trait.DefaultPathType, annotations)
		for _, host := range resolveIngressRouteHosts(route.Host, trait.Hosts) {
			result = append(result, apisv1.ComponentIngressRouteInfo{
				Host:        host,
				Path:        path,
				PathType:    pathType,
				ServiceName: backend.ServiceName,
				ServicePort: backend.ServicePort,
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
