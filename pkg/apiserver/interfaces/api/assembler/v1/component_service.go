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
	namespace := component.Namespace
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

// buildComponentGateway returns the effective route under the platform gateway,
// or nil when the component declares none or the platform disables gateways.
// A backend that no longer resolves is shown without Service and port.
func buildComponentGateway(component *apisv1.ApplicationComponent, cfg spec.GatewayConfig) *apisv1.ComponentGatewayInfo {
	if component == nil || component.Traits.Gateway == nil || cfg.Provider == spec.GatewayProviderNone {
		return nil
	}
	if component.ComponentType != config.ServerJob && component.ComponentType != config.StoreJob {
		return nil
	}
	gateway := component.Traits.Gateway
	hosts := append([]string(nil), gateway.Hosts...)
	if len(hosts) == 0 {
		hosts = []string{spec.GatewayDefaultHost(component.Name, component.Namespace, cfg.Domain)}
	}
	routes := make([]apisv1.ComponentGatewayRouteInfo, 0, len(gateway.Routes))
	for _, route := range gateway.Routes {
		info := apisv1.ComponentGatewayRouteInfo{Path: route.EffectivePath(), Match: route.EffectiveMatch(), Rewrite: route.Rewrite, Timeout: route.Timeout}
		if service, port, err := spec.ResolveGatewayBackend(route.Backend, component.Traits.Service, component.Properties.Ports); err == nil {
			if service == "" {
				service = naming.ServiceName(component.Name, componentResourceAppName(component))
			}
			info.Service, info.Port = service, port
		}
		routes = append(routes, info)
	}
	return &apisv1.ComponentGatewayInfo{Hosts: hosts, Routes: routes}
}

func componentDeploysService(component *apisv1.ApplicationComponent) bool {
	if component == nil {
		return false
	}
	switch component.ComponentType {
	case config.InstantJob, config.ScheduledJob:
		return false
	default:
		return true
	}
}
