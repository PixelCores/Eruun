package v1

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func buildComponentExternalLinks(component *apisv1.ApplicationComponent) []apisv1.ExternalLink {
	if component == nil {
		return nil
	}
	if links := buildGatewayLinks(component.Gateway); len(links) > 0 {
		return links
	}
	return buildServiceLinks(component.Services)
}

// buildGatewayLinks lists the reachable host and path of every route.
func buildGatewayLinks(gateway *apisv1.ComponentGatewayInfo) []apisv1.ExternalLink {
	if gateway == nil {
		return nil
	}
	links := make([]apisv1.ExternalLink, 0, len(gateway.Hosts)*len(gateway.Routes))
	seen := make(map[string]struct{})
	for _, host := range gateway.Hosts {
		for _, route := range gateway.Routes {
			value := host + route.Path
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			links = append(links, apisv1.ExternalLink{Type: "gateway", Value: value})
		}
	}
	return links
}

func buildServiceLinks(services []apisv1.ComponentServiceInfo) []apisv1.ExternalLink {
	if len(services) == 0 {
		return nil
	}
	service := services[0]
	for _, candidate := range services {
		if candidate.Type != string(spec.ServiceAccessExternal) {
			service = candidate
			break
		}
	}
	if service.Name == "" || service.Namespace == "" {
		return nil
	}
	ports := make([]string, 0, len(service.Ports))
	seen := make(map[int32]struct{})
	for _, port := range service.Ports {
		if _, ok := seen[port.Port]; ok {
			continue
		}
		seen[port.Port] = struct{}{}
		ports = append(ports, fmt.Sprintf("%d", port.Port))
	}
	if len(ports) == 0 {
		return nil
	}
	return []apisv1.ExternalLink{
		{
			Type:  "svc",
			Value: fmt.Sprintf("%s.%s.svc:%s", service.Name, service.Namespace, strings.Join(ports, ",")),
		},
	}
}

func componentResourceAppName(component *apisv1.ApplicationComponent) string {
	if component == nil {
		return ""
	}
	if component.Traits.Share != nil {
		return strings.TrimSpace(component.Name)
	}
	if name := strings.TrimSpace(component.ResourceAppName); name != "" {
		return name
	}
	return component.AppID
}
