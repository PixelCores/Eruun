package v1

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func buildComponentExternalLinks(component *apisv1.ApplicationComponent) []apisv1.ExternalLink {
	if component == nil {
		return nil
	}
	ingressLinks := buildIngressLinks(component)
	if len(ingressLinks) > 0 {
		return ingressLinks
	}
	return buildServiceLinks(component)
}

func buildIngressLinks(component *apisv1.ApplicationComponent) []apisv1.ExternalLink {
	if !componentDeploysIngress(component) || len(component.Traits.Ingress) == 0 {
		return nil
	}
	links := make([]apisv1.ExternalLink, 0)
	seen := make(map[string]struct{})
	for _, ing := range component.Traits.Ingress {
		for _, route := range ing.Routes {
			path := strings.TrimSpace(route.Path)
			if path == "" {
				path = "/"
			}
			for _, host := range resolveIngressRouteHosts(route.Host, ing.Hosts) {
				if host == "" {
					host = "*"
				}
				value := host + path
				if _, ok := seen[value]; ok {
					continue
				}
				seen[value] = struct{}{}
				links = append(links, apisv1.ExternalLink{Type: "ingress", Value: value})
			}
		}
	}
	return links
}

func buildServiceLinks(component *apisv1.ApplicationComponent) []apisv1.ExternalLink {
	if !componentDeploysService(component) {
		return nil
	}
	if len(component.Properties.Ports) == 0 {
		return nil
	}
	namespace := strings.TrimSpace(component.Namespace)
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	serviceName := resolveServiceLinkName(component)
	if serviceName == "" || namespace == "" {
		return nil
	}
	ports := make([]string, 0, len(component.Properties.Ports))
	seen := make(map[int32]struct{})
	for _, port := range component.Properties.Ports {
		if port.Port == 0 {
			continue
		}
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
			Value: fmt.Sprintf("%s.%s.svc:%s", serviceName, namespace, strings.Join(ports, ",")),
		},
	}
}

func resolveServiceLinkName(component *apisv1.ApplicationComponent) string {
	if component == nil {
		return ""
	}

	componentName := strings.TrimSpace(component.Name)
	if componentName == "" {
		return ""
	}

	defaultServiceName := componentName
	if strings.TrimSpace(componentResourceAppName(component)) != "" {
		defaultServiceName = naming.ServiceName(componentName, componentResourceAppName(component))
	}

	traitIndex, ok := selectServiceTraitForLink(component)
	if !ok {
		return defaultServiceName
	}

	explicitName := strings.TrimSpace(component.Traits.Service[traitIndex].Name)
	if explicitName != "" {
		return explicitName
	}
	return defaultServiceName
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
