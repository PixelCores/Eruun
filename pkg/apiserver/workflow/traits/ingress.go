package traits

import (
	"encoding/json"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"

	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"

	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	networkingv1 "k8s.io/api/networking/v1"
)

func processIngress(ctx *TraitContext, traits []spec.IngressTraitsSpec) (*TraitResult, error) {
	if len(traits) == 0 {
		return nil, nil
	}

	properties := decodeComponentProperties(ctx.Component)
	result := &TraitResult{}
	for idx, t := range traits {
		// Apply defaults
		if err := applyIngressDefaults(&t, ctx.Component, ctx.componentTraits, properties, idx); err != nil {
			return nil, fmt.Errorf("apply ingress defaults trait[%d]: %w", idx, err)
		}

		// Build ingress resource
		obj, err := BuildIngress(&t)
		if err != nil {
			return nil, fmt.Errorf("build ingress trait[%d]: %w", idx, err)
		}
		result.AdditionalObjects = append(result.AdditionalObjects, obj)
	}
	return result, nil
}

func applyIngressDefaults(traitSpec *spec.IngressTraitsSpec, component *model.ApplicationComponent, traits *spec.Traits, properties *model.Properties, idx int) error {
	if traitSpec == nil {
		return nil
	}
	if traitSpec.Name == "" && component != nil {
		base := strings.ToLower(component.Name)
		suffix := "ingress"
		if idx > 0 {
			suffix = fmt.Sprintf("ingress-%d", idx+1)
		}
		traitSpec.Name = fmt.Sprintf("%s-%s", base, suffix)
	}

	if component != nil && component.Namespace != "" {
		traitSpec.Namespace = component.Namespace
	} else if traitSpec.Namespace == "" {
		traitSpec.Namespace = config.DefaultNamespace
	}

	if component == nil {
		return nil
	}
	var services []spec.ServiceTraitSpec
	if traits != nil {
		services = traits.Service
	}
	if ingressNeedsDefaultBackendName(traitSpec) {
		nonExternal := 0
		for _, service := range services {
			accessType, _ := spec.NormalizeServiceAccessType(service.Type)
			if accessType != spec.ServiceAccessExternal {
				nonExternal++
			}
		}
		if nonExternal > 1 {
			return fmt.Errorf("ingress backend serviceName is required when component %q defines multiple non-external service traits", component.Name)
		}
	}
	var ports []spec.Ports
	if properties != nil {
		ports = properties.Ports
	}
	defaultServiceName := naming.ServiceName(component.Name, component.ResourceNameKey())
	for i := range traitSpec.Routes {
		traitSpec.Routes[i].Backend = spec.ResolveIngressBackend(traitSpec.Routes[i].Backend, defaultServiceName, services, ports)
	}
	return nil
}

func ingressNeedsDefaultBackendName(traitSpec *spec.IngressTraitsSpec) bool {
	if traitSpec == nil {
		return false
	}
	for i := range traitSpec.Routes {
		if strings.TrimSpace(traitSpec.Routes[i].Backend.ServiceName) == "" {
			return true
		}
	}
	return false
}

func decodeComponentProperties(component *model.ApplicationComponent) *model.Properties {
	if component == nil || component.Properties == nil {
		return nil
	}
	raw, err := json.Marshal(component.Properties)
	if err != nil || string(raw) == "{}" || string(raw) == "null" {
		return nil
	}
	var properties model.Properties
	if err := json.Unmarshal(raw, &properties); err != nil {
		return nil
	}
	return &properties
}

func BuildIngress(ingressSpec *spec.IngressTraitsSpec) (*networkingv1.Ingress, error) {
	if ingressSpec == nil {
		return nil, fmt.Errorf("ingress spec is nil")
	}

	if len(ingressSpec.Routes) == 0 {
		return nil, fmt.Errorf("at least one route is required")
	}

	return buildIngressFromSpec(ingressSpec), nil
}

func buildIngressFromSpec(ingressSpec *model.IngressTraitsSpec) *networkingv1.Ingress {
	annotations := utils.CopyStringMap(ingressSpec.Annotations)
	ingSpec := networkingv1.IngressSpec{
		TLS: convertTLS(ingressSpec.TLS),
	}

	if ingressSpec.IngressClassName != "" {
		ingSpec.IngressClassName = utils.StringPtr(ingressSpec.IngressClassName)
	}

	hostRules := map[string]*networkingv1.HTTPIngressRuleValue{}
	var hostOrder []string

	for _, route := range ingressSpec.Routes {
		annotations = spec.ApplyIngressRewriteAnnotations(annotations, route.Rewrite)

		path := route.Path
		if path == "" {
			path = "/"
		}
		pathType := determinePathType(route, ingressSpec, annotations)
		backend := convertBackend(route.Backend)

		targetHosts := deriveHosts(ingressSpec, route)
		for _, host := range targetHosts {
			if _, ok := hostRules[host]; !ok {
				hostRules[host] = &networkingv1.HTTPIngressRuleValue{}
				hostOrder = append(hostOrder, host)
			}
			hostRules[host].Paths = append(hostRules[host].Paths, networkingv1.HTTPIngressPath{
				Path:     path,
				PathType: pointerToPathType(pathType),
				Backend:  backend,
			})
		}
	}

	for _, host := range hostOrder {
		rule := networkingv1.IngressRule{
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: hostRules[host],
			},
		}
		if host != "" {
			rule.Host = host
		}
		ingSpec.Rules = append(ingSpec.Rules, rule)
	}

	meta := metav1.ObjectMeta{
		Name:      ingressSpec.Name,
		Namespace: ingressSpec.Namespace,
		Labels:    naming.NormalizeLabelValues(ingressSpec.Label),
	}
	if len(annotations) > 0 {
		meta.Annotations = annotations
	}

	ing := &networkingv1.Ingress{
		ObjectMeta: meta,
		Spec:       ingSpec,
	}
	ing.SetGroupVersionKind(networkingv1.SchemeGroupVersion.WithKind("Ingress"))
	return ing
}

func convertTLS(src []model.IngressTLSConfig) []networkingv1.IngressTLS {
	if len(src) == 0 {
		return nil
	}
	res := make([]networkingv1.IngressTLS, 0, len(src))
	for _, tls := range src {
		hosts := append([]string(nil), tls.Hosts...)
		res = append(res, networkingv1.IngressTLS{
			Hosts:      hosts,
			SecretName: tls.SecretName,
		})
	}
	return res
}

func convertBackend(route model.IngressRoute) networkingv1.IngressBackend {
	port := route.ServicePort
	if port <= 0 {
		port = 80
	}
	return networkingv1.IngressBackend{
		Service: &networkingv1.IngressServiceBackend{
			Name: route.ServiceName,
			Port: networkingv1.ServiceBackendPort{
				Number: port,
			},
		},
	}
}

func deriveHosts(feature *model.IngressTraitsSpec, route spec.IngressRoutes) []string {
	if route.Host != "" {
		return []string{route.Host}
	}
	if len(feature.Hosts) > 0 {
		return append([]string(nil), feature.Hosts...)
	}
	return []string{""}
}

func pointerToPathType(pt networkingv1.PathType) *networkingv1.PathType {
	value := pt
	return &value
}

func determinePathType(route model.IngressRoutes, ingressSpec *model.IngressTraitsSpec, annotations map[string]string) networkingv1.PathType {
	return networkingv1.PathType(spec.IngressPathType(route.PathType, ingressSpec.DefaultPathType, annotations))
}
