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

type IngressProcessor struct{}

func (p *IngressProcessor) Name() string { return "ingress" }

func (p *IngressProcessor) Process(ctx *TraitContext) (*TraitResult, error) {
	traits, ok := ctx.TraitData.([]spec.IngressTraitsSpec)
	if !ok {
		return nil, fmt.Errorf("ingress trait expects []spec.IngressTraitSpec, got %T", ctx.TraitData)
	}
	if len(traits) == 0 {
		return nil, nil
	}

	result := &TraitResult{}
	for idx, t := range traits {
		// Apply defaults
		if err := applyIngressDefaults(&t, ctx.Component, idx); err != nil {
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

func applyIngressDefaults(traitSpec *spec.IngressTraitsSpec, component *model.ApplicationComponent, idx int) error {
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
	traits := decodeComponentTraits(component)
	properties := decodeComponentProperties(component)
	defaultServiceName, defaultServicePort, err := resolveIngressDefaultService(component, traits, properties, ingressNeedsDefaultBackendName(traitSpec))
	if err != nil {
		return err
	}
	for i := range traitSpec.Routes {
		if strings.TrimSpace(traitSpec.Routes[i].Backend.ServiceName) == "" {
			traitSpec.Routes[i].Backend.ServiceName = defaultServiceName
		}
		if traitSpec.Routes[i].Backend.ServicePort <= 0 {
			if servicePort := resolveIngressBackendPort(component, traits, properties, traitSpec.Routes[i].Backend.ServiceName, defaultServicePort); servicePort > 0 {
				traitSpec.Routes[i].Backend.ServicePort = servicePort
			}
		}
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

func resolveIngressDefaultService(component *model.ApplicationComponent, traits *spec.Traits, properties *model.Properties, requireUnambiguous bool) (string, int32, error) {
	if component == nil {
		return "", 0, nil
	}

	if svcTrait, ok, err := selectIngressServiceTrait(component, traits, requireUnambiguous); err != nil {
		return "", 0, err
	} else if ok {
		serviceName := strings.TrimSpace(svcTrait.Name)
		if serviceName == "" {
			serviceName = naming.ServiceName(component.Name, component.ResourceNameKey())
		}
		return serviceName, firstServiceTraitPort(svcTrait), nil
	}

	defaultName := naming.ServiceName(component.Name, component.ResourceNameKey())
	if properties == nil {
		return defaultName, 0, nil
	}
	for _, port := range properties.Ports {
		if port.Port > 0 {
			return defaultName, port.Port, nil
		}
	}
	return defaultName, 0, nil
}

func resolveIngressBackendPort(component *model.ApplicationComponent, traits *spec.Traits, properties *model.Properties, serviceName string, fallbackPort int32) int32 {
	if component == nil {
		return fallbackPort
	}
	normalizedServiceName := strings.TrimSpace(serviceName)
	defaultServiceName := naming.ServiceName(component.Name, component.ResourceNameKey())
	if traits != nil {
		for _, svcTrait := range traits.Service {
			candidateName := strings.TrimSpace(svcTrait.Name)
			if candidateName == "" {
				candidateName = defaultServiceName
			}
			if candidateName == normalizedServiceName {
				if port := firstServiceTraitPort(svcTrait); port > 0 {
					return port
				}
			}
		}
	}
	if normalizedServiceName == defaultServiceName && properties != nil {
		for _, port := range properties.Ports {
			if port.Port > 0 {
				return port.Port
			}
		}
	}
	return fallbackPort
}

func selectIngressServiceTrait(component *model.ApplicationComponent, traits *spec.Traits, requireUnambiguous bool) (spec.ServiceTraitSpec, bool, error) {
	if component == nil || traits == nil || len(traits.Service) == 0 {
		return spec.ServiceTraitSpec{}, false, nil
	}

	var fallback *spec.ServiceTraitSpec
	var nonExternal []spec.ServiceTraitSpec
	for i := range traits.Service {
		current := &traits.Service[i]
		if fallback == nil {
			fallback = current
		}
		accessType, _ := spec.NormalizeServiceAccessType(current.Type)
		if accessType != spec.ServiceAccessExternal {
			nonExternal = append(nonExternal, *current)
		}
	}
	if requireUnambiguous && len(nonExternal) > 1 {
		return spec.ServiceTraitSpec{}, false, fmt.Errorf("ingress backend serviceName is required when component %q defines multiple non-external service traits", component.Name)
	}
	if len(nonExternal) > 0 {
		return nonExternal[0], true, nil
	}
	if fallback != nil {
		return *fallback, true, nil
	}
	return spec.ServiceTraitSpec{}, false, nil
}

func firstServiceTraitPort(serviceTrait spec.ServiceTraitSpec) int32 {
	for _, port := range serviceTrait.Ports {
		if port.Port > 0 {
			return port.Port
		}
	}
	return 0
}

func decodeComponentTraits(component *model.ApplicationComponent) *spec.Traits {
	if component == nil || component.Traits == nil {
		return nil
	}
	raw, err := json.Marshal(component.Traits)
	if err != nil || string(raw) == "{}" || string(raw) == "null" {
		return nil
	}
	var traits spec.Traits
	if err := json.Unmarshal(raw, &traits); err != nil {
		return nil
	}
	return &traits
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
		annotations = applyRewriteAnnotations(annotations, route.Rewrite)

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

func applyRewriteAnnotations(annotations map[string]string, rewrite *spec.RewritePolicy) map[string]string {
	if rewrite == nil {
		return annotations
	}
	if annotations == nil {
		annotations = make(map[string]string)
	}
	rewriteType := strings.ToLower(rewrite.Type)
	if rewrite.Replacement != "" {
		if _, exists := annotations["nginx.ingress.kubernetes.io/rewrite-target"]; !exists {
			annotations["nginx.ingress.kubernetes.io/rewrite-target"] = rewrite.Replacement
		}
	}
	if rewriteType == "regex" || rewriteType == "regexreplace" {
		annotations["nginx.ingress.kubernetes.io/use-regex"] = "true"
	}
	return annotations
}

func determinePathType(route model.IngressRoutes, ingressSpec *model.IngressTraitsSpec, annotations map[string]string) networkingv1.PathType {
	if route.PathType != "" {
		if pt, ok := parsePathType(route.PathType); ok {
			return pt
		}
	}
	if ingressSpec.DefaultPathType != "" {
		if pt, ok := parsePathType(ingressSpec.DefaultPathType); ok {
			return pt
		}
	}
	if val, ok := annotations["nginx.ingress.kubernetes.io/use-regex"]; ok && strings.EqualFold(val, "true") {
		return networkingv1.PathTypeImplementationSpecific
	}
	return networkingv1.PathTypePrefix
}

func parsePathType(value string) (networkingv1.PathType, bool) {
	switch strings.ToLower(value) {
	case "prefix":
		return networkingv1.PathTypePrefix, true
	case "exact":
		return networkingv1.PathTypeExact, true
	case "implementationspecific", "implementation-specific":
		return networkingv1.PathTypeImplementationSpecific, true
	default:
		return networkingv1.PathType(""), false
	}
}
