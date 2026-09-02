package conversion

import (
	"fmt"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func applyIngressTraits(ingresses []*networkingv1.Ingress, components []apis.CreateComponentRequest) []string {
	if len(ingresses) == 0 || len(components) == 0 {
		return nil
	}
	indexes := make(map[string][]int)
	for i := range components {
		name := strings.TrimSpace(components[i].Name)
		if name == "" {
			continue
		}
		indexes[name] = append(indexes[name], i)
	}
	var warnings []string
	for _, ingress := range ingresses {
		if ingress == nil {
			continue
		}
		name := strings.TrimSpace(ingress.Name)
		if name == "" {
			continue
		}
		trait, traitWarnings := buildIngressTrait(ingress)
		warnings = append(warnings, traitWarnings...)
		if trait == nil {
			continue
		}
		targets := indexes[name]
		if len(targets) == 0 {
			warnings = append(warnings, fmt.Sprintf("ingress %s has no matching component; skipped", name))
			continue
		}
		for _, idx := range targets {
			components[idx].Traits.Ingress = append(components[idx].Traits.Ingress, *trait)
		}
	}
	return warnings
}

func buildIngressTrait(ingress *networkingv1.Ingress) (*spec.IngressTraitsSpec, []string) {
	if ingress == nil {
		return nil, nil
	}
	var warnings []string
	trait := &spec.IngressTraitsSpec{
		Name:        ingress.Name,
		Namespace:   ingress.Namespace,
		Label:       filterReservedConvertedLabels(ingress.Labels),
		Annotations: utils.CopyStringMap(ingress.Annotations),
	}
	if ingress.Spec.IngressClassName != nil {
		trait.IngressClassName = *ingress.Spec.IngressClassName
	}
	for _, tls := range ingress.Spec.TLS {
		trait.TLS = append(trait.TLS, spec.IngressTLSConfig{
			SecretName: tls.SecretName,
			Hosts:      append([]string(nil), tls.Hosts...),
		})
	}
	for _, rule := range ingress.Spec.Rules {
		if rule.HTTP == nil || len(rule.HTTP.Paths) == 0 {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service == nil {
				warnings = append(warnings, fmt.Sprintf("ingress %s has non-service backend; skipped", ingress.Name))
				continue
			}
			servicePort := int32(0)
			if path.Backend.Service.Port.Number != 0 {
				servicePort = path.Backend.Service.Port.Number
			} else if path.Backend.Service.Port.Name != "" {
				warnings = append(warnings, fmt.Sprintf("ingress %s uses named service port %s; default port will be used", ingress.Name, path.Backend.Service.Port.Name))
			}
			route := spec.IngressRoutes{
				Path:     path.Path,
				PathType: "",
				Host:     rule.Host,
				Backend: spec.IngressRoute{
					ServiceName: path.Backend.Service.Name,
					ServicePort: servicePort,
				},
			}
			if path.PathType != nil {
				route.PathType = string(*path.PathType)
			}
			trait.Routes = append(trait.Routes, route)
		}
	}
	if len(trait.Routes) == 0 {
		return nil, append(warnings, fmt.Sprintf("ingress %s has no valid routes; skipped", ingress.Name))
	}
	return trait, warnings
}

func applyRBACPolicies(policies map[string][]spec.RBACPolicySpec, refs []componentRef, components []apis.CreateComponentRequest) []string {
	if len(policies) == 0 || len(refs) == 0 {
		return nil
	}
	used := make(map[string]bool, len(policies))
	for _, ref := range refs {
		if strings.TrimSpace(ref.serviceAccount) == "" {
			continue
		}
		key := buildNamespacedKey(ref.namespace, ref.serviceAccount)
		policySet := policies[key]
		if len(policySet) == 0 {
			continue
		}
		components[ref.index].Traits.RBAC = append(components[ref.index].Traits.RBAC, policySet...)
		used[key] = true
	}
	var warnings []string
	for key := range policies {
		if !used[key] {
			parts := strings.SplitN(key, "/", 2)
			name := key
			if len(parts) == 2 {
				name = fmt.Sprintf("%s/%s", parts[0], parts[1])
			}
			warnings = append(warnings, fmt.Sprintf("rbac serviceaccount %s has no matching workload; skipped", name))
		}
	}
	return warnings
}
