package workflow

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
)

func buildWorkloadInfo(jobType config.JobType, info interface{}, fallbackNamespace, fallbackName string) string {
	kind := config.ResourceKind("resource")
	switch jobType {
	case config.JobDeploy:
		kind = config.ResourceDeployment
	case config.JobDeployStore:
		kind = config.ResourceStatefulSet
	case config.CloudJob, config.JobDeployCloud:
		kind = config.ResourceCloudJob
	case config.InstantJob:
		kind = config.ResourceJob
	case config.ScheduledJob:
		switch v := info.(type) {
		case *batchv1.CronJob:
			return buildResourceInfo(config.ResourceCronJob, v.Namespace, v.Name)
		case *batchv1.Job:
			return buildResourceInfo(config.ResourceJob, v.Namespace, v.Name)
		}
	}
	if obj, ok := info.(metav1.Object); ok {
		return buildResourceInfo(kind, obj.GetNamespace(), obj.GetName())
	}
	return buildResourceInfo(kind, fallbackNamespace, fallbackName)
}

func buildConfigLikeInfo(kind config.ResourceKind, info interface{}, fallbackNamespace, fallbackName string) string {
	switch v := info.(type) {
	case *model.ConfigMapInput:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *model.SecretInput:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *corev1.ConfigMap:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *corev1.Secret:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	default:
		return buildResourceInfo(kind, fallbackNamespace, fallbackName)
	}
}

func buildServiceDeployInfo(svc *applyv1.ServiceApplyConfiguration, fallbackName, fallbackNamespace string) string {
	if svc == nil {
		return buildResourceInfo(config.ResourceService, fallbackNamespace, fallbackName)
	}
	name := derefString(svc.Name)
	if name == "" {
		name = fallbackName
	}
	namespace := derefString(svc.Namespace)
	if namespace == "" {
		namespace = fallbackNamespace
	}
	portNums, portDetails := servicePortInfo(svc)
	parts := make([]string, 0, 2)
	if name != "" && namespace != "" {
		fqdn := fmt.Sprintf("%s.%s.svc", name, namespace)
		if len(portNums) > 0 {
			fqdn = fmt.Sprintf("%s:%s", fqdn, strings.Join(portNums, ","))
		}
		parts = append(parts, fmt.Sprintf("svc: %s", fqdn))
	}
	if len(portDetails) > 0 {
		parts = append(parts, fmt.Sprintf("ports: %s", strings.Join(portDetails, ", ")))
	}
	if len(parts) == 0 {
		return buildResourceInfo(config.ResourceService, namespace, name)
	}
	return strings.Join(parts, "; ")
}

func buildIngressDeployInfo(ingress *networkingv1.Ingress, fallbackName, fallbackNamespace string) string {
	if ingress == nil {
		return buildResourceInfo(config.ResourceIngress, fallbackNamespace, fallbackName)
	}
	routes := ingressRoutes(ingress)
	if len(routes) == 0 {
		name := nameOrFallback(ingress.Name, fallbackName)
		namespace := nameOrFallback(ingress.Namespace, fallbackNamespace)
		return buildResourceInfo(config.ResourceIngress, namespace, name)
	}
	return fmt.Sprintf("ingress: %s", strings.Join(routes, ", "))
}

func buildResourceInfo(kind config.ResourceKind, namespace, name string) string {
	kindValue := strings.TrimSpace(string(kind))
	if kindValue == "" {
		kindValue = "resource"
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return kindValue
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return fmt.Sprintf("%s: %s", kindValue, name)
	}
	return fmt.Sprintf("%s: %s/%s", kindValue, namespace, name)
}

func servicePortInfo(svc *applyv1.ServiceApplyConfiguration) ([]string, []string) {
	if svc == nil || svc.Spec == nil {
		return nil, nil
	}
	portNums := make([]string, 0, len(svc.Spec.Ports))
	portDetails := make([]string, 0, len(svc.Spec.Ports))
	for _, port := range svc.Spec.Ports {
		if port.Port == nil {
			continue
		}
		portNum := fmt.Sprintf("%d", *port.Port)
		portNums = append(portNums, portNum)
		proto := "TCP"
		if port.Protocol != nil && string(*port.Protocol) != "" {
			proto = string(*port.Protocol)
		}
		name := derefString(port.Name)
		target := targetPortString(port.TargetPort)
		detail := portNum + "/" + proto
		if name != "" {
			detail = name + ":" + detail
		}
		if target != "" {
			detail += "->" + target
		}
		portDetails = append(portDetails, detail)
	}
	return portNums, portDetails
}

func targetPortString(port *intstr.IntOrString) string {
	if port == nil {
		return ""
	}
	return port.String()
}

func ingressRoutes(ingress *networkingv1.Ingress) []string {
	if ingress == nil {
		return nil
	}
	routes := make([]string, 0)
	seen := make(map[string]struct{})
	for _, rule := range ingress.Spec.Rules {
		host := strings.TrimSpace(rule.Host)
		if host == "" {
			host = "*"
		}
		if rule.HTTP == nil || len(rule.HTTP.Paths) == 0 {
			addUniqueRoute(host+"/", seen, &routes)
			continue
		}
		for _, path := range rule.HTTP.Paths {
			segment := strings.TrimSpace(path.Path)
			if segment == "" {
				segment = "/"
			}
			addUniqueRoute(host+segment, seen, &routes)
		}
	}
	return routes
}

func addUniqueRoute(value string, seen map[string]struct{}, routes *[]string) {
	if value == "" {
		return
	}
	if _, ok := seen[value]; ok {
		return
	}
	seen[value] = struct{}{}
	*routes = append(*routes, value)
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
