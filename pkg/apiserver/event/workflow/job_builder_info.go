package workflow

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	domainspec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
)

func buildWorkloadInfo(jobType config.JobType, info interface{}, fallbackNamespace, fallbackName string) string {
	kind := domainspec.ResourceKind("resource")
	switch jobType {
	case config.JobDeploy:
		kind = domainspec.ResourceDeployment
	case config.JobDeployStore:
		kind = domainspec.ResourceStatefulSet
	case config.InstantJob:
		kind = domainspec.ResourceJob
	case config.ScheduledJob:
		switch v := info.(type) {
		case *batchv1.CronJob:
			return buildResourceInfo(domainspec.ResourceCronJob, v.Namespace, v.Name)
		case *batchv1.Job:
			return buildResourceInfo(domainspec.ResourceJob, v.Namespace, v.Name)
		}
	}
	if obj, ok := info.(metav1.Object); ok {
		return buildResourceInfo(kind, obj.GetNamespace(), obj.GetName())
	}
	return buildResourceInfo(kind, fallbackNamespace, fallbackName)
}

func buildConfigLikeInfo(kind domainspec.ResourceKind, info interface{}, fallbackNamespace, fallbackName string) string {
	switch v := info.(type) {
	case *jobinfo.ConfigMapInput:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *jobinfo.SecretInput:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *corev1.ConfigMap:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	case *corev1.Secret:
		return buildResourceInfo(kind, v.Namespace, v.Name)
	default:
		return buildResourceInfo(kind, fallbackNamespace, fallbackName)
	}
}

func buildServiceDeployInfo(svc *corev1.Service, fallbackName, fallbackNamespace string) string {
	if svc == nil {
		return buildResourceInfo(domainspec.ResourceService, fallbackNamespace, fallbackName)
	}
	name := svc.Name
	if name == "" {
		name = fallbackName
	}
	namespace := svc.Namespace
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
		return buildResourceInfo(domainspec.ResourceService, namespace, name)
	}
	return strings.Join(parts, "; ")
}

// buildRouteDeployInfo lists the effective host and path of every route.
func buildRouteDeployInfo(name string, gateway domainspec.GatewayTraitSpec, component *model.ApplicationComponent, cfg domainspec.GatewayConfig) string {
	hosts, err := domainspec.GatewayHosts(gateway, component.Name, component.Namespace, cfg)
	if err != nil || len(gateway.Routes) == 0 {
		return buildResourceInfo(domainspec.ResourceIngress, component.Namespace, name)
	}
	routes := make([]string, 0, len(hosts)*len(gateway.Routes))
	seen := make(map[string]struct{})
	for _, host := range hosts {
		for _, route := range gateway.Routes {
			addUniqueRoute(host+route.EffectivePath(), seen, &routes)
		}
	}
	return fmt.Sprintf("gateway: %s", strings.Join(routes, ", "))
}

func buildResourceInfo(kind domainspec.ResourceKind, namespace, name string) string {
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

func servicePortInfo(svc *corev1.Service) ([]string, []string) {
	if svc == nil {
		return nil, nil
	}
	portNums := make([]string, 0, len(svc.Spec.Ports))
	portDetails := make([]string, 0, len(svc.Spec.Ports))
	for _, port := range svc.Spec.Ports {
		portNum := fmt.Sprintf("%d", port.Port)
		portNums = append(portNums, portNum)
		proto := "TCP"
		if port.Protocol != "" {
			proto = string(port.Protocol)
		}
		name := port.Name
		target := ""
		if port.TargetPort.IntVal != 0 || port.TargetPort.StrVal != "" {
			target = port.TargetPort.String()
		}
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
