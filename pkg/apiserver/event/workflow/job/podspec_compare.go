package job

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func normalizePodSpecForCompare(spec corev1.PodSpec) corev1.PodSpec {
	normalized := spec.DeepCopy()
	if normalized == nil {
		return corev1.PodSpec{}
	}
	normalizePodSpecDefaults(normalized)
	return *normalized
}

func normalizePodSpecDefaults(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = corev1.RestartPolicyAlways
	}
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	if spec.TerminationGracePeriodSeconds == nil {
		spec.TerminationGracePeriodSeconds = utils.Int64Ptr(30)
	}
	if spec.EnableServiceLinks == nil {
		enableServiceLinks := true
		spec.EnableServiceLinks = &enableServiceLinks
	}
	normalizeContainerDefaults(spec.Containers)
	normalizeContainerDefaults(spec.InitContainers)
}

func normalizeContainerDefaults(containers []corev1.Container) {
	for i := range containers {
		if containers[i].TerminationMessagePath == "" {
			containers[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
		}
		if containers[i].TerminationMessagePolicy == "" {
			containers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		for j := range containers[i].Ports {
			if containers[i].Ports[j].Protocol == "" {
				containers[i].Ports[j].Protocol = corev1.ProtocolTCP
			}
		}
		normalizeResourceRequirements(&containers[i].Resources)
		normalizeProbeDefaults(containers[i].LivenessProbe)
		normalizeProbeDefaults(containers[i].ReadinessProbe)
		normalizeProbeDefaults(containers[i].StartupProbe)
	}
}

func normalizeResourceRequirements(resources *corev1.ResourceRequirements) {
	if resources == nil {
		return
	}
	resources.Requests = normalizeResourceList(resources.Requests)
	resources.Limits = normalizeResourceList(resources.Limits)
}

func normalizeResourceList(resources corev1.ResourceList) corev1.ResourceList {
	if len(resources) == 0 {
		return nil
	}
	normalized := corev1.ResourceList{}
	for name, value := range resources {
		if value.IsZero() {
			continue
		}
		normalized[name] = value
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func normalizeProbeDefaults(probe *corev1.Probe) {
	if probe == nil {
		return
	}
	if probe.TimeoutSeconds == 0 {
		probe.TimeoutSeconds = 1
	}
	if probe.PeriodSeconds == 0 {
		probe.PeriodSeconds = 10
	}
	if probe.SuccessThreshold == 0 {
		probe.SuccessThreshold = 1
	}
	if probe.FailureThreshold == 0 {
		probe.FailureThreshold = 3
	}
	if probe.HTTPGet != nil && probe.HTTPGet.Scheme == "" {
		probe.HTTPGet.Scheme = corev1.URISchemeHTTP
	}
}
