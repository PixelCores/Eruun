package conversion

import (
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

const (
	envFromTypeSecret    = "secret"
	envFromTypeConfigMap = "configMap"
)

type componentRef struct {
	index          int
	name           string
	namespace      string
	labels         map[string]string
	serviceAccount string
}

type claimTemplateInfo struct {
	size         string
	storageClass string
}

type workloadConversionInput struct {
	meta          metav1.ObjectMeta
	podSpec       corev1.PodSpec
	podLabels     map[string]string
	replicas      *int32
	claims        []corev1.PersistentVolumeClaim
	rollout       *spec.RolloutTraitSpec
	componentType config.JobType
	kind          string
	applyMetadata func(*apis.CreateComponentRequest)
}

func appendConvertedComponent(
	components []apis.CreateComponentRequest,
	refs []componentRef,
	comp *apis.CreateComponentRequest,
	labels map[string]string,
	serviceAccount string,
) ([]apis.CreateComponentRequest, []componentRef) {
	if comp == nil {
		return components, refs
	}
	components = append(components, *comp)
	refs = append(refs, componentRef{
		index:          len(components) - 1,
		name:           comp.Name,
		namespace:      comp.Namespace,
		labels:         labels,
		serviceAccount: serviceAccount,
	})
	return components, refs
}

func convertKubeYAMLToComponents(yamlText string) ([]apis.CreateComponentRequest, []string, error) {
	objects, err := decodeKubeObjects(yamlText)
	if err != nil {
		return nil, nil, err
	}
	return convertKubeObjectsToComponents(objects)
}

func decodeKubeObjects(yamlText string) ([]*unstructured.Unstructured, error) {
	decoder := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(yamlText), 4096)
	var objects []*unstructured.Unstructured
	for {
		var raw map[string]interface{}
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		if strings.EqualFold(obj.GetKind(), string(config.KubeKindList)) {
			items := expandListItems(obj)
			objects = append(objects, items...)
			continue
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

func expandListItems(obj *unstructured.Unstructured) []*unstructured.Unstructured {
	rawItems, ok := obj.Object["items"].([]interface{})
	if !ok {
		return nil
	}
	items := make([]*unstructured.Unstructured, 0, len(rawItems))
	for _, item := range rawItems {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		items = append(items, &unstructured.Unstructured{Object: m})
	}
	return items
}

func convertWorkloadObject(obj *unstructured.Unstructured, pvcLookup map[string]claimTemplateInfo) (*apis.CreateComponentRequest, map[string]string, string, []string, error) {
	input, err := buildWorkloadConversionInput(obj)
	if err != nil {
		return nil, nil, "", nil, err
	}
	comp, labels, saName, warns, err := convertWorkload(input.meta, input.podSpec, input.podLabels, input.replicas, input.claims, input.rollout, input.componentType, input.kind, pvcLookup)
	if err != nil || comp == nil {
		return comp, labels, saName, warns, err
	}
	if input.applyMetadata != nil {
		input.applyMetadata(comp)
	}
	return comp, labels, saName, warns, nil
}

func buildWorkloadConversionInput(obj *unstructured.Unstructured) (workloadConversionInput, error) {
	if obj == nil {
		return workloadConversionInput{}, fmt.Errorf("kubernetes workload object is nil")
	}
	switch strings.TrimSpace(obj.GetKind()) {
	case string(config.KubeKindStatefulSet):
		var sts appsv1.StatefulSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &sts); err != nil {
			return workloadConversionInput{}, err
		}
		return workloadConversionInput{
			meta:          sts.ObjectMeta,
			podSpec:       sts.Spec.Template.Spec,
			podLabels:     sts.Spec.Template.Labels,
			replicas:      sts.Spec.Replicas,
			claims:        sts.Spec.VolumeClaimTemplates,
			rollout:       rolloutTraitFromStatefulSetStrategy(sts.Spec.UpdateStrategy),
			componentType: config.StoreJob,
			kind:          string(config.KubeKindStatefulSet),
		}, nil
	case string(config.KubeKindDeployment):
		var deploy appsv1.Deployment
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &deploy); err != nil {
			return workloadConversionInput{}, err
		}
		return workloadConversionInput{
			meta:          deploy.ObjectMeta,
			podSpec:       deploy.Spec.Template.Spec,
			podLabels:     deploy.Spec.Template.Labels,
			replicas:      deploy.Spec.Replicas,
			rollout:       rolloutTraitFromDeploymentStrategy(deploy.Spec.Strategy),
			componentType: config.ServerJob,
			kind:          string(config.KubeKindDeployment),
		}, nil
	case string(config.KubeKindDaemonSet):
		var ds appsv1.DaemonSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &ds); err != nil {
			return workloadConversionInput{}, err
		}
		return workloadConversionInput{
			meta:          ds.ObjectMeta,
			podSpec:       ds.Spec.Template.Spec,
			podLabels:     ds.Spec.Template.Labels,
			componentType: config.ServerJob,
			kind:          string(config.KubeKindDaemonSet),
		}, nil
	case string(config.KubeKindJob):
		var job batchv1.Job
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &job); err != nil {
			return workloadConversionInput{}, err
		}
		return workloadConversionInput{
			meta:          job.ObjectMeta,
			podSpec:       job.Spec.Template.Spec,
			podLabels:     job.Spec.Template.Labels,
			componentType: config.InstantJob,
			kind:          string(config.KubeKindJob),
			applyMetadata: func(comp *apis.CreateComponentRequest) {
				applyJobMetadataToComponent(&job, comp)
			},
		}, nil
	case string(config.KubeKindCronJob):
		var cron batchv1.CronJob
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &cron); err != nil {
			return workloadConversionInput{}, err
		}
		return workloadConversionInput{
			meta:          cron.ObjectMeta,
			podSpec:       cron.Spec.JobTemplate.Spec.Template.Spec,
			podLabels:     cron.Spec.JobTemplate.Spec.Template.Labels,
			componentType: config.ScheduledJob,
			kind:          string(config.KubeKindCronJob),
			applyMetadata: func(comp *apis.CreateComponentRequest) {
				applyCronJobMetadataToComponent(&cron, comp)
			},
		}, nil
	default:
		return workloadConversionInput{}, fmt.Errorf("unsupported workload kind %s", strings.TrimSpace(obj.GetKind()))
	}
}

func convertService(obj *unstructured.Unstructured) (*corev1.Service, error) {
	var svc corev1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &svc); err != nil {
		return nil, err
	}
	return &svc, nil
}

func applyJobMetadataToComponent(job *batchv1.Job, comp *apis.CreateComponentRequest) {
	if job == nil || comp == nil {
		return
	}
	annotations := job.GetAnnotations()
	if annotations == nil {
		return
	}
	if raw := strings.TrimSpace(annotations[config.AnnotationJobRunPolicy]); raw != "" {
		if policy, ok := config.NormalizeJobRunPolicy(raw); ok {
			comp.Properties.RunPolicy = string(policy)
		}
	}
	if raw := strings.TrimSpace(annotations[config.AnnotationJobStartTime]); raw != "" {
		if value, err := strconv.ParseInt(raw, 10, 64); err == nil {
			comp.Properties.StartTime = value
		}
	}
}

func applyCronJobMetadataToComponent(cron *batchv1.CronJob, comp *apis.CreateComponentRequest) {
	if cron == nil || comp == nil {
		return
	}
	comp.Properties.Schedule = strings.TrimSpace(cron.Spec.Schedule)
	if cron.Spec.SuccessfulJobsHistoryLimit != nil {
		comp.Properties.SuccessfulJobsHistoryLimit = cron.Spec.SuccessfulJobsHistoryLimit
	}
	if cron.Spec.FailedJobsHistoryLimit != nil {
		comp.Properties.FailedJobsHistoryLimit = cron.Spec.FailedJobsHistoryLimit
	}
}

func convertConfigMap(obj *unstructured.Unstructured) (*apis.CreateComponentRequest, []string, error) {
	var cm corev1.ConfigMap
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &cm); err != nil {
		return nil, nil, err
	}
	name := strings.TrimSpace(cm.Name)
	if name == "" {
		return nil, []string{"configmap missing metadata.name; skipped"}, nil
	}
	shareTrait, shareWarnings := buildShareTrait(cm.Labels, nil)
	component := apis.CreateComponentRequest{
		Name:          name,
		Namespace:     cm.Namespace,
		ComponentType: config.ConfJob,
		Properties: apis.Properties{
			Conf: utils.CopyStringMap(cm.Data),
		},
	}
	if shareTrait != nil {
		component.Traits.Share = shareTrait
	}
	if len(cm.BinaryData) > 0 {
		if component.Properties.Conf == nil {
			component.Properties.Conf = make(map[string]string, len(cm.BinaryData))
		}
		for k, v := range cm.BinaryData {
			component.Properties.Conf[k] = base64.StdEncoding.EncodeToString(v)
		}
	}
	return &component, shareWarnings, nil
}

func convertSecret(obj *unstructured.Unstructured) (*apis.CreateComponentRequest, []string, error) {
	var secret corev1.Secret
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &secret); err != nil {
		return nil, nil, err
	}
	name := strings.TrimSpace(secret.Name)
	if name == "" {
		return nil, []string{"secret missing metadata.name; skipped"}, nil
	}
	shareTrait, shareWarnings := buildShareTrait(secret.Labels, nil)
	values := make(map[string]string, len(secret.Data)+len(secret.StringData))
	for k, v := range secret.Data {
		if !utf8.Valid(v) {
			return nil, nil, fmt.Errorf("secret %s key %s contains non-UTF-8 binary data", name, k)
		}
		values[k] = string(v)
	}
	for k, v := range secret.StringData {
		if _, exists := values[k]; exists {
			continue
		}
		if !utf8.ValidString(v) {
			return nil, nil, fmt.Errorf("secret %s key %s contains non-UTF-8 binary data", name, k)
		}
		values[k] = v
	}
	component := apis.CreateComponentRequest{
		Name:          name,
		Namespace:     secret.Namespace,
		ComponentType: config.SecretJob,
		Properties: apis.Properties{
			Secret: values,
		},
	}
	if shareTrait != nil {
		component.Traits.Share = shareTrait
	}
	return &component, shareWarnings, nil
}

func convertWorkload(meta metav1.ObjectMeta, podSpec corev1.PodSpec, podLabels map[string]string, replicas *int32, claims []corev1.PersistentVolumeClaim, rollout *spec.RolloutTraitSpec, componentType config.JobType, kind string, pvcLookup map[string]claimTemplateInfo) (*apis.CreateComponentRequest, map[string]string, string, []string, error) {
	name := strings.TrimSpace(meta.Name)
	if name == "" {
		return nil, nil, "", []string{fmt.Sprintf("%s missing metadata.name; skipped", strings.ToLower(kind))}, nil
	}
	if len(podSpec.Containers) == 0 {
		return nil, nil, "", []string{fmt.Sprintf("%s %s has no containers; skipped", strings.ToLower(kind), name)}, nil
	}

	labels := resolveWorkloadLabels(podLabels, meta.Labels)
	shareTrait, shareWarnings := buildShareTrait(labels, meta.Labels)
	warnings := append([]string(nil), shareWarnings...)
	mainContainer := podSpec.Containers[0]
	component := apis.CreateComponentRequest{
		Name:          name,
		Namespace:     meta.Namespace,
		ComponentType: componentType,
		Image:         mainContainer.Image,
		Replicas:      normalizeReplicas(replicas),
	}

	properties, traits, warns := buildComponentTraits(mainContainer, podSpec, claims, labels, name, meta.Namespace, pvcLookup, shareTrait)
	if rollout != nil {
		traits.Rollout = rollout
		traits = sanitizeTraits(traits)
	}
	component.Properties = properties
	component.Traits = traits
	warnings = append(warnings, warns...)

	return &component, labels, strings.TrimSpace(podSpec.ServiceAccountName), warnings, nil
}

func resolveWorkloadLabels(podLabels, metaLabels map[string]string) map[string]string {
	if len(podLabels) > 0 {
		return utils.CopyStringMap(podLabels)
	}
	return utils.CopyStringMap(metaLabels)
}

func filterReservedConvertedLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	reserved := applicationservice.ReservedComponentLabelKeys()
	filtered := make(map[string]string, len(labels))
	for key, value := range labels {
		if _, ok := reserved[key]; ok {
			continue
		}
		filtered[key] = value
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func normalizeReplicas(replicas *int32) int32 {
	if replicas == nil || *replicas <= 0 {
		return 1
	}
	return *replicas
}

func buildComponentTraits(container corev1.Container, podSpec corev1.PodSpec, claims []corev1.PersistentVolumeClaim, labels map[string]string, componentName, componentNamespace string, pvcLookup map[string]claimTemplateInfo, shareTrait *spec.ShareTraitSpec) (apis.Properties, apis.Traits, []string) {
	var warnings []string

	envs, traits, traitWarnings := buildContainerTraits(container, podSpec.Volumes, claims, componentNamespace, componentName, container.Name, pvcLookup)
	warnings = append(warnings, traitWarnings...)

	properties := apis.Properties{
		Ports:   portsFromContainer(container.Ports),
		Env:     envs,
		Command: container.Command,
		Labels:  filterReservedConvertedLabels(labels),
	}

	traits.TargetWorkEnv = applicationservice.CopyStringMap(podSpec.NodeSelector)
	if shareTrait != nil {
		traits.Share = shareTrait
	}

	initTraits, initWarnings := convertInitContainers(podSpec.InitContainers, podSpec.Volumes, claims, componentNamespace, componentName, pvcLookup)
	warnings = append(warnings, initWarnings...)
	if len(initTraits) > 0 {
		traits.Init = initTraits
	}

	sidecarTraits, sidecarWarnings := convertSidecarContainers(podSpec.Containers[1:], podSpec.Volumes, claims, componentNamespace, componentName, pvcLookup)
	warnings = append(warnings, sidecarWarnings...)
	if len(sidecarTraits) > 0 {
		traits.Sidecar = sidecarTraits
	}

	return sanitizeProperties(properties), sanitizeTraits(traits), warnings
}

func sanitizeProperties(props apis.Properties) apis.Properties {
	if len(props.Ports) == 0 {
		props.Ports = nil
	}
	if len(props.Env) == 0 {
		props.Env = nil
	}
	if len(props.Command) == 0 {
		props.Command = nil
	}
	if len(props.Labels) == 0 {
		props.Labels = nil
	}
	return props
}

func sanitizeTraits(traits apis.Traits) apis.Traits {
	if len(traits.Storage) == 0 {
		traits.Storage = nil
	}
	if len(traits.Service) == 0 {
		traits.Service = nil
	}
	if len(traits.EnvFrom) == 0 {
		traits.EnvFrom = nil
	}
	if len(traits.Envs) == 0 {
		traits.Envs = nil
	}
	if len(traits.Probes) == 0 {
		traits.Probes = nil
	}
	if len(traits.TargetWorkEnv) == 0 {
		traits.TargetWorkEnv = nil
	}
	if len(traits.Init) == 0 {
		traits.Init = nil
	}
	if len(traits.Sidecar) == 0 {
		traits.Sidecar = nil
	}
	if traits.Resources != nil && traits.Resources.CPU == "" && traits.Resources.Memory == "" && traits.Resources.CPULimit == "" && traits.Resources.MemoryLimit == "" && traits.Resources.GPU == "" {
		traits.Resources = nil
	}
	if traits.SecurityPolicy != nil && isSecurityContextEmpty(traits.SecurityPolicy) {
		traits.SecurityPolicy = nil
	}
	if traits.Share != nil && strings.TrimSpace(traits.Share.Strategy) == "" {
		traits.Share = nil
	}
	if traits.Rollout != nil && rolloutTraitEmpty(traits.Rollout) {
		traits.Rollout = nil
	}
	return traits
}

func rolloutTraitFromDeploymentStrategy(strategy appsv1.DeploymentStrategy) *spec.RolloutTraitSpec {
	if strategy.Type == "" && strategy.RollingUpdate == nil {
		return nil
	}
	rollout := &spec.RolloutTraitSpec{Type: string(strategy.Type)}
	if rollout.Type == "" && strategy.RollingUpdate != nil {
		rollout.Type = string(appsv1.RollingUpdateDeploymentStrategyType)
	}
	if rollout.Type == string(appsv1.RollingUpdateDeploymentStrategyType) &&
		(strategy.RollingUpdate == nil ||
			strategy.RollingUpdate.MaxSurge == nil ||
			strategy.RollingUpdate.MaxUnavailable == nil) {
		return nil
	}
	if strategy.RollingUpdate != nil {
		rollout.RollingUpdate = &spec.RolloutRollingUpdateSpec{
			MaxSurge:       copyIntOrString(strategy.RollingUpdate.MaxSurge),
			MaxUnavailable: copyIntOrString(strategy.RollingUpdate.MaxUnavailable),
		}
	}
	return rollout
}

func rolloutTraitFromStatefulSetStrategy(strategy appsv1.StatefulSetUpdateStrategy) *spec.RolloutTraitSpec {
	if strategy.Type == "" && strategy.RollingUpdate == nil {
		return nil
	}
	rollout := &spec.RolloutTraitSpec{Type: string(strategy.Type)}
	if rollout.Type == "" && strategy.RollingUpdate != nil {
		rollout.Type = string(appsv1.RollingUpdateStatefulSetStrategyType)
	}
	if strategy.RollingUpdate != nil {
		rollout.RollingUpdate = &spec.RolloutRollingUpdateSpec{
			Partition:      copyInt32(strategy.RollingUpdate.Partition),
			MaxUnavailable: copyIntOrString(strategy.RollingUpdate.MaxUnavailable),
		}
	}
	return rollout
}

func rolloutTraitEmpty(rollout *spec.RolloutTraitSpec) bool {
	if rollout == nil {
		return true
	}
	if strings.TrimSpace(rollout.Type) != "" {
		return false
	}
	if rollout.RollingUpdate == nil {
		return true
	}
	return rollout.RollingUpdate.MaxSurge == nil &&
		rollout.RollingUpdate.MaxUnavailable == nil &&
		rollout.RollingUpdate.Partition == nil
}

func copyIntOrString(value *intstr.IntOrString) *intstr.IntOrString {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func convertSecurityPolicy(ctx *corev1.SecurityContext) *spec.SecurityPolicySpec {
	if ctx == nil {
		return nil
	}
	return ctx.DeepCopy()
}

func isSecurityContextEmpty(ctx *corev1.SecurityContext) bool {
	if ctx == nil {
		return true
	}
	return reflect.DeepEqual(*ctx, corev1.SecurityContext{})
}

func portsFromContainer(ports []corev1.ContainerPort) []spec.Ports {
	if len(ports) == 0 {
		return nil
	}
	result := make([]spec.Ports, 0, len(ports))
	seen := make(map[int32]struct{})
	for _, port := range ports {
		if port.ContainerPort == 0 {
			continue
		}
		if _, ok := seen[port.ContainerPort]; ok {
			continue
		}
		seen[port.ContainerPort] = struct{}{}
		result = append(result, spec.Ports{Port: port.ContainerPort})
	}
	return result
}

func splitEnvVars(envs []corev1.EnvVar, componentName string) (map[string]string, []spec.SimplifiedEnvSpec, []string) {
	if len(envs) == 0 {
		return nil, nil, nil
	}
	static := make(map[string]string)
	var valueFrom []spec.SimplifiedEnvSpec
	var warnings []string
	for _, env := range envs {
		if env.Name == "" {
			warnings = append(warnings, fmt.Sprintf("env name missing in component %s; skipped", componentName))
			continue
		}
		if env.ValueFrom == nil {
			static[env.Name] = env.Value
			continue
		}
		valueSource, ok := buildValueSource(env)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("env %s in component %s uses unsupported valueFrom; skipped", env.Name, componentName))
			continue
		}
		valueFrom = append(valueFrom, spec.SimplifiedEnvSpec{
			Name:      env.Name,
			ValueFrom: valueSource,
		})
	}
	return static, valueFrom, warnings
}

func buildValueSource(env corev1.EnvVar) (spec.ValueSource, bool) {
	if env.ValueFrom == nil {
		return spec.ValueSource{}, false
	}
	if env.ValueFrom.SecretKeyRef != nil {
		return spec.ValueSource{
			Secret: &spec.SecretSelectorSpec{
				Name: env.ValueFrom.SecretKeyRef.Name,
				Key:  env.ValueFrom.SecretKeyRef.Key,
			},
		}, true
	}
	if env.ValueFrom.ConfigMapKeyRef != nil {
		return spec.ValueSource{
			Config: &spec.ConfigMapSelectorSpec{
				Name: env.ValueFrom.ConfigMapKeyRef.Name,
				Key:  env.ValueFrom.ConfigMapKeyRef.Key,
			},
		}, true
	}
	if env.ValueFrom.FieldRef != nil {
		field := env.ValueFrom.FieldRef.FieldPath
		return spec.ValueSource{Field: &field}, true
	}
	return spec.ValueSource{}, false
}

func convertEnvFromSources(sources []corev1.EnvFromSource, componentName string) ([]spec.EnvFromSourceSpec, []string) {
	if len(sources) == 0 {
		return nil, nil
	}
	var (
		result   []spec.EnvFromSourceSpec
		warnings []string
	)
	for _, src := range sources {
		if src.SecretRef != nil {
			name := strings.TrimSpace(src.SecretRef.Name)
			if name == "" {
				warnings = append(warnings, fmt.Sprintf("envFrom secret in component %s missing name; skipped", componentName))
				continue
			}
			result = append(result, spec.EnvFromSourceSpec{Type: envFromTypeSecret, SourceName: name})
		}
		if src.ConfigMapRef != nil {
			name := strings.TrimSpace(src.ConfigMapRef.Name)
			if name == "" {
				warnings = append(warnings, fmt.Sprintf("envFrom configMap in component %s missing name; skipped", componentName))
				continue
			}
			result = append(result, spec.EnvFromSourceSpec{Type: envFromTypeConfigMap, SourceName: name})
		}
	}
	return result, warnings
}

func convertProbes(container corev1.Container) []spec.ProbeTraitsSpec {
	var probes []spec.ProbeTraitsSpec
	appendProbe(&probes, container.LivenessProbe, "liveness")
	appendProbe(&probes, container.ReadinessProbe, "readiness")
	appendProbe(&probes, container.StartupProbe, "startup")
	return probes
}

func convertResourceTraits(container corev1.Container) *spec.ResourceTraitsSpec {
	cpuRequest := selectRequestResourceQuantity(container.Resources, corev1.ResourceCPU)
	cpuLimit := selectLimitResourceQuantity(container.Resources, corev1.ResourceCPU)
	memoryRequest := selectRequestResourceQuantity(container.Resources, corev1.ResourceMemory)
	memoryLimit := selectLimitResourceQuantity(container.Resources, corev1.ResourceMemory)
	gpu := selectResourceQuantity(container.Resources, corev1.ResourceName(config.ResourceNvidiaGPU))
	if cpuRequest == "" && cpuLimit == "" && memoryRequest == "" && memoryLimit == "" && gpu == "" {
		return nil
	}
	resources := &spec.ResourceTraitsSpec{
		CPU:    cpuRequest,
		Memory: memoryRequest,
		GPU:    gpu,
	}
	if cpuLimit != "" && cpuLimit != cpuRequest {
		resources.CPULimit = cpuLimit
	}
	if memoryLimit != "" && memoryLimit != memoryRequest {
		resources.MemoryLimit = memoryLimit
	}
	return resources
}

func selectResourceQuantity(req corev1.ResourceRequirements, name corev1.ResourceName) string {
	if qty := selectLimitResourceQuantity(req, name); qty != "" {
		return qty
	}
	return selectRequestResourceQuantity(req, name)
}

func selectLimitResourceQuantity(req corev1.ResourceRequirements, name corev1.ResourceName) string {
	if qty, ok := req.Limits[name]; ok && !qty.IsZero() {
		return qty.String()
	}
	return ""
}

func selectRequestResourceQuantity(req corev1.ResourceRequirements, name corev1.ResourceName) string {
	if qty, ok := req.Requests[name]; ok && !qty.IsZero() {
		return qty.String()
	}
	return ""
}

func appendProbe(target *[]spec.ProbeTraitsSpec, probe *corev1.Probe, probeType string) {
	if probe == nil {
		return
	}
	if probe.Exec == nil && probe.HTTPGet == nil && probe.TCPSocket == nil {
		return
	}
	specProbe := spec.ProbeTraitsSpec{
		Type:                probeType,
		InitialDelaySeconds: probe.InitialDelaySeconds,
		PeriodSeconds:       probe.PeriodSeconds,
		TimeoutSeconds:      probe.TimeoutSeconds,
		FailureThreshold:    probe.FailureThreshold,
		SuccessThreshold:    probe.SuccessThreshold,
	}
	if probe.Exec != nil {
		specProbe.Exec = &spec.ExecProbe{Command: probe.Exec.Command}
	}
	if probe.HTTPGet != nil {
		port := probe.HTTPGet.Port.IntValue()
		if port == 0 && probe.HTTPGet.Port.Type == intstr.String {
			port = 0
		}
		specProbe.HTTPGet = &spec.HTTPGetProbe{
			Path:   probe.HTTPGet.Path,
			Port:   port,
			Host:   probe.HTTPGet.Host,
			Scheme: string(probe.HTTPGet.Scheme),
		}
	}
	if probe.TCPSocket != nil {
		specProbe.TCPSocket = &spec.TCPSocketProbe{
			Port: int(probe.TCPSocket.Port.IntValue()),
			Host: probe.TCPSocket.Host,
		}
	}
	*target = append(*target, specProbe)
}

func convertStorageTraits(mounts []corev1.VolumeMount, volumes []corev1.Volume, claims []corev1.PersistentVolumeClaim, componentNamespace, componentName, containerName string, pvcLookup map[string]claimTemplateInfo) ([]spec.StorageTraitSpec, []string) {
	if len(mounts) == 0 {
		return nil, nil
	}
	volumeLookup := buildVolumeLookup(volumes)
	claimLookup := buildClaimTemplateLookup(claims)
	seen := make(map[string]struct{})
	var (
		result   []spec.StorageTraitSpec
		warnings []string
	)
	for _, mount := range mounts {
		key := mount.Name + "|" + mount.MountPath + "|" + mount.SubPath + "|" + mount.SubPathExpr
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if claimInfo, ok := claimLookup[mount.Name]; ok {
			result = append(result, spec.StorageTraitSpec{
				Name:         mount.Name,
				Type:         config.StorageTypePersistent,
				MountPath:    mount.MountPath,
				SubPath:      mount.SubPath,
				SubPathExpr:  mount.SubPathExpr,
				ReadOnly:     mount.ReadOnly,
				TmpCreate:    true,
				Size:         claimInfo.size,
				StorageClass: claimInfo.storageClass,
			})
			continue
		}
		volume, ok := volumeLookup[mount.Name]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("volume %s for container %s in component %s not found; skipped", mount.Name, containerName, componentName))
			continue
		}
		if volume.EmptyDir != nil {
			result = append(result, spec.StorageTraitSpec{
				Name:        mount.Name,
				Type:        config.StorageTypeEphemeral,
				MountPath:   mount.MountPath,
				SubPath:     mount.SubPath,
				SubPathExpr: mount.SubPathExpr,
				ReadOnly:    mount.ReadOnly,
			})
			continue
		}
		if volume.ConfigMap != nil {
			result = append(result, spec.StorageTraitSpec{
				Name:        mount.Name,
				Type:        config.StorageTypeConfig,
				MountPath:   mount.MountPath,
				SubPath:     mount.SubPath,
				SubPathExpr: mount.SubPathExpr,
				ReadOnly:    mount.ReadOnly,
				SourceName:  volume.ConfigMap.Name,
			})
			continue
		}
		if volume.Secret != nil {
			result = append(result, spec.StorageTraitSpec{
				Name:        mount.Name,
				Type:        config.StorageTypeSecret,
				MountPath:   mount.MountPath,
				SubPath:     mount.SubPath,
				SubPathExpr: mount.SubPathExpr,
				ReadOnly:    mount.ReadOnly,
				SourceName:  volume.Secret.SecretName,
			})
			continue
		}
		if volume.PersistentVolumeClaim != nil {
			claimName := volume.PersistentVolumeClaim.ClaimName
			info := pvcLookup[buildNamespacedKey(componentNamespace, claimName)]
			result = append(result, spec.StorageTraitSpec{
				Name:         mount.Name,
				Type:         config.StorageTypePersistent,
				MountPath:    mount.MountPath,
				SubPath:      mount.SubPath,
				SubPathExpr:  mount.SubPathExpr,
				ReadOnly:     mount.ReadOnly,
				ClaimName:    claimName,
				Size:         info.size,
				StorageClass: info.storageClass,
			})
			continue
		}
		warnings = append(warnings, fmt.Sprintf("volume %s for container %s in component %s uses unsupported source; skipped", mount.Name, containerName, componentName))
	}
	return result, warnings
}

func buildVolumeLookup(volumes []corev1.Volume) map[string]corev1.Volume {
	if len(volumes) == 0 {
		return nil
	}
	result := make(map[string]corev1.Volume, len(volumes))
	for _, volume := range volumes {
		if volume.Name == "" {
			continue
		}
		result[volume.Name] = volume
	}
	return result
}

func buildClaimTemplateLookup(claims []corev1.PersistentVolumeClaim) map[string]claimTemplateInfo {
	if len(claims) == 0 {
		return nil
	}
	result := make(map[string]claimTemplateInfo, len(claims))
	for _, claim := range claims {
		if claim.Name == "" {
			continue
		}
		info := claimTemplateInfo{}
		if qty, ok := claim.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			info.size = qty.String()
		}
		if claim.Spec.StorageClassName != nil {
			info.storageClass = *claim.Spec.StorageClassName
		}
		result[claim.Name] = info
	}
	return result
}

func convertInitContainers(containers []corev1.Container, volumes []corev1.Volume, claims []corev1.PersistentVolumeClaim, componentNamespace, componentName string, pvcLookup map[string]claimTemplateInfo) ([]spec.InitTraitSpec, []string) {
	if len(containers) == 0 {
		return nil, nil
	}
	var (
		result   []spec.InitTraitSpec
		warnings []string
	)
	for i, container := range containers {
		name := strings.TrimSpace(container.Name)
		if name == "" {
			name = fmt.Sprintf("%s-init-%d", componentName, i+1)
			warnings = append(warnings, fmt.Sprintf("init container name missing for component %s; using %s", componentName, name))
		}
		envs, traits, traitWarnings := buildContainerTraits(container, volumes, claims, componentNamespace, componentName, name, pvcLookup)
		warnings = append(warnings, traitWarnings...)
		initTrait := spec.InitTraitSpec{
			Name:  name,
			Image: container.Image,
			Properties: spec.Properties{
				Env:     envs,
				Command: container.Command,
			},
			Traits: traits,
		}
		initTrait.Properties = sanitizeProperties(initTrait.Properties)
		result = append(result, initTrait)
	}
	return result, warnings
}

func convertSidecarContainers(containers []corev1.Container, volumes []corev1.Volume, claims []corev1.PersistentVolumeClaim, componentNamespace, componentName string, pvcLookup map[string]claimTemplateInfo) ([]spec.SidecarTraitsSpec, []string) {
	if len(containers) == 0 {
		return nil, nil
	}
	var (
		result   []spec.SidecarTraitsSpec
		warnings []string
	)
	for i, container := range containers {
		name := strings.TrimSpace(container.Name)
		if name == "" {
			name = fmt.Sprintf("%s-sidecar-%d", componentName, i+1)
			warnings = append(warnings, fmt.Sprintf("sidecar container name missing for component %s; using %s", componentName, name))
		}
		envs, traits, traitWarnings := buildContainerTraits(container, volumes, claims, componentNamespace, componentName, name, pvcLookup)
		warnings = append(warnings, traitWarnings...)
		sidecar := spec.SidecarTraitsSpec{
			Name:    name,
			Image:   container.Image,
			Command: container.Command,
			Args:    container.Args,
			Env:     envs,
			Traits:  traits,
		}
		sidecar.Env = sanitizeEnvMap(sidecar.Env)
		result = append(result, sidecar)
	}
	return result, warnings
}

func buildContainerTraits(container corev1.Container, volumes []corev1.Volume, claims []corev1.PersistentVolumeClaim, componentNamespace, componentName, containerName string, pvcLookup map[string]claimTemplateInfo) (map[string]string, spec.Traits, []string) {
	var warnings []string
	envs, valueFromEnvs, envWarnings := splitEnvVars(container.Env, componentName)
	warnings = append(warnings, envWarnings...)
	envFrom, envFromWarnings := convertEnvFromSources(container.EnvFrom, componentName)
	warnings = append(warnings, envFromWarnings...)
	storage, storageWarnings := convertStorageTraits(container.VolumeMounts, volumes, claims, componentNamespace, componentName, containerName, pvcLookup)
	warnings = append(warnings, storageWarnings...)

	traits := spec.Traits{
		EnvFrom:        envFrom,
		Envs:           valueFromEnvs,
		Probes:         convertProbes(container),
		Resources:      convertResourceTraits(container),
		Storage:        storage,
		SecurityPolicy: convertSecurityPolicy(container.SecurityContext),
	}
	return envs, sanitizeTraits(traits), warnings
}

func sanitizeEnvMap(env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	return env
}

func applyServicesToComponents(services []*corev1.Service, components []apis.CreateComponentRequest, refs []componentRef) []string {
	if len(services) == 0 || len(components) == 0 {
		return nil
	}
	var warnings []string
	for _, svc := range services {
		if svc == nil {
			continue
		}
		selector := svc.Spec.Selector
		if len(selector) == 0 {
			warnings = append(warnings, fmt.Sprintf("service %s has no selector; skipped", svc.Name))
			continue
		}
		matched := false
		for _, ref := range refs {
			if !serviceMatchesRefNamespace(svc, ref) {
				continue
			}
			if !labelsMatch(selector, ref.labels) {
				continue
			}
			matched = true
			trait := buildServiceTrait(svc)
			components[ref.index].Traits.Service = append(components[ref.index].Traits.Service, trait)
		}
		if !matched {
			warnings = append(warnings, fmt.Sprintf("service %s has no matching workload; skipped", svc.Name))
		}
	}
	return warnings
}

func serviceMatchesRefNamespace(svc *corev1.Service, ref componentRef) bool {
	if svc == nil {
		return false
	}
	svcNS := normalizeResourceNamespace(svc.Namespace)
	refNS := normalizeResourceNamespace(ref.namespace)
	return strings.EqualFold(svcNS, refNS)
}

func labelsMatch(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels == nil {
			return false
		}
		if labels[key] != value {
			return false
		}
	}
	return true
}

func buildServiceTrait(svc *corev1.Service) spec.ServiceTraitSpec {
	if svc == nil {
		return spec.ServiceTraitSpec{}
	}
	serviceType := svc.Spec.Type
	if serviceType == "" {
		serviceType = corev1.ServiceTypeClusterIP
	}
	return spec.ServiceTraitSpec{
		Name:         strings.TrimSpace(svc.Name),
		Type:         string(config.ServiceAccessTypeFromKube(serviceType)),
		ExternalName: strings.TrimSpace(svc.Spec.ExternalName),
		Headless:     strings.EqualFold(strings.TrimSpace(svc.Spec.ClusterIP), corev1.ClusterIPNone),
		Selector:     normalizeConvertedServiceSelector(svc.Spec.Selector),
		Labels:       filterReservedConvertedLabels(svc.Labels),
		Ports:        servicePortsFromService(svc.Spec.Ports),
	}
}

func normalizeConvertedServiceSelector(selector map[string]string) map[string]string {
	normalized := utils.CopyStringMap(selector)
	// Keep managed identity keys so non-external Service traits remain valid.
	// Native generation rebinds their values to the target component, while
	// adopted generation keeps the source selector identity unchanged.
	if _, exists := normalized[config.LabelManagedBy]; exists {
		normalized[config.LabelManagedBy] = config.ManagedByEruun
	}
	return normalized
}

func normalizeResourceNamespace(namespace string) string {
	normalized := strings.TrimSpace(namespace)
	if normalized == "" {
		return config.DefaultNamespace
	}
	return normalized
}

func servicePortsFromService(ports []corev1.ServicePort) []spec.ServicePortTraitSpec {
	if len(ports) == 0 {
		return nil
	}
	result := make([]spec.ServicePortTraitSpec, 0, len(ports))
	for _, port := range ports {
		if port.Port == 0 {
			continue
		}
		protocol := corev1.ProtocolTCP
		if port.Protocol != "" {
			protocol = port.Protocol
		}
		traitPort := spec.ServicePortTraitSpec{
			Name:     strings.TrimSpace(port.Name),
			Port:     port.Port,
			Protocol: string(protocol),
		}
		if port.TargetPort.Type == intstr.Int && port.TargetPort.IntVal > 0 {
			traitPort.TargetPort = port.TargetPort.IntVal
		}
		result = append(result, traitPort)
	}
	return result
}

func ensureShareLabels(labels map[string]string, name string) map[string]string {
	name = strings.TrimSpace(name)
	if name == "" {
		return labels
	}
	if labels == nil {
		labels = make(map[string]string, 2)
	}
	if strings.TrimSpace(labels[config.LabelShareName]) == "" {
		labels[config.LabelShareName] = name
	}
	if strings.TrimSpace(labels[config.LabelShareStrategy]) == "" {
		labels[config.LabelShareStrategy] = string(config.ShareStrategyDefault)
	}
	return labels
}

func EnsureShareLabels(labels map[string]string, name string) map[string]string {
	return ensureShareLabels(labels, name)
}

func buildShareTrait(primaryLabels, fallbackLabels map[string]string) (*spec.ShareTraitSpec, []string) {
	strategy := strings.TrimSpace(primaryLabels[config.LabelShareStrategy])
	if strategy == "" && len(fallbackLabels) > 0 {
		strategy = strings.TrimSpace(fallbackLabels[config.LabelShareStrategy])
	}
	if strategy == "" {
		return nil, nil
	}
	normalized, ok := config.NormalizeShareStrategy(strategy)
	if !ok {
		return &spec.ShareTraitSpec{Strategy: string(normalized)}, []string{fmt.Sprintf("unknown share strategy %q; using default", strategy)}
	}
	return &spec.ShareTraitSpec{Strategy: string(normalized)}, nil
}

func buildNamespacedKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func buildPVCInfoLookup(pvcs []*unstructured.Unstructured) (map[string]claimTemplateInfo, []string, error) {
	if len(pvcs) == 0 {
		return nil, nil, nil
	}
	result := make(map[string]claimTemplateInfo, len(pvcs))
	var warnings []string
	for _, obj := range pvcs {
		if obj == nil {
			continue
		}
		var pvc corev1.PersistentVolumeClaim
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &pvc); err != nil {
			return nil, warnings, err
		}
		name := strings.TrimSpace(pvc.Name)
		if name == "" {
			warnings = append(warnings, "persistentvolumeclaim missing metadata.name; skipped")
			continue
		}
		info := claimTemplateInfo{}
		if qty, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			info.size = qty.String()
		}
		if pvc.Spec.StorageClassName != nil {
			info.storageClass = *pvc.Spec.StorageClassName
		}
		result[buildNamespacedKey(pvc.Namespace, name)] = info
	}
	return result, warnings, nil
}

func convertServices(objs []*unstructured.Unstructured) ([]*corev1.Service, []string, error) {
	if len(objs) == 0 {
		return nil, nil, nil
	}
	var (
		services []*corev1.Service
		warnings []string
	)
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		svc, err := convertService(obj)
		if err != nil {
			return nil, warnings, err
		}
		if svc == nil {
			continue
		}
		if strings.TrimSpace(svc.Name) == "" {
			warnings = append(warnings, "service missing metadata.name; skipped")
			continue
		}
		services = append(services, svc)
	}
	return services, warnings, nil
}

func convertIngresses(objs []*unstructured.Unstructured) ([]*networkingv1.Ingress, []string, error) {
	if len(objs) == 0 {
		return nil, nil, nil
	}
	var (
		ingresses []*networkingv1.Ingress
		warnings  []string
	)
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		var ing networkingv1.Ingress
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &ing); err != nil {
			return nil, warnings, err
		}
		if strings.TrimSpace(ing.Name) == "" {
			warnings = append(warnings, "ingress missing metadata.name; skipped")
			continue
		}
		ingresses = append(ingresses, &ing)
	}
	return ingresses, warnings, nil
}
