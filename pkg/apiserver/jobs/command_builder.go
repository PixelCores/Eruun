package jobs

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	traitprocessors "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

// buildCommandJob renders a standalone workload without constructing an application
// or component. Its fixed stop policy enables durable recovery without replaying
// user commands after a failed attempt.
func buildCommandJob(name, namespace string, command spec.CommandJobSpec, traits spec.JobTraits) (*batchv1.Job, error) {
	if len(validation.IsDNS1123Subdomain(name)) != 0 || len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, fmt.Errorf("invalid Job name or namespace")
	}
	if !spec.ExplicitJobImage(command.Image) || len(command.Command) == 0 || command.Command[0] == "" || command.TimeoutSeconds <= 0 {
		return nil, fmt.Errorf("command requires an explicit image, command and positive timeout")
	}
	container := corev1.Container{Name: "job", Image: command.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: append([]string(nil), command.Command...), Args: append([]string(nil), command.Args...)}
	if traits.Resources != nil {
		result, err := (&traitprocessors.ResourcesProcessor{}).Process(traits.Resources)
		if err != nil {
			return nil, fmt.Errorf("render Job resources: %w", err)
		}
		container.Resources = *result.ResourceRequirements
	}
	container.SecurityContext = traits.SecurityPolicy.DeepCopy()
	seenEnv := map[string]bool{}
	for _, item := range traits.Envs {
		if len(validation.IsEnvVarName(item.Name)) != 0 || seenEnv[item.Name] {
			return nil, fmt.Errorf("invalid or duplicate environment variable name")
		}
		seenEnv[item.Name] = true
		value := corev1.EnvVar{Name: item.Name}
		source := item.ValueFrom
		count := 0
		if source.Static != nil {
			count++
			value.Value = *source.Static
		}
		if source.Secret != nil {
			count++
			if len(validation.IsDNS1123Subdomain(source.Secret.Name)) != 0 || len(validation.IsConfigMapKey(source.Secret.Key)) != 0 {
				return nil, fmt.Errorf("invalid environment Secret reference")
			}
			value.ValueFrom = &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: source.Secret.Name}, Key: source.Secret.Key}}
		}
		if source.Config != nil {
			count++
			if len(validation.IsDNS1123Subdomain(source.Config.Name)) != 0 || len(validation.IsConfigMapKey(source.Config.Key)) != 0 {
				return nil, fmt.Errorf("invalid environment ConfigMap reference")
			}
			value.ValueFrom = &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: source.Config.Name}, Key: source.Config.Key}}
		}
		if source.Field != nil {
			count++
			switch *source.Field {
			case "metadata.name", "metadata.namespace", "metadata.uid", "status.podIP":
			default:
				return nil, fmt.Errorf("unsupported environment field reference")
			}
			value.ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: *source.Field}}
		}
		if count != 1 {
			return nil, fmt.Errorf("environment variable requires exactly one source")
		}
		container.Env = append(container.Env, value)
	}
	for _, source := range traits.EnvFrom {
		if len(validation.IsDNS1123Subdomain(source.SourceName)) != 0 {
			return nil, fmt.Errorf("invalid envFrom source name")
		}
		ref := corev1.LocalObjectReference{Name: source.SourceName}
		switch source.Type {
		case spec.StorageTypeSecret:
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref}})
		case spec.StorageTypeConfig:
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: ref}})
		default:
			return nil, fmt.Errorf("unsupported envFrom type")
		}
	}
	var volumes []corev1.Volume
	seenVolume := map[string]bool{}
	seenMount := map[string]bool{}
	for _, storage := range traits.Storage {
		if len(validation.IsDNS1123Label(storage.Name)) != 0 || seenVolume[storage.Name] || !strings.HasPrefix(storage.MountPath, "/") || seenMount[storage.MountPath] {
			return nil, fmt.Errorf("invalid or duplicate storage name or mount path")
		}
		if storage.TmpCreate || storage.Size != "" || storage.StorageClass != "" || (storage.SubPath != "" && storage.SubPathExpr != "") {
			return nil, fmt.Errorf("Job storage only supports existing references and one subpath form")
		}
		seenVolume[storage.Name], seenMount[storage.MountPath] = true, true
		volume := corev1.Volume{Name: storage.Name}
		sourceName := storage.SourceName
		if sourceName == "" {
			sourceName = storage.Name
		}
		if len(validation.IsDNS1123Subdomain(sourceName)) != 0 {
			return nil, fmt.Errorf("invalid storage source")
		}
		switch storage.Type {
		case spec.StorageTypePersistent:
			claim := storage.ClaimName
			if claim == "" {
				claim = storage.Name
			}
			if len(validation.IsDNS1123Subdomain(claim)) != 0 {
				return nil, fmt.Errorf("invalid PVC reference")
			}
			volume.PersistentVolumeClaim = &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim, ReadOnly: storage.ReadOnly}
		case spec.StorageTypeConfig:
			volume.ConfigMap = &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: sourceName}}
		case spec.StorageTypeSecret:
			volume.Secret = &corev1.SecretVolumeSource{SecretName: sourceName}
		default:
			return nil, fmt.Errorf("Job storage requires a PVC, ConfigMap or Secret reference")
		}
		volumes = append(volumes, volume)
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: storage.Name, MountPath: storage.MountPath,
			SubPath: storage.SubPath, SubPathExpr: storage.SubPathExpr, ReadOnly: storage.ReadOnly})
	}
	backoff := int32(0)
	automount := false
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: map[string]string{
			config.AnnotationJobRunPolicy:           string(workflowconfig.JobRunPolicyRecreate),
			workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
		}},
		Spec: batchv1.JobSpec{BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
				AutomountServiceAccountToken: &automount, Containers: []corev1.Container{container}, Volumes: volumes}},
		},
	}, nil
}
