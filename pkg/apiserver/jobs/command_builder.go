package jobs

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	traitprocessors "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

// buildCommandJob applies the shared component traits to a normalized Job step.
// Existing PVC references returned by ApplyTraits remain externally owned.
func buildCommandJob(name, namespace string, command spec.CommandJobSpec, traits spec.Traits, profiles traitprocessors.Profiles) (*batchv1.Job, error) {
	if len(validation.IsDNS1123Label(name)) != 0 || len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, fmt.Errorf("invalid Job name or namespace")
	}
	if !spec.ExplicitJobImage(command.Image) || len(command.Command) == 0 || command.Command[0] == "" || command.TimeoutSeconds <= 0 {
		return nil, fmt.Errorf("command requires an explicit image, command and positive timeout")
	}
	container := corev1.Container{Name: name, Image: command.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: append([]string(nil), command.Command...), Args: append([]string(nil), command.Args...)}
	backoff := int32(0)
	automount := false
	workload := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: map[string]string{
			config.AnnotationJobRunPolicy:           string(workflowconfig.JobRunPolicyRecreate),
			workflowconfig.AnnotationJobRetryPolicy: `{"onOOM":"stop"}`,
		}},
		Spec: batchv1.JobSpec{BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
				AutomountServiceAccountToken: &automount, Containers: []corev1.Container{container}}},
		},
	}
	componentTraits, err := model.NewJSONStructByStruct(traits)
	if err != nil {
		return nil, fmt.Errorf("encode Job traits: %w", err)
	}
	additional, err := traitprocessors.ApplyTraits(&model.ApplicationComponent{Name: name, Namespace: namespace, Traits: componentTraits}, workload, profiles)
	if err != nil {
		return nil, fmt.Errorf("render Job traits: %w", err)
	}
	for _, object := range additional {
		if _, ok := object.(*corev1.PersistentVolumeClaim); !ok {
			return nil, fmt.Errorf("Job traits produced unsupported additional object %T", object)
		}
	}
	return workload, nil
}
