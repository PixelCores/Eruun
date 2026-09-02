package job

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func requiredJobInfo[T any](job *model.JobTask) (T, error) {
	var zero T
	if job == nil {
		return zero, fmt.Errorf("job task is nil")
	}
	info, ok := job.JobInfo.(T)
	if !ok {
		return zero, fmt.Errorf("job info is not %s (actual %T)", jobInfoTypeName[T](), job.JobInfo)
	}
	if isNilJobInfo(info) {
		return zero, fmt.Errorf("job info %s is nil", jobInfoTypeName[T]())
	}
	return info, nil
}

func optionalJobInfo[T any](job *model.JobTask) (T, bool) {
	var zero T
	if job == nil {
		return zero, false
	}
	info, ok := job.JobInfo.(T)
	if !ok || isNilJobInfo(info) {
		return zero, false
	}
	return info, true
}

func jobInfoTypeName[T any]() string {
	var zero T
	return fmt.Sprintf("%T", zero)
}

func isNilJobInfo(info interface{}) bool {
	if info == nil {
		return true
	}
	value := reflect.ValueOf(info)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func configMapFromJobInfo(ctx context.Context, job *model.JobTask, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*corev1.ConfigMap, error) {
	if job == nil {
		return nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *model.ConfigMapInput:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*model.ConfigMapInput]())
		}
		conf, err := info.GenerateConf(ctx, urlSecurityPolicy)
		if err != nil {
			return nil, fmt.Errorf("invalid ConfigMap spec: %w", err)
		}
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:        conf.Name,
				Namespace:   conf.Namespace,
				Labels:      conf.Labels,
				Annotations: conf.Annotations,
			},
			Data: conf.Data,
		}, nil
	case *corev1.ConfigMap:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*corev1.ConfigMap]())
		}
		return info, nil
	default:
		return nil, fmt.Errorf("unsupported configmap job info type: %T", job.JobInfo)
	}
}

func secretFromJobInfo(ctx context.Context, job *model.JobTask, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*corev1.Secret, error) {
	if job == nil {
		return nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *corev1.Secret:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*corev1.Secret]())
		}
		return info, nil
	case *model.SecretInput:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*model.SecretInput]())
		}
		secretType := corev1.SecretTypeOpaque
		if info.Type != "" {
			secretType = corev1.SecretType(info.Type)
		}
		stringData := map[string]string{}
		if info.URL != "" {
			body, err := utils.ReadFileFromURLSimple(ctx, info.URL, urlSecurityPolicy)
			if err != nil {
				return nil, fmt.Errorf("fetch secret url failed: %w", err)
			}
			fileName := info.FileName
			if fileName == "" {
				fileName = model.ExtractFileNameFromURLForSecret(info.URL)
			}
			stringData[fileName] = string(body)
		}
		for key, value := range info.Data {
			stringData[key] = value
		}
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:        info.Name,
				Namespace:   info.Namespace,
				Labels:      info.Labels,
				Annotations: info.Annotations,
			},
			Type:       secretType,
			StringData: stringData,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported secret job info type: %T", job.JobInfo)
	}
}

func scheduledJobInfo(job *model.JobTask) (*batchv1.CronJob, *batchv1.Job, error) {
	if job == nil {
		return nil, nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *batchv1.CronJob:
		if info == nil {
			return nil, nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*batchv1.CronJob]())
		}
		return info, nil, nil
	case *batchv1.Job:
		if info == nil {
			return nil, nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*batchv1.Job]())
		}
		return nil, info, nil
	default:
		return nil, nil, fmt.Errorf("scheduled job info has unexpected type %T", job.JobInfo)
	}
}

func serviceApplyFromJobInfo(job *model.JobTask) (*applyv1.ServiceApplyConfiguration, error) {
	return requiredJobInfo[*applyv1.ServiceApplyConfiguration](job)
}

func deploymentFromJobInfo(job *model.JobTask) (*appsv1.Deployment, error) {
	return requiredJobInfo[*appsv1.Deployment](job)
}

func statefulSetFromJobInfo(job *model.JobTask) (*appsv1.StatefulSet, error) {
	return requiredJobInfo[*appsv1.StatefulSet](job)
}

func pvcFromJobInfo(job *model.JobTask) (*corev1.PersistentVolumeClaim, error) {
	return requiredJobInfo[*corev1.PersistentVolumeClaim](job)
}

func ingressFromJobInfo(job *model.JobTask) (*networkingv1.Ingress, error) {
	return requiredJobInfo[*networkingv1.Ingress](job)
}

func batchJobFromJobInfo(job *model.JobTask) (*batchv1.Job, error) {
	return requiredJobInfo[*batchv1.Job](job)
}

// ApplyTaskIDAnnotation copies JobTask.TaskID into a batch Job payload when present.
func ApplyTaskIDAnnotation(job *model.JobTask) {
	jobObj, ok := optionalJobInfo[*batchv1.Job](job)
	if !ok || job.TaskID == "" {
		return
	}
	annotations := jobObj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string, 1)
	}
	annotations[config.AnnotationJobTaskID] = job.TaskID
	jobObj.SetAnnotations(annotations)
}

// ApplyExecutionIdentity copies a fenced JobTask identity into asynchronous payloads.
func ApplyExecutionIdentity(job *model.JobTask) {
	if job == nil || job.ExecutionKey == "" || job.RunGeneration == 0 {
		return
	}
	if jobObj, ok := optionalJobInfo[*batchv1.Job](job); ok {
		annotations := jobObj.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 2)
		}
		annotations[config.AnnotationJobExecutionKey] = job.ExecutionKey
		annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(job.RunGeneration, 10)
		jobObj.SetAnnotations(annotations)
	}
	if info, ok := optionalJobInfo[*CallbackJobInfo](job); ok {
		info.Payload.ExecutionKey = job.ExecutionKey
	}
}

func cloudInfoFromJobInfo(job *model.JobTask) (*CloudJobInfo, error) {
	return requiredJobInfo[*CloudJobInfo](job)
}

func callbackInfoFromJobInfo(job *model.JobTask) (*CallbackJobInfo, error) {
	return requiredJobInfo[*CallbackJobInfo](job)
}

func cleanupComponentFromJobInfo(job *model.JobTask) (*model.ApplicationComponent, error) {
	return requiredJobInfo[*model.ApplicationComponent](job)
}

func serviceAccountFromJobInfo(job *model.JobTask) (*corev1.ServiceAccount, error) {
	return requiredJobInfo[*corev1.ServiceAccount](job)
}

func roleFromJobInfo(job *model.JobTask) (*rbacv1.Role, error) {
	return requiredJobInfo[*rbacv1.Role](job)
}

func roleBindingFromJobInfo(job *model.JobTask) (*rbacv1.RoleBinding, error) {
	return requiredJobInfo[*rbacv1.RoleBinding](job)
}

func clusterRoleFromJobInfo(job *model.JobTask) (*rbacv1.ClusterRole, error) {
	return requiredJobInfo[*rbacv1.ClusterRole](job)
}

func clusterRoleBindingFromJobInfo(job *model.JobTask) (*rbacv1.ClusterRoleBinding, error) {
	return requiredJobInfo[*rbacv1.ClusterRoleBinding](job)
}
