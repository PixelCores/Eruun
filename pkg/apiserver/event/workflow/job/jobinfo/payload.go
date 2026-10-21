package jobinfo

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/clients"
)

func RequiredJobInfo[T any](job *model.JobTask) (T, error) {
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

func OptionalJobInfo[T any](job *model.JobTask) (T, bool) {
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

func ConfigMapFromJobInfo(ctx context.Context, job *model.JobTask, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*corev1.ConfigMap, error) {
	if job == nil {
		return nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *ConfigMapInput:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*ConfigMapInput]())
		}
		conf, err := info.GenerateConf(ctx, urlSecurityPolicy)
		if err != nil {
			return nil, fmt.Errorf("invalid ConfigMap spec: %w", err)
		}
		return conf, nil
	case *corev1.ConfigMap:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*corev1.ConfigMap]())
		}
		return info, nil
	default:
		return nil, fmt.Errorf("unsupported configmap job info type: %T", job.JobInfo)
	}
}

func SecretFromJobInfo(ctx context.Context, job *model.JobTask, urlSecurityPolicy *spec.URLSecurityPolicySpec) (*corev1.Secret, error) {
	if job == nil {
		return nil, fmt.Errorf("job task is nil")
	}
	switch info := job.JobInfo.(type) {
	case *corev1.Secret:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*corev1.Secret]())
		}
		return info, nil
	case *SecretInput:
		if info == nil {
			return nil, fmt.Errorf("job info %s is nil", jobInfoTypeName[*SecretInput]())
		}
		secretType := corev1.SecretTypeOpaque
		if info.Type != "" {
			secretType = corev1.SecretType(info.Type)
		}
		stringData := map[string]string{}
		if info.URL != "" {
			body, err := clients.ReadURL(ctx, info.URL, urlSecurityPolicy, configMapMaxSize+1024)
			if err != nil {
				return nil, fmt.Errorf("fetch secret url failed: %w", err)
			}
			fileName := info.FileName
			if fileName == "" {
				fileName = extractFileNameFromURLForSecret(info.URL)
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

func ScheduledJobInfo(job *model.JobTask) (*batchv1.CronJob, *batchv1.Job, error) {
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

func ServiceFromJobInfo(job *model.JobTask) (*corev1.Service, error) {
	return RequiredJobInfo[*corev1.Service](job)
}

func DeploymentFromJobInfo(job *model.JobTask) (*appsv1.Deployment, error) {
	return RequiredJobInfo[*appsv1.Deployment](job)
}

func StatefulSetFromJobInfo(job *model.JobTask) (*appsv1.StatefulSet, error) {
	return RequiredJobInfo[*appsv1.StatefulSet](job)
}

func PvcFromJobInfo(job *model.JobTask) (*corev1.PersistentVolumeClaim, error) {
	return RequiredJobInfo[*corev1.PersistentVolumeClaim](job)
}

func IngressFromJobInfo(job *model.JobTask) (*networkingv1.Ingress, error) {
	return RequiredJobInfo[*networkingv1.Ingress](job)
}

func BatchJobFromJobInfo(job *model.JobTask) (*batchv1.Job, error) {
	return RequiredJobInfo[*batchv1.Job](job)
}

// ApplyTaskIDAnnotation copies JobTask.TaskID into a batch Job payload when present.
func ApplyTaskIDAnnotation(job *model.JobTask) {
	jobObj, ok := OptionalJobInfo[*batchv1.Job](job)
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
	if jobObj, ok := OptionalJobInfo[*batchv1.Job](job); ok {
		annotations := jobObj.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 2)
		}
		annotations[config.AnnotationJobExecutionKey] = job.ExecutionKey
		annotations[config.AnnotationJobRunGeneration] = strconv.FormatUint(job.RunGeneration, 10)
		jobObj.SetAnnotations(annotations)
		StampWorkspaceJobPodIdentity(job, jobObj)
	}
	if info, ok := OptionalJobInfo[*CallbackJobInfo](job); ok {
		info.Payload.ExecutionKey = job.ExecutionKey
	}
}

func CallbackInfoFromJobInfo(job *model.JobTask) (*CallbackJobInfo, error) {
	return RequiredJobInfo[*CallbackJobInfo](job)
}

func CleanupComponentFromJobInfo(job *model.JobTask) (*model.ApplicationComponent, error) {
	return RequiredJobInfo[*model.ApplicationComponent](job)
}
