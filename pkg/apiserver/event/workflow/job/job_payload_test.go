package job

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
)

func TestConfigMapFromJobInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("input", func(t *testing.T) {
		jobTask := &model.JobTask{
			JobInfo: &ConfigMapInput{
				Name:      "app-config",
				Namespace: "ops",
				Labels:    map[string]string{"app": "demo"},
				Data:      map[string]string{"config.yaml": "enabled: true"},
			},
		}

		got, err := configMapFromJobInfo(ctx, jobTask, nil)

		require.NoError(t, err)
		require.Equal(t, "app-config", got.Name)
		require.Equal(t, "ops", got.Namespace)
		require.Equal(t, map[string]string{"app": "demo"}, got.Labels)
		require.Equal(t, map[string]string{"config.yaml": "enabled: true"}, got.Data)
	})

	t.Run("existing", func(t *testing.T) {
		existing := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "existing"}}
		got, err := configMapFromJobInfo(ctx, &model.JobTask{JobInfo: existing}, nil)

		require.NoError(t, err)
		require.Same(t, existing, got)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := configMapFromJobInfo(ctx, &model.JobTask{JobInfo: "bad"}, nil)

		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported configmap job info type")
	})

	t.Run("nil-input", func(t *testing.T) {
		_, err := configMapFromJobInfo(ctx, &model.JobTask{JobInfo: (*ConfigMapInput)(nil)}, nil)

		require.Error(t, err)
		require.Contains(t, err.Error(), "nil")
	})
}

func TestSecretFromJobInfo(t *testing.T) {
	ctx := context.Background()

	t.Run("input", func(t *testing.T) {
		jobTask := &model.JobTask{
			JobInfo: &SecretInput{
				Name:      "app-secret",
				Namespace: "ops",
				Labels:    map[string]string{"app": "demo"},
				Data:      map[string]string{"token": "abc"},
			},
		}

		got, err := secretFromJobInfo(ctx, jobTask, nil)

		require.NoError(t, err)
		require.Equal(t, "app-secret", got.Name)
		require.Equal(t, "ops", got.Namespace)
		require.Equal(t, map[string]string{"app": "demo"}, got.Labels)
		require.Equal(t, corev1.SecretTypeOpaque, got.Type)
		require.Equal(t, map[string]string{"token": "abc"}, got.StringData)
	})

	t.Run("existing", func(t *testing.T) {
		existing := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "existing"}}
		got, err := secretFromJobInfo(ctx, &model.JobTask{JobInfo: existing}, nil)

		require.NoError(t, err)
		require.Same(t, existing, got)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := secretFromJobInfo(ctx, &model.JobTask{JobInfo: "bad"}, nil)

		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported secret job info type")
	})

	t.Run("nil-input", func(t *testing.T) {
		_, err := secretFromJobInfo(ctx, &model.JobTask{JobInfo: (*SecretInput)(nil)}, nil)

		require.Error(t, err)
		require.Contains(t, err.Error(), "nil")
	})
}

func TestScheduledJobInfo(t *testing.T) {
	t.Run("cronjob", func(t *testing.T) {
		expected := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cron"}}

		cron, oneTime, err := scheduledJobInfo(&model.JobTask{JobInfo: expected})

		require.NoError(t, err)
		require.Same(t, expected, cron)
		require.Nil(t, oneTime)
	})

	t.Run("one-time-job", func(t *testing.T) {
		expected := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "one-time"}}

		cron, oneTime, err := scheduledJobInfo(&model.JobTask{JobInfo: expected})

		require.NoError(t, err)
		require.Nil(t, cron)
		require.Same(t, expected, oneTime)
	})

	t.Run("invalid", func(t *testing.T) {
		_, _, err := scheduledJobInfo(&model.JobTask{JobInfo: "bad"})

		require.Error(t, err)
		require.Contains(t, err.Error(), "scheduled job info has unexpected type")
	})
}

func TestApplyTaskIDAnnotation(t *testing.T) {
	jobObj := &batchv1.Job{}
	ApplyTaskIDAnnotation(&model.JobTask{
		TaskID:  "task-1",
		JobInfo: jobObj,
	})

	require.Equal(t, "task-1", jobObj.GetAnnotations()[config.AnnotationJobTaskID])
}

func TestApplyExecutionIdentityPropagatesToAsyncPayloads(t *testing.T) {
	jobObj := &batchv1.Job{}
	batchTask := &model.JobTask{ExecutionKey: "execution-1", RunGeneration: 3, JobInfo: jobObj}
	ApplyExecutionIdentity(batchTask)

	require.Equal(t, "execution-1", jobObj.GetAnnotations()[config.AnnotationJobExecutionKey])
	require.Equal(t, "3", jobObj.GetAnnotations()[config.AnnotationJobRunGeneration])

	callbackInfo := &CallbackJobInfo{}
	callbackTask := &model.JobTask{ExecutionKey: "execution-2", RunGeneration: 4, JobInfo: callbackInfo}
	ApplyExecutionIdentity(callbackTask)

	require.Equal(t, "execution-2", callbackInfo.Payload.ExecutionKey)
}

func TestConfigurationURLReadLimits(t *testing.T) {
	for _, size := range []int{configMapMaxSize, configMapMaxSize + 1, configMapMaxSize + 1025} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := strings.Repeat("x", size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(payload))
			}))
			defer server.Close()
			policy := &spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}
			readSize := min(size, configMapMaxSize+1024)
			configMap, err := configMapFromJobInfo(context.Background(), &model.JobTask{JobInfo: &ConfigMapInput{
				Name: "config", URL: server.URL, FileName: "payload",
			}}, policy)
			if size > configMapMaxSize {
				require.EqualError(t, err, fmt.Sprintf("invalid ConfigMap spec: file size %d bytes exceeds ConfigMap maximum size %d bytes", readSize, configMapMaxSize))
			} else {
				require.NoError(t, err)
				require.Equal(t, payload, configMap.Data["payload"])
			}
			secret, err := secretFromJobInfo(context.Background(), &model.JobTask{JobInfo: &SecretInput{
				Name: "secret", URL: server.URL, FileName: "payload",
			}}, policy)
			require.NoError(t, err)
			require.Equal(t, payload[:readSize], secret.StringData["payload"], "Secret URL input retains its existing bounded-read behavior")
		})
	}
}
