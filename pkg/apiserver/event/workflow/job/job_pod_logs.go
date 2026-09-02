package job

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/kube"
)

func finalizeCompletedJob(ctx context.Context, client kubernetes.Interface, jobTask *model.JobTask, jobObj *batchv1.Job) {
	if client == nil || jobTask == nil || jobObj == nil {
		return
	}
	if jobTask.Status != config.StatusCompleted {
		return
	}
	namespace := jobObj.Namespace
	if namespace == "" {
		namespace = jobTask.Namespace
	}
	name := jobObj.Name
	if name == "" {
		name = jobTask.Name
	}
	if namespace == "" || name == "" {
		return
	}

	logs, err := collectJobPodLogs(ctx, client, namespace, name)
	if err != nil {
		klog.Warningf("collect job logs %s/%s failed: %v", namespace, name, err)
	} else if logs != "" {
		jobTask.Info = logs
	}

	if err := deleteCompletedJobAndPods(ctx, client, namespace, name, jobObj); err != nil {
		klog.Warningf("clean completed job %s/%s failed: %v", namespace, name, err)
	}
}

func finalizeCompletedJobIfNeeded(ctx context.Context, client kubernetes.Interface, jobTask *model.JobTask) {
	jobObj, ok := completedJobForFinalize(jobTask)
	if !ok {
		return
	}
	finalizeCompletedJob(ctx, client, jobTask, jobObj)
}

func completedJobForFinalize(jobTask *model.JobTask) (*batchv1.Job, bool) {
	if jobTask == nil {
		return nil, false
	}
	if jobTask.Status != config.StatusCompleted {
		return nil, false
	}
	if jobTask.JobType != string(config.JobDeployInstant) && jobTask.JobType != string(config.JobDeployScheduled) {
		return nil, false
	}
	jobObj, err := batchJobFromJobInfo(jobTask)
	if err != nil {
		return nil, false
	}
	return jobObj, true
}

func collectJobPodLogs(ctx context.Context, client kubernetes.Interface, namespace, jobName string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("client is nil")
	}
	if namespace == "" || jobName == "" {
		return "", fmt.Errorf("job namespace or name is empty")
	}
	labelSet := labels.Set{batchv1.JobNameLabel: jobName}
	pods, err := kube.ListPodsByLabels(ctx, client, namespace, labelSet)
	if err != nil {
		return "", fmt.Errorf("list job pods %s/%s: %w", namespace, jobName, err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for job %s/%s", namespace, jobName)
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Name < pods.Items[j].Name
	})

	var builder strings.Builder
	for _, pod := range pods.Items {
		containerNames := podLogContainerNames(&pod)
		if len(containerNames) == 0 {
			continue
		}
		for _, container := range containerNames {
			if builder.Len() > 0 {
				builder.WriteString("\n")
			}
			builder.WriteString(fmt.Sprintf("=== Pod: %s (container: %s) ===\n", pod.Name, container))
			logText, err := readPodContainerLogs(ctx, client, namespace, pod.Name, container)
			if err != nil {
				klog.Warningf("read pod logs %s/%s (container %s) failed: %v", namespace, pod.Name, container, err)
				continue
			}
			builder.WriteString(logText)
			if !strings.HasSuffix(logText, "\n") {
				builder.WriteString("\n")
			}
		}
	}
	logs := strings.TrimSpace(builder.String())
	if logs == "" {
		return "", fmt.Errorf("no logs collected for job %s/%s", namespace, jobName)
	}
	return logs, nil
}

func jobForCompletedCleanup(ctx context.Context, client kubernetes.Interface, namespace, name string, fallback *batchv1.Job) (*batchv1.Job, error) {
	if client == nil {
		return nil, fmt.Errorf("client is nil")
	}
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("job namespace or name is empty")
	}
	if fallback != nil && fallback.UID != "" && fallback.Name == name && (fallback.Namespace == "" || fallback.Namespace == namespace) {
		return fallback, nil
	}
	jobObj, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job %s/%s: %w", namespace, name, err)
	}
	if jobObj.UID == "" {
		return nil, fmt.Errorf("job %s/%s UID is empty", namespace, name)
	}
	return jobObj, nil
}

func deleteCompletedJobAndPods(ctx context.Context, client kubernetes.Interface, namespace, name string, fallback *batchv1.Job) error {
	cleanupJob, err := jobForCompletedCleanup(ctx, client, namespace, name, fallback)
	if err != nil || cleanupJob == nil {
		return err
	}

	uid := cleanupJob.UID
	preconditions := &metav1.Preconditions{UID: &uid}
	if resourceVersion := cleanupJob.ResourceVersion; resourceVersion != "" {
		preconditions.ResourceVersion = &resourceVersion
	}
	propagation := metav1.DeletePropagationBackground
	deleteErr := client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions:     preconditions,
		PropagationPolicy: &propagation,
	})
	switch {
	case deleteErr == nil:
		if err := waitForJobUIDDeletion(ctx, client, namespace, name, uid); err != nil {
			return err
		}
	case k8serrors.IsNotFound(deleteErr):
		// The target Job is already absent; any matching Pods are residuals.
	case k8serrors.IsConflict(deleteErr):
		absent, err := jobUIDAbsent(ctx, client, namespace, name, uid)
		if err != nil {
			return fmt.Errorf("verify job %s/%s after UID conflict: %w", namespace, name, err)
		}
		if !absent {
			return fmt.Errorf("delete job %s/%s with identity precondition: %w", namespace, name, deleteErr)
		}
	default:
		return fmt.Errorf("delete job %s/%s with identity precondition: %w", namespace, name, deleteErr)
	}

	if _, err := deleteCompletedPodsForJob(ctx, client, namespace, cleanupJob); err != nil {
		return err
	}
	return nil
}

func waitForJobUIDDeletion(ctx context.Context, client kubernetes.Interface, namespace, name string, uid types.UID) error {
	deleteCtx, cancel := context.WithTimeout(ctx, jobDeleteTimeout)
	defer cancel()

	if err := wait.PollUntilContextCancel(deleteCtx, jobPollInterval, true, func(checkCtx context.Context) (bool, error) {
		return jobUIDAbsent(checkCtx, client, namespace, name, uid)
	}); err != nil {
		return fmt.Errorf("wait for job %s/%s UID %q deletion: %w", namespace, name, uid, err)
	}
	return nil
}

func jobUIDAbsent(ctx context.Context, client kubernetes.Interface, namespace, name string, uid types.UID) (bool, error) {
	jobObj, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get job %s/%s: %w", namespace, name, err)
	}
	return jobObj.UID != uid, nil
}

func deleteCompletedPodsForJob(ctx context.Context, client kubernetes.Interface, namespace string, jobObj *batchv1.Job) (int, error) {
	if client == nil {
		return 0, fmt.Errorf("client is nil")
	}
	if jobObj == nil || jobObj.Name == "" || jobObj.UID == "" {
		return 0, nil
	}
	if namespace == "" {
		namespace = jobObj.Namespace
	}
	if namespace == "" {
		return 0, fmt.Errorf("job namespace is empty")
	}
	labelSet := labels.Set{batchv1.JobNameLabel: jobObj.Name}
	pods, err := kube.ListPodsByLabels(ctx, client, namespace, labelSet)
	if err != nil {
		return 0, fmt.Errorf("list completed pods for job %s/%s: %w", namespace, jobObj.Name, err)
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Name < pods.Items[j].Name
	})

	var deleted int
	var firstErr error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		if !podOwnedByJob(pod, jobObj) {
			continue
		}
		if err := client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
			if firstErr == nil {
				firstErr = fmt.Errorf("delete completed pod %s/%s: %w", namespace, pod.Name, err)
			}
			continue
		}
		deleted++
	}
	return deleted, firstErr
}

func podLogContainerNames(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	names := make([]string, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	for _, container := range pod.Spec.InitContainers {
		if container.Name == "" {
			continue
		}
		names = append(names, container.Name)
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "" {
			continue
		}
		names = append(names, container.Name)
	}
	return names
}

func readPodContainerLogs(ctx context.Context, client kubernetes.Interface, namespace, podName, container string) (string, error) {
	return readPodContainerLogsWithMode(ctx, client, namespace, podName, container, false)
}

func readPodContainerLogsWithMode(ctx context.Context, client kubernetes.Interface, namespace, podName, container string, previous bool) (string, error) {
	tailLines := config.DefaultJobLogTailLines
	limitBytes := config.DefaultJobLogLimitBytes
	options := &corev1.PodLogOptions{Container: container, Previous: previous}
	if tailLines > 0 {
		options.TailLines = &tailLines
	}
	if limitBytes > 0 {
		options.LimitBytes = &limitBytes
	}
	req := client.CoreV1().Pods(namespace).GetLogs(podName, options)
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	content, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	text, _ := finalizeLogContent(content, limitBytes)
	return text, nil
}

const logTruncatedMarker = "[log truncated]"

func finalizeLogContent(content []byte, limitBytes int64) (string, bool) {
	text := string(content)
	if limitBytes <= 0 {
		return text, false
	}
	if int64(len(content)) < limitBytes {
		return text, false
	}
	text = strings.TrimRight(text, "\n")
	return text + "\n" + logTruncatedMarker, true
}
