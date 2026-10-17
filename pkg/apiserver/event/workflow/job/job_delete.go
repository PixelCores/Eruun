package job

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// deleteJobForeground deletes the observed Job and its Pods. Only the UID is
// pinned: an active Job's resourceVersion changes with every status update, so
// requiring it would turn routine Pod progress into a deletion conflict.
func deleteJobForeground(ctx context.Context, client kubernetes.Interface, namespace string, live *batchv1.Job) error {
	if client == nil || live == nil || namespace == "" || live.Name == "" || live.UID == "" {
		return fmt.Errorf("delete Job requires client, namespace, name and UID")
	}
	propagation := metav1.DeletePropagationForeground
	return client.BatchV1().Jobs(namespace).Delete(ctx, live.Name, metav1.DeleteOptions{
		Preconditions:     &metav1.Preconditions{UID: &live.UID},
		PropagationPolicy: &propagation,
	})
}

// foregroundJobDeleteTimeout leaves room for the Pods' graceful termination
// before garbage collection can remove the Job itself.
func foregroundJobDeleteTimeout(live *batchv1.Job) time.Duration {
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if live != nil && live.Spec.Template.Spec.TerminationGracePeriodSeconds != nil && *live.Spec.Template.Spec.TerminationGracePeriodSeconds >= 0 {
		grace = *live.Spec.Template.Spec.TerminationGracePeriodSeconds
	}
	return jobDeleteTimeout + time.Duration(grace)*time.Second
}

func waitForJobUIDDeletion(ctx context.Context, client kubernetes.Interface, namespace, name string, uid types.UID, timeout time.Duration) error {
	deleteCtx, cancel := context.WithTimeout(ctx, timeout)
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
