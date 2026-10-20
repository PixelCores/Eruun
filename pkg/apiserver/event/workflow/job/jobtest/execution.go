package jobtest

import (
	"context"
	"sync"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

const ShareLockerPrefix = "eruun-share"

func JobForPodFallback(name string, completions *int32) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
		},
		Spec: batchv1.JobSpec{
			Completions: completions,
		},
	}
}

func SucceededPodForJob(jobObj *batchv1.Job, name string, ownerUID types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: jobObj.Namespace,
			Labels: map[string]string{
				batchv1.JobNameLabel: jobObj.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       jobObj.Name,
					UID:        ownerUID,
				},
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
}

type WorkflowOwnershipStore struct {
	*NoopStore
	mu        sync.Mutex
	Task      model.WorkflowQueue
	Tasks     []model.WorkflowQueue
	TasksByID map[string]model.WorkflowQueue
	Errs      []error
}

func (s *WorkflowOwnershipStore) Get(_ context.Context, entity datastore.Entity) error {
	task, ok := entity.(*model.WorkflowQueue)
	if !ok {
		return s.NoopStore.Get(context.Background(), entity)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.Errs) > 0 {
		err := s.Errs[0]
		s.Errs = s.Errs[1:]
		if err != nil {
			return err
		}
	}
	if len(s.Tasks) > 0 {
		*task = s.Tasks[0]
		if len(s.Tasks) > 1 {
			s.Tasks = s.Tasks[1:]
		}
		return nil
	}
	if stored, exists := s.TasksByID[task.TaskID]; exists {
		*task = stored
		return nil
	}
	*task = s.Task
	return nil
}

// The fixture models an explicit transaction boundary; SQL tests verify isolation.
func (s *WorkflowOwnershipStore) WithReadCommittedTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return s.WithTransaction(ctx, fn)
}

func (s *WorkflowOwnershipStore) WithTransaction(ctx context.Context, fn func(datastore.DataStore) error) error {
	return fn(s)
}

func (s *WorkflowOwnershipStore) CompareAndSwapWithConditions(_ context.Context, entity datastore.Entity, _ map[string]interface{}, _ map[string]interface{}) (bool, error) {
	if _, ok := entity.(*model.WorkflowQueue); ok {
		return true, nil
	}
	return false, nil
}
