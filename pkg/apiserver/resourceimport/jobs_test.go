package resourceimport

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	access "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/account"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	importcontract "github.com/PixelCores/Eruun/pkg/apiserver/resourceimport/contract"
)

type resourceImportJobStore struct {
	*inMemoryAppStore
	tasks map[string]*model.WorkflowQueue
	jobs  []*model.JobInfo
}

func newResourceImportJobStore() *resourceImportJobStore {
	return &resourceImportJobStore{
		inMemoryAppStore: newInMemoryAppStore(),
		tasks:            make(map[string]*model.WorkflowQueue),
	}
}

func (s *resourceImportJobStore) Get(ctx context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
	case *model.WorkflowQueue:
		stored := s.tasks[value.TaskID]
		if stored == nil {
			return datastore.ErrRecordNotExist
		}
		*value = *stored
		return nil
	case *model.JobInfo:
		for _, stored := range s.jobs {
			if stored.ID == value.ID {
				*value = *stored
				return nil
			}
		}
		return datastore.ErrRecordNotExist
	default:
		return s.inMemoryAppStore.Get(ctx, entity)
	}
}

func (s *resourceImportJobStore) List(ctx context.Context, query datastore.Entity, options *datastore.ListOptions) ([]datastore.Entity, error) {
	if value, ok := query.(*model.JobInfo); ok {
		result := make([]datastore.Entity, 0, len(s.jobs))
		for _, stored := range s.jobs {
			if value.TaskID != "" && stored.TaskID != value.TaskID {
				continue
			}
			if value.WorkspaceID != "" && stored.WorkspaceID != value.WorkspaceID {
				continue
			}
			result = append(result, stored)
		}
		return result, nil
	}
	return s.inMemoryAppStore.List(ctx, query, options)
}

func (s *resourceImportJobStore) Add(ctx context.Context, entity datastore.Entity) error {
	switch value := entity.(type) {
	case *model.WorkflowQueue:
		copy := *value
		s.tasks[value.TaskID] = &copy
		return nil
	case *model.JobInfo:
		copy := *value
		s.jobs = append(s.jobs, &copy)
		return nil
	default:
		return s.inMemoryAppStore.Add(ctx, entity)
	}
}

func resourceImportTestContext() context.Context {
	return access.WithScope(context.Background(), access.Scope{
		UserID:      "user-1",
		WorkspaceID: "workspace-1",
		Namespace:   "team-production",
		Role:        "admin",
	})
}

func TestExecuteResourceImportScanAppliesUserRules(t *testing.T) {
	namespace := "team-production"
	objects := []runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name:            "payments-api",
			Namespace:       namespace,
			UID:             types.UID("payments-api-uid"),
			ResourceVersion: "17",
			Labels:          map[string]string{"team": "payments"},
		}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "api", Image: "nginx:1.27"}},
		}}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name:            "orders-api",
			Namespace:       namespace,
			UID:             types.UID("orders-api-uid"),
			ResourceVersion: "21",
			Labels:          map[string]string{"team": "orders"},
		}},
	}
	service := &serviceImpl{KubeClient: fake.NewSimpleClientset(objects...)}
	request, err := json.Marshal(apisv1.ResourceImportScanJobRequest{
		Namespace: namespace,
		Rules: []apisv1.ResourceImportScanRule{{
			Kinds:         []string{"Deployment"},
			NameRegex:     `^payments-`,
			LabelSelector: "team=payments",
		}},
	})
	require.NoError(t, err)

	raw, err := service.ExecuteResourceImportJob(
		resourceImportTestContext(),
		config.WorkflowTaskTypeResourceImportScan,
		request,
	)
	require.NoError(t, err)

	var result apisv1.ResourceImportScanResult
	require.NoError(t, json.Unmarshal(raw, &result))
	require.Len(t, result.Resources, 1)
	candidate := result.Resources[0]
	assert.Equal(t, "payments-api", candidate.Name)
	assert.Equal(t, resourceImportCandidate, candidate.Status)
	require.NotNil(t, candidate.Source)
	assert.Equal(t, "payments-api-uid", candidate.Source.UID)
	assert.Equal(t, "17", candidate.Source.ResourceVersion)
	assert.NotEmpty(t, candidate.Source.SpecDigest)
}

func TestSubmitResourceImportJobsSeparatesScanFromManagement(t *testing.T) {
	ctx := resourceImportTestContext()
	store := newResourceImportJobStore()
	service := &serviceImpl{Store: store}

	scanAccepted, err := service.SubmitScanJob(ctx, apisv1.ResourceImportScanJobRequest{
		Namespace: "team-production",
		Rules: []apisv1.ResourceImportScanRule{{
			Kinds:     []string{"Deployment"},
			NameRegex: `^payments-`,
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, config.WorkflowTaskTypeResourceImportScan, scanAccepted.Type)
	assert.Equal(t, string(config.StatusWaiting), scanAccepted.Status)

	scanTask := store.tasks[scanAccepted.TaskID]
	require.NotNil(t, scanTask)
	assert.Equal(t, "workspace-1", scanTask.WorkspaceID)
	assert.Empty(t, scanTask.ProjectID)
	assert.Empty(t, scanTask.AppID)
	var scanEnvelope importcontract.TaskEnvelope
	require.NoError(t, json.Unmarshal([]byte(scanTask.ResourceActionInfo), &scanEnvelope))
	assert.Equal(t, "team-production", scanEnvelope.Namespace)

	scanResult := apisv1.ResourceImportScanResult{
		Namespace: "team-production",
		Resources: []apisv1.ImportNamespaceResourceResult{{
			Kind:      "Deployment",
			Namespace: "team-production",
			Name:      "payments-api",
			Status:    resourceImportCandidate,
			Source: &apisv1.ImportNamespaceResourceIdentity{
				APIVersion:      "apps/v1",
				Kind:            "Deployment",
				Namespace:       "team-production",
				Name:            "payments-api",
				UID:             "payments-api-uid",
				ResourceVersion: "17",
				SpecDigest:      "sha256:example",
			},
		}},
	}
	resultJSON, err := json.Marshal(scanResult)
	require.NoError(t, err)
	scanTask.Status = config.StatusCompleted
	store.jobs = append(store.jobs, &model.JobInfo{
		ID:          1,
		TaskID:      scanTask.TaskID,
		WorkspaceID: scanTask.WorkspaceID,
		Type:        string(config.JobResourceImportScan),
		Status:      string(config.StatusCompleted),
		Info:        string(resultJSON),
	})

	manageAccepted, err := service.SubmitManageJob(ctx, apisv1.ResourceImportManageJobRequest{
		ScanTaskID: scanTask.TaskID,
		Applications: []apisv1.ImportNamespaceApplicationMapping{{
			Name: "payments",
			Components: []apisv1.ImportNamespaceComponentMapping{{
				Name: "api",
				Workload: apisv1.ImportNamespaceWorkloadReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "payments-api",
				},
			}},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, config.WorkflowTaskTypeResourceImportManage, manageAccepted.Type)
	assert.NotEqual(t, scanAccepted.TaskID, manageAccepted.TaskID)
	manageTask := store.tasks[manageAccepted.TaskID]
	require.NotNil(t, manageTask)
	assert.Equal(t, scanTask.WorkspaceID, manageTask.WorkspaceID)
	assert.Empty(t, manageTask.ProjectID)
	assert.Equal(t, config.StatusWaiting, manageTask.Status)
}

func TestSubmitManageJobRejectsResourceOutsideScan(t *testing.T) {
	ctx := resourceImportTestContext()
	store := newResourceImportJobStore()
	service := &serviceImpl{Store: store}
	store.tasks["scan-1"] = &model.WorkflowQueue{
		TaskID:      "scan-1",
		WorkspaceID: "workspace-1",
		Type:        config.WorkflowTaskTypeResourceImportScan,
		Status:      config.StatusCompleted,
	}
	resultJSON, err := json.Marshal(apisv1.ResourceImportScanResult{Namespace: "team-production"})
	require.NoError(t, err)
	store.jobs = append(store.jobs, &model.JobInfo{ID: 1, TaskID: "scan-1", WorkspaceID: "workspace-1", Info: string(resultJSON)})

	_, err = service.SubmitManageJob(ctx, apisv1.ResourceImportManageJobRequest{
		ScanTaskID: "scan-1",
		Applications: []apisv1.ImportNamespaceApplicationMapping{{
			Name: "payments",
			Components: []apisv1.ImportNamespaceComponentMapping{{
				Name: "api",
				Workload: apisv1.ImportNamespaceWorkloadReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "not-scanned",
				},
			}},
		}},
	})
	require.Error(t, err)
	assert.Len(t, store.tasks, 1)
}
