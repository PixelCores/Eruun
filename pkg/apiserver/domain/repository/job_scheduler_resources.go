package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// This is the declared short-lived Job budget, not an inventory of the cluster.
// Long-lived application Pods and external workloads still need actual quota
// headroom, which cannot be established by a database admission transaction.
type jobResourceBudget struct {
	quota              corev1.ResourceList
	keys               []corev1.ResourceName
	used               map[string]corev1.ResourceList
	unknown            map[string]bool
	activeExecutions   map[string]bool
	retainedExecutions map[string]corev1.ResourceList
	candidates         map[int]corev1.ResourceList
}

func newJobResourceBudget(quotas []corev1.ResourceList) (*jobResourceBudget, error) {
	if len(quotas) > 1 {
		return nil, fmt.Errorf("job admission requires one workspace resource budget")
	}
	if len(quotas) == 0 || quotas[0] == nil {
		return nil, nil
	}
	quota := corev1.ResourceList{}
	var keys []corev1.ResourceName
	for _, key := range []corev1.ResourceName{corev1.ResourcePods, corev1.ResourceRequestsCPU, corev1.ResourceRequestsMemory,
		corev1.ResourceLimitsCPU, corev1.ResourceLimitsMemory, corev1.ResourceRequestsEphemeralStorage, corev1.ResourceLimitsEphemeralStorage} {
		if q, ok := quotas[0][key]; ok {
			if q.Sign() < 0 {
				return nil, fmt.Errorf("negative job admission quota %s", key)
			}
			quota[key] = q.DeepCopy()
			keys = append(keys, key)
		}
	}
	return &jobResourceBudget{quota: quota, keys: keys, used: map[string]corev1.ResourceList{}, unknown: map[string]bool{}, activeExecutions: map[string]bool{}, retainedExecutions: map[string]corev1.ResourceList{}, candidates: map[int]corev1.ResourceList{}}, nil
}

func resourceFreeJob(kind string) bool {
	switch config.JobType(kind) {
	case config.JobDeployService, config.JobDeployPVC, config.JobDeployConfigMap, config.JobDeploySecret,
		config.JobDeployIngress, config.JobDeployServiceAccount, config.JobDeployRole, config.JobDeployRoleBinding,
		config.JobDeployClusterRole, config.JobDeployClusterRoleBinding, config.JobDeployPodDisruptionBudget,
		config.JobDeployNetworkPolicy, config.JobDeployCloud, config.JobDeployCallback, config.JobCleanupResources,
		config.JobLogArchiveUpload, config.JobDatabaseReset, config.JobVersionRestart:
		return true
	default:
		return false
	}
}

func evaluationResourceTraits(job *model.JobInfo) (*spec.ResourceTraitsSpec, *spec.ResourceTraitsSpec, int, error) {
	// Decode only the resource declaration. Runner capabilities and user env
	// are deliberately neither retained nor carried into the queue snapshot.
	var info struct {
		Traits struct {
			Resources  *spec.ResourceTraitsSpec `json:"resources"`
			Evaluation *struct {
				Concurrency int                      `json:"concurrency"`
				Resources   *spec.ResourceTraitsSpec `json:"sandboxResources"`
			} `json:"eval"`
		} `json:"traits"`
	}
	if json.Unmarshal([]byte(job.EvaluationInfo), &info) != nil || info.Traits.Evaluation == nil || info.Traits.Evaluation.Concurrency < 1 || info.Traits.Evaluation.Concurrency > 16 {
		return nil, nil, 0, fmt.Errorf("evaluation resource declaration unavailable")
	}
	return info.Traits.Resources, info.Traits.Evaluation.Resources, info.Traits.Evaluation.Concurrency, nil
}

func traitResourceDemand(traits *spec.ResourceTraitsSpec) corev1.ResourceList {
	if traits == nil {
		return nil
	}
	resources := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")}
	for key, value := range map[corev1.ResourceName]string{corev1.ResourceRequestsCPU: traits.CPU, corev1.ResourceRequestsMemory: traits.Memory,
		corev1.ResourceLimitsCPU: traits.CPULimit, corev1.ResourceLimitsMemory: traits.MemoryLimit} {
		q, err := resource.ParseQuantity(value)
		if err != nil || q.Sign() <= 0 {
			return nil
		}
		resources[key] = q
	}
	return resources
}

func scheduledJobResourceDemand(job *model.JobInfo) corev1.ResourceList {
	if resourceFreeJob(job.Type) {
		return corev1.ResourceList{}
	}
	var demand corev1.ResourceList
	if job.SchedulingResources != "" {
		if err := json.Unmarshal([]byte(job.SchedulingResources), &demand); err != nil {
			return nil
		}
		for _, q := range demand {
			if q.Sign() < 0 {
				return nil
			}
		}
		if pods, ok := demand[corev1.ResourcePods]; !ok || pods.Sign() <= 0 {
			return nil
		}
	}
	if job.Type != string(config.JobEval) {
		return demand
	}
	runner, trial, concurrency, err := evaluationResourceTraits(job)
	if err != nil {
		return nil
	}
	if demand == nil {
		demand = traitResourceDemand(runner) // Legacy evaluation CPU/memory remain reconstructible.
	}
	trialDemand := traitResourceDemand(trial)
	if demand == nil || trialDemand == nil {
		return nil
	}
	for key, q := range trialDemand {
		q.Mul(int64(concurrency))
		sum, ok := demand[key]
		if !ok {
			continue
		}
		sum.Add(q)
		demand[key] = sum
	}
	// Harbor determines each trial's storage at allocation time. Until that
	// declaration exists, an ephemeral-storage quota cannot be safely reserved.
	delete(demand, corev1.ResourceRequestsEphemeralStorage)
	delete(demand, corev1.ResourceLimitsEphemeralStorage)
	return demand
}

func (b *jobResourceBudget) add(workspace string, demand corev1.ResourceList) {
	if b == nil {
		return
	}
	if demand == nil {
		b.unknown[workspace] = true
		return
	}
	if len(demand) == 0 { // A non-Pod configuration/cleanup controller.
		return
	}
	if b.used[workspace] == nil {
		b.used[workspace] = corev1.ResourceList{}
	}
	for _, key := range b.keys {
		q, ok := demand[key]
		if !ok {
			b.unknown[workspace] = true
			continue
		}
		sum := b.used[workspace][key]
		sum.Add(q)
		b.used[workspace][key] = sum
	}
}

func (b *jobResourceBudget) reason(workspace string, demand corev1.ResourceList) string {
	if b == nil || (demand != nil && len(demand) == 0) {
		return ""
	}
	if demand == nil {
		return "resource declaration unavailable for workspace admission"
	}
	if b.unknown[workspace] {
		return "workspace has an active resource reservation with unknown size"
	}
	for _, key := range b.keys {
		limit := b.quota[key]
		q, ok := demand[key]
		if !ok {
			return "resource declaration unavailable for quota " + string(key)
		}
		total := b.used[workspace][key].DeepCopy()
		total.Add(q)
		if total.Cmp(limit) > 0 {
			return "workspace resource budget exhausted: " + string(key)
		}
	}
	return ""
}

func (b *jobResourceBudget) candidateDemand(job *model.JobInfo) corev1.ResourceList {
	demand := b.candidates[job.ID]
	if job.ExecutionKey == nil || demand == nil {
		return demand
	}
	retained := b.retainedExecutions[*job.ExecutionKey]
	if len(retained) == 0 {
		return demand
	}
	// A recovered execution already owns the retained slots counted above.
	// Reserve only its missing bundle portion, including the Runner itself.
	demand = demand.DeepCopy()
	for key, existing := range retained {
		if q, ok := demand[key]; ok {
			q.Sub(existing)
			if q.Sign() < 0 {
				q = resource.Quantity{}
			}
			demand[key] = q
		}
	}
	return demand
}

// Retained trial environments outlive their Runner admission. Charge only
// executions without an active bundle, so a live trial is not counted twice.
func (b *jobResourceBudget) addRetainedSandboxes(ctx context.Context, tx datastore.DataStore) error {
	if b == nil {
		return nil
	}
	lastID := ""
	for {
		opts := &datastore.ListOptions{Page: 1, PageSize: jobSchedulerBatchSize, SortBy: []datastore.SortOption{{Key: "id", Order: datastore.SortOrderDescending}}}
		if lastID != "" {
			opts.LessThan = []datastore.ComparisonQueryOption{{Key: "id", Value: lastID}}
		}
		rows, err := tx.List(ctx, &model.JobSandbox{SlotReserved: true}, opts)
		if err != nil {
			return fmt.Errorf("load retained Sandbox resource reservations: %w", err)
		}
		ids := make([]string, 0, len(rows))
		for _, entity := range rows {
			row, ok := entity.(*model.JobSandbox)
			if !ok || row == nil || row.ID == "" || (lastID != "" && row.ID >= lastID) {
				return fmt.Errorf("invalid Sandbox resource reservation page")
			}
			lastID = row.ID
			if !b.activeExecutions[row.ExecutionKey] {
				ids = append(ids, fmt.Sprint(row.JobID))
			}
		}
		jobs := map[int]*model.JobInfo{}
		if len(ids) > 0 {
			parents, err := tx.List(ctx, &model.JobInfo{}, &datastore.ListOptions{Page: 1, PageSize: len(ids), FilterOptions: datastore.FilterOptions{In: []datastore.InQueryOption{{Key: "id", Values: ids}}}})
			if err != nil {
				return fmt.Errorf("load retained Sandbox declarations: %w", err)
			}
			for _, entity := range parents {
				job, ok := entity.(*model.JobInfo)
				if !ok || job == nil {
					return datastore.ErrEntityInvalid
				}
				jobs[job.ID] = job
			}
		}
		for _, entity := range rows {
			row := entity.(*model.JobSandbox)
			if b.activeExecutions[row.ExecutionKey] {
				continue
			}
			job := jobs[row.JobID]
			if job == nil || job.WorkspaceID != row.WorkspaceID || job.ExecutionKey == nil || *job.ExecutionKey != row.ExecutionKey || job.Type != string(config.JobEval) {
				b.add(row.WorkspaceID, nil)
				continue
			}
			_, trial, _, err := evaluationResourceTraits(job)
			demand := traitResourceDemand(trial)
			if err != nil || demand == nil || row.StorageMiB <= 0 {
				b.add(row.WorkspaceID, nil)
				continue
			}
			storage := *resource.NewQuantity(row.StorageMiB*1024*1024, resource.BinarySI)
			demand[corev1.ResourceRequestsEphemeralStorage], demand[corev1.ResourceLimitsEphemeralStorage] = storage.DeepCopy(), storage.DeepCopy()
			b.add(row.WorkspaceID, demand)
			if b.retainedExecutions[row.ExecutionKey] == nil {
				b.retainedExecutions[row.ExecutionKey] = corev1.ResourceList{}
			}
			for key, q := range demand {
				sum := b.retainedExecutions[row.ExecutionKey][key]
				sum.Add(q)
				b.retainedExecutions[row.ExecutionKey][key] = sum
			}
		}
		if len(rows) < jobSchedulerBatchSize {
			return nil
		}
	}
}
