package workflow

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	workflowconfig "github.com/PixelCores/Eruun/pkg/apiserver/workflow/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

func NewJobTask(name, namespace, workflowID, projectID, appID, taskID string, timeoutSeconds int64, resourceAppName ...string) *model.JobTask {
	if timeoutSeconds <= 0 {
		timeoutSeconds = int64(config.DefaultJobTaskTimeout)
	}
	jobTask := &model.JobTask{
		Name:       name,
		Namespace:  namespace,
		WorkflowID: workflowID,
		ProjectID:  projectID,
		AppID:      appID,
		TaskID:     taskID,
		Status:     config.StatusQueued,
		Timeout:    timeoutSeconds,
	}
	if len(resourceAppName) > 0 {
		jobTask.ResourceAppName = strings.TrimSpace(resourceAppName[0])
	}
	return jobTask
}

// setDeployTimeout forces deployment-related jobs to use the standard deploy timeout (20 minutes).
func setDeployTimeout(jobTask *model.JobTask) {
	if jobTask == nil {
		return
	}
	jobTask.Timeout = config.DeployTimeout
}

// setCloudJobTimeout forces cloudjob tasks to use cloudjob-specific timeout.
func setCloudJobTimeout(jobTask *model.JobTask) {
	if jobTask == nil {
		return
	}
	jobTask.Timeout = config.CloudJobTimeout
}

func CreateObjectJobsFromResult(additionalObjects []client.Object, component *model.ApplicationComponent, task *model.WorkflowQueue, jobs []*model.JobTask, defaultJobTimeoutSeconds int64) ([]*model.JobTask, error) {
	if len(additionalObjects) == 0 {
		return jobs, nil
	}

	share := shareConfigForComponent(component)
	rbacShare := rbacShareConfigForComponent(component, share)
	resourceAppName := component.ResourceNameKey()

	for _, obj := range additionalObjects {
		job.ApplyComponentAnnotationsToObject(obj, component)

		if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
			ns := pvc.Namespace
			if ns == "" {
				ns = component.Namespace
				pvc.Namespace = ns
			}
			applyShareLabelsToObject(pvc, share)
			pvcJob := NewJobTask(
				pvc.Name,
				ns,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			pvcJob.JobType = string(config.JobDeployPVC)
			pvcJob.JobInfo = pvc
			pvcJob.Info = buildResourceInfo(config.ResourcePVC, ns, pvc.Name)
			setDeployTimeout(pvcJob)
			markJobSkippedIfIgnored(share, pvcJob)

			jobs = append(jobs, pvcJob)
			klog.Infof("Created PVC job for component %s: %s", component.Name, pvc.Name)
		}
		if ingress, ok := obj.(*networkingv1.Ingress); ok {
			baseName := nameOrFallback(ingress.Name, component.Name)
			normalizedName := job.BuildIngressName(baseName, resourceAppName)
			ingress.Name = normalizedName
			if ingress.Namespace == "" {
				ingress.Namespace = component.Namespace
			}
			properties := job.ParseProperties(component.Properties)
			labels := job.BuildLabels(component, &properties)
			for k, v := range naming.NormalizeLabelValues(ingress.GetLabels()) {
				labels[k] = v
			}
			ingress.SetLabels(job.ApplyComponentManagedLabels(labels, component))
			applyShareLabelsToObject(ingress, share)
			ingressJob := NewJobTask(
				ingress.Name,
				ingress.Namespace,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			ingressJob.JobType = string(config.JobDeployIngress)
			ingressJob.JobInfo = ingress
			ingressJob.Info = buildIngressDeployInfo(ingress, ingress.Name, ingress.Namespace)
			setDeployTimeout(ingressJob)
			markJobSkippedIfIgnored(share, ingressJob)
			jobs = append(jobs, ingressJob)
			klog.Infof("Created Ingress job for component %s: %s", component.Name, ingress.Name)
		}
		if sa, ok := obj.(*corev1.ServiceAccount); ok {
			ns := sa.Namespace
			if ns == "" {
				ns = component.Namespace
				sa.Namespace = ns
			}
			applyShareLabelsToObject(sa, rbacShare)
			jobTask := NewJobTask(
				sa.Name,
				ns,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			jobTask.JobType = string(config.JobDeployServiceAccount)
			jobTask.JobInfo = sa.DeepCopy()
			jobTask.Info = buildResourceInfo(config.ResourceServiceAccount, ns, sa.Name)
			setDeployTimeout(jobTask)
			markJobSkippedIfIgnored(rbacShare, jobTask)
			jobs = append(jobs, jobTask)
			klog.Infof("Created ServiceAccount job for component %s: %s/%s", component.Name, ns, sa.Name)
		}
		if role, ok := obj.(*rbacv1.Role); ok {
			ns := role.Namespace
			if ns == "" {
				ns = component.Namespace
				role.Namespace = ns
			}
			applyShareLabelsToObject(role, rbacShare)
			jobTask := NewJobTask(
				role.Name,
				ns,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			jobTask.JobType = string(config.JobDeployRole)
			jobTask.JobInfo = role.DeepCopy()
			jobTask.Info = buildResourceInfo(config.ResourceRole, ns, role.Name)
			setDeployTimeout(jobTask)
			markJobSkippedIfIgnored(rbacShare, jobTask)
			jobs = append(jobs, jobTask)
			klog.Infof("Created Role job for component %s: %s/%s", component.Name, ns, role.Name)
		}
		if binding, ok := obj.(*rbacv1.RoleBinding); ok {
			ns := binding.Namespace
			if ns == "" {
				ns = component.Namespace
				binding.Namespace = ns
			}
			applyShareLabelsToObject(binding, rbacShare)
			jobTask := NewJobTask(
				binding.Name,
				ns,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			jobTask.JobType = string(config.JobDeployRoleBinding)
			jobTask.JobInfo = binding.DeepCopy()
			jobTask.Info = buildResourceInfo(config.ResourceRoleBinding, ns, binding.Name)
			setDeployTimeout(jobTask)
			markJobSkippedIfIgnored(rbacShare, jobTask)
			jobs = append(jobs, jobTask)
			klog.Infof("Created RoleBinding job for component %s: %s/%s", component.Name, ns, binding.Name)
		}
		if clusterRole, ok := obj.(*rbacv1.ClusterRole); ok {
			applyShareLabelsToObject(clusterRole, rbacShare)
			jobTask := NewJobTask(
				clusterRole.Name,
				component.Namespace,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			jobTask.JobType = string(config.JobDeployClusterRole)
			jobTask.JobInfo = clusterRole.DeepCopy()
			jobTask.Info = buildResourceInfo(config.ResourceClusterRole, "", clusterRole.Name)
			setDeployTimeout(jobTask)
			markJobSkippedIfIgnored(rbacShare, jobTask)
			jobs = append(jobs, jobTask)
			klog.Infof("Created ClusterRole job for component %s: %s", component.Name, clusterRole.Name)
		}
		if clusterBinding, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
			applyShareLabelsToObject(clusterBinding, rbacShare)
			jobTask := NewJobTask(
				clusterBinding.Name,
				component.Namespace,
				task.WorkflowID,
				task.ProjectID,
				task.AppID,
				task.TaskID,
				defaultJobTimeoutSeconds,
				resourceAppName,
			)
			jobTask.JobType = string(config.JobDeployClusterRoleBinding)
			jobTask.JobInfo = clusterBinding.DeepCopy()
			jobTask.Info = buildResourceInfo(config.ResourceClusterRoleBinding, "", clusterBinding.Name)
			setDeployTimeout(jobTask)
			markJobSkippedIfIgnored(rbacShare, jobTask)
			jobs = append(jobs, jobTask)
			klog.Infof("Created ClusterRoleBinding job for component %s: %s", component.Name, clusterBinding.Name)
		}
	}
	return jobs, nil
}

func appendComponentGroup(
	ctx context.Context,
	buckets map[int][]*model.JobTask,
	componentNames []string,
	workflowType config.JobType,
	workflowProperties []model.Policies,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	executionKeyForComponent func(componentIndex int) string,
) {
	logger := klog.FromContext(ctx)
	if workflowType == config.JobDatabaseReset {
		executionKey := ""
		if executionKeyForComponent != nil {
			executionKey = executionKeyForComponent(0)
		}
		mergeJobBuckets(buckets, buildDatabaseResetJobs(ctx, componentNames, workflowProperties, componentMap, task, defaultJobTimeoutSeconds, executionKey))
		return
	}
	if workflowType == config.JobLogArchiveUpload {
		mergeJobBuckets(buckets, buildLogArchiveUploadJobs(ctx, componentNames, componentMap, task, defaultJobTimeoutSeconds, workflowProperties))
		return
	}
	for componentIndex, name := range componentNames {
		component, ok := componentMap[name]
		if !ok {
			logger.Info("Component referenced in workflow step not found", "componentName", name)
			continue
		}
		executionKey := ""
		if executionKeyForComponent != nil {
			executionKey = executionKeyForComponent(componentIndex)
		}
		if workflowType == config.JobCleanupResources {
			mergeJobBuckets(buckets, buildCleanupJobsForComponent(component, task, defaultJobTimeoutSeconds))
			continue
		}
		componentBuckets := buildJobsForComponent(ctx, component, task, defaultJobTimeoutSeconds, executionKey)
		mergeJobBuckets(buckets, componentBuckets)
	}
}

func buildCleanupJobsForComponent(
	component *model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
) map[int][]*model.JobTask {
	buckets := newJobBuckets()
	if component == nil {
		return buckets
	}
	namespace := component.Namespace
	if namespace == "" {
		namespace = config.DefaultNamespace
	}
	componentCopy := *component
	componentCopy.Namespace = namespace
	jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, component.ResourceNameKey())
	jobTask.JobType = string(config.JobCleanupResources)
	jobTask.JobInfo = &componentCopy
	jobTask.Info = fmt.Sprintf("cleanup resources: %s/%s", namespace, component.Name)
	setDeployTimeout(jobTask)
	buckets[config.JobPriorityLow] = append(buckets[config.JobPriorityLow], jobTask)
	return buckets
}

func buildLogArchiveUploadJobs(
	ctx context.Context,
	componentNames []string,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	workflowProperties []model.Policies,
) map[int][]*model.JobTask {
	logger := klog.FromContext(ctx)
	buckets := newJobBuckets()
	if task == nil || len(componentNames) == 0 {
		return buckets
	}

	seen := make(map[string]struct{}, len(componentNames))
	for _, name := range componentNames {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		component, ok := componentMap[name]
		if !ok || component == nil {
			logger.Info("Log archive upload component referenced in workflow step not found", "componentName", name)
			continue
		}
		path, container := logArchiveUploadOptionsForComponent(component.Name, workflowProperties)
		componentCopy := cloneLogArchiveUploadComponent(component)
		namespace := componentCopy.Namespace
		jobTask := NewJobTask(componentCopy.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, componentCopy.ResourceNameKey())
		jobTask.JobType = string(config.JobLogArchiveUpload)
		jobTask.JobInfo = &job.LogArchiveUploadJobInfo{
			Component: componentCopy,
			Path:      strings.TrimSpace(path),
			Container: strings.TrimSpace(container),
		}
		jobTask.Info = fmt.Sprintf("log archive upload: %s/%s %s", namespace, componentCopy.Name, strings.TrimSpace(path))
		setDeployTimeout(jobTask)
		buckets[config.JobPriorityLow] = append(buckets[config.JobPriorityLow], jobTask)
	}
	return buckets
}

func cloneLogArchiveUploadComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return nil
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}

func logArchiveUploadOptionsForComponent(componentName string, properties []model.Policies) (string, string) {
	normalizedComponentName := strings.ToLower(strings.TrimSpace(componentName))
	var fallbackPath string
	var fallbackContainer string
	for _, property := range properties {
		path := strings.TrimSpace(property.Path)
		container := strings.TrimSpace(property.Container)
		if fallbackPath == "" && path != "" {
			fallbackPath = path
			fallbackContainer = container
		}
		for _, policy := range property.Policies {
			if strings.ToLower(strings.TrimSpace(policy)) == normalizedComponentName {
				return path, container
			}
		}
	}
	return fallbackPath, fallbackContainer
}

func buildDatabaseResetJobs(
	ctx context.Context,
	componentNames []string,
	workflowProperties []model.Policies,
	componentMap map[string]*model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	executionKey string,
) map[int][]*model.JobTask {
	logger := klog.FromContext(ctx)
	buckets := newJobBuckets()
	if task == nil || len(componentNames) == 0 {
		return buckets
	}

	databaseComponents := make([]*model.ApplicationComponent, 0, len(componentNames))
	seenDatabases := make(map[string]struct{}, len(componentNames))
	for _, name := range componentNames {
		component, ok := componentMap[name]
		if !ok || component == nil {
			logger.Info("Database reset component referenced in workflow step not found", "componentName", name)
			continue
		}
		if component.ComponentType != config.StoreJob {
			logger.Info("Database reset skips non-store component", "componentName", component.Name, "componentType", component.ComponentType)
			continue
		}
		key := strings.ToLower(strings.TrimSpace(component.Name))
		if _, ok := seenDatabases[key]; ok {
			continue
		}
		seenDatabases[key] = struct{}{}
		databaseComponents = append(databaseComponents, cloneDatabaseResetComponent(component))
	}
	if len(databaseComponents) == 0 {
		return buckets
	}

	namespace := databaseComponents[0].Namespace
	if strings.TrimSpace(namespace) == "" {
		namespace = config.DefaultNamespace
	}
	resourceAppName := databaseComponents[0].ResourceNameKey()
	jobTask := NewJobTask(databaseResetWorkflowName(), namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
	jobTask.JobType = string(config.JobDatabaseReset)
	jobTask.JobInfo = &job.DatabaseResetJobInfo{
		DatabaseComponents: databaseComponents,
		InitSQLURL:         databaseResetInitSQLURL(workflowProperties),
		ExecutionKey:       strings.TrimSpace(executionKey),
	}
	jobTask.Info = fmt.Sprintf("database reset: %s", strings.Join(componentNamesForInfo(databaseComponents), ", "))
	setDeployTimeout(jobTask)
	buckets[config.JobPriorityLow] = append(buckets[config.JobPriorityLow], jobTask)
	return buckets
}

func databaseResetInitSQLURL(properties []model.Policies) string {
	for _, property := range properties {
		if value := strings.TrimSpace(property.InitSQLURL); value != "" {
			return value
		}
	}
	return ""
}

func cloneDatabaseResetComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return nil
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}

func componentNamesForInfo(components []*model.ApplicationComponent) []string {
	names := make([]string, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		names = append(names, component.Name)
	}
	return names
}

func databaseResetWorkflowName() string {
	return "database-reset"
}

func buildJobsForComponent(
	ctx context.Context,
	component *model.ApplicationComponent,
	task *model.WorkflowQueue,
	defaultJobTimeoutSeconds int64,
	cloudExecutionKey string,
) map[int][]*model.JobTask {
	logger := klog.FromContext(ctx)
	buckets := newJobBuckets()
	if component == nil {
		return buckets
	}

	namespace := component.Namespace
	if namespace == "" {
		namespace = config.DefaultNamespace
		component.Namespace = namespace
	}

	properties := ParseProperties(ctx, component.Properties)
	share := shareConfigForComponent(component)
	resourceAppName := component.ResourceNameKey()

	switch component.ComponentType {
	case config.ServerJob:
		serviceJobs := job.GenerateWebService(component, &properties)
		queueServiceJobs(logger, buckets, component, task, namespace, config.JobDeploy, serviceJobs, defaultJobTimeoutSeconds, share)
	case config.StoreJob:
		storeJobs := job.GenerateStoreService(component)
		queueServiceJobs(logger, buckets, component, task, namespace, config.JobDeployStore, storeJobs, defaultJobTimeoutSeconds, share)

	case config.ConfJob:
		jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
		jobTask.JobType = string(config.JobDeployConfigMap)
		jobTask.JobInfo = job.GenerateConfigMap(component, &properties)
		applyShareLabelsToJobInfo(jobTask.JobInfo, share)
		jobTask.Info = buildConfigLikeInfo(config.ResourceConfigMap, jobTask.JobInfo, namespace, component.Name)
		setDeployTimeout(jobTask)
		markJobSkippedIfIgnored(share, jobTask)
		buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)

	case config.SecretJob:
		jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
		jobTask.JobType = string(config.JobDeploySecret)
		jobTask.JobInfo = job.GenerateSecret(component, &properties)
		applyShareLabelsToJobInfo(jobTask.JobInfo, share)
		jobTask.Info = buildConfigLikeInfo(config.ResourceSecret, jobTask.JobInfo, namespace, component.Name)
		setDeployTimeout(jobTask)
		markJobSkippedIfIgnored(share, jobTask)
		buckets[config.JobPriorityMaxHigh] = append(buckets[config.JobPriorityMaxHigh], jobTask)
	case config.CloudJob:
		cloudInfo := &job.CloudJobInfo{}
		if properties.Cloud == nil {
			logger.Info("Cloudjob component missing properties.cloud, enqueuing fail-fast cloud job", "componentName", component.Name)
		} else {
			cloudInfo.Provider = properties.Cloud.Provider
			cloudInfo.Action = properties.Cloud.Action
			cloudInfo.Params = cloneMapInterface(properties.Cloud.Params)
		}
		cloudInfo.ExecutionKey = strings.TrimSpace(cloudExecutionKey)
		jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
		jobTask.JobType = string(config.JobDeployCloud)
		jobTask.JobInfo = cloudInfo
		jobTask.Info = buildResourceInfo(config.ResourceCloudJob, namespace, component.Name)
		setCloudJobTimeout(jobTask)
		buckets[config.JobPriorityNormal] = append(buckets[config.JobPriorityNormal], jobTask)
	case config.InstantJob:
		if properties.StartTime > 0 {
			result := job.GenerateOneTimeJob(component, &properties, properties.RunPolicy, properties.StartTime)
			fallbackName := naming.JobName(component.Name, resourceAppName)
			jobTask := appendBatchJob(logger, buckets, component, task, namespace, config.InstantJob, config.JobDeployInstant, result, defaultJobTimeoutSeconds, share, fallbackName)
			applyJobFailurePolicyOverride(jobTask, properties.FailurePolicy)
			break
		}
		result := job.GenerateInstantJob(component, &properties, properties.RunPolicy)
		fallbackName := naming.JobName(component.Name, resourceAppName)
		jobTask := appendBatchJob(logger, buckets, component, task, namespace, config.InstantJob, config.JobDeployInstant, result, defaultJobTimeoutSeconds, share, fallbackName)
		applyJobFailurePolicyOverride(jobTask, properties.FailurePolicy)
	case config.ScheduledJob:
		if schedule := strings.TrimSpace(properties.Schedule); schedule != "" {
			normalized, err := utils.NormalizeCronSchedule(schedule)
			if err != nil {
				logger.Error(err, "Invalid cron schedule for scheduled job", "componentName", component.Name)
				break
			}
			result := job.GenerateScheduledCronJob(component, &properties, normalized)
			fallbackName := naming.CronJobName(component.Name, resourceAppName)
			appendBatchJob(logger, buckets, component, task, namespace, config.ScheduledJob, config.JobDeployScheduled, result, defaultJobTimeoutSeconds, share, fallbackName)
			break
		}
	}

	if component.ComponentType != config.InstantJob && component.ComponentType != config.ScheduledJob && component.ComponentType != config.CloudJob {
		serviceTraits := serviceTraitsForComponent(component, &properties)
		if len(serviceTraits) > 0 {
			for _, trait := range serviceTraits {
				svcName := strings.TrimSpace(trait.Name)
				if svcName == "" {
					svcName = component.Name
				}
				svcJob := NewJobTask(svcName, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
				svcJob.JobType = string(config.JobDeployService)
				svcInfo := job.GenerateServiceFromTrait(component, &properties, trait)
				svcJob.JobInfo = svcInfo
				applyShareLabelsToJobInfo(svcJob.JobInfo, share)
				svcJob.Info = buildServiceDeployInfo(svcInfo, svcName, namespace)
				setDeployTimeout(svcJob)
				markJobSkippedIfIgnored(share, svcJob)
				buckets[config.JobPriorityHigh] = append(buckets[config.JobPriorityHigh], svcJob)
			}
		} else if len(properties.Ports) > 0 {
			svcJob := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, resourceAppName)
			svcJob.JobType = string(config.JobDeployService)
			svcInfo := job.GenerateService(component, &properties)
			svcJob.JobInfo = svcInfo
			applyShareLabelsToJobInfo(svcJob.JobInfo, share)
			svcJob.Info = buildServiceDeployInfo(svcInfo, component.Name, namespace)
			setDeployTimeout(svcJob)
			markJobSkippedIfIgnored(share, svcJob)
			buckets[config.JobPriorityHigh] = append(buckets[config.JobPriorityHigh], svcJob)
		}
	}

	return buckets
}

func queueServiceJobs(
	logger klog.Logger,
	buckets map[int][]*model.JobTask,
	component *model.ApplicationComponent,
	task *model.WorkflowQueue,
	namespace string,
	jobType config.JobType,
	result *job.GenerateServiceResult,
	defaultJobTimeoutSeconds int64,
	share shareConfig,
) {
	if result == nil {
		return
	}

	appendJob := func(priority int, jobTask *model.JobTask) {
		if jobTask == nil {
			return
		}
		buckets[priority] = append(buckets[priority], jobTask)
	}

	// Traits may emit extra Kubernetes objects (PVC, Ingress, etc.). Schedule them
	// ahead of the base workload so dependencies are ready before the deployment runs.
	if len(result.AdditionalObjects) > 0 {
		jobs, err := CreateObjectJobsFromResult(result.AdditionalObjects, component, task, nil, defaultJobTimeoutSeconds)
		if err != nil {
			logger.Error(err, "Failed to create additional resource jobs", "componentName", component.Name)
		} else {
			for _, jt := range jobs {
				appendJob(config.JobPriorityHigh, jt)
			}
		}
	}

	jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, component.ResourceNameKey())
	jobTask.JobType = string(jobType)
	jobTask.JobInfo = result.Service
	applyShareLabelsToJobInfo(jobTask.JobInfo, share)
	jobTask.Info = buildWorkloadInfo(jobType, result.Service, namespace, jobTask.Name)
	setDeployTimeout(jobTask)
	applyVersionUpdateImageReadyTimeout(jobTask, component, task)
	markJobSkippedIfIgnored(share, jobTask)
	appendJob(config.JobPriorityNormal, jobTask)
}

func applyVersionUpdateImageReadyTimeout(jobTask *model.JobTask, component *model.ApplicationComponent, task *model.WorkflowQueue) {
	if jobTask == nil || component == nil || task == nil {
		return
	}
	timeout, ok := versionUpdateImageReadyTimeoutForComponent(task, component.Name)
	if !ok {
		return
	}
	jobTask.Timeout = timeout
}

func appendBatchJob(
	logger klog.Logger,
	buckets map[int][]*model.JobTask,
	component *model.ApplicationComponent,
	task *model.WorkflowQueue,
	namespace string,
	jobType config.JobType,
	jobTaskType config.JobType,
	result *job.GenerateServiceResult,
	defaultJobTimeoutSeconds int64,
	share shareConfig,
	infoFallbackName string,
) *model.JobTask {
	if result == nil {
		return nil
	}

	if len(result.AdditionalObjects) > 0 {
		jobs, err := CreateObjectJobsFromResult(result.AdditionalObjects, component, task, nil, defaultJobTimeoutSeconds)
		if err != nil {
			logger.Error(err, "Failed to create additional resource jobs", "componentName", component.Name)
		} else {
			for _, jt := range jobs {
				buckets[config.JobPriorityHigh] = append(buckets[config.JobPriorityHigh], jt)
			}
		}
	}

	jobTask := NewJobTask(component.Name, namespace, task.WorkflowID, task.ProjectID, task.AppID, task.TaskID, defaultJobTimeoutSeconds, component.ResourceNameKey())
	jobTask.JobType = string(jobTaskType)
	jobTask.JobInfo = result.Service
	applyShareLabelsToJobInfo(jobTask.JobInfo, share)
	applyJobTaskMetadata(jobTask)
	jobTask.Info = buildWorkloadInfo(jobType, result.Service, namespace, infoFallbackName)
	markJobSkippedIfIgnored(share, jobTask)
	buckets[config.JobPriorityNormal] = append(buckets[config.JobPriorityNormal], jobTask)
	return jobTask
}

func markJobSkippedIfIgnored(share shareConfig, jobTask *model.JobTask) {
	if jobTask == nil {
		return
	}
	if share.ignore() {
		jobTask.Status = config.StatusSkipped
		jobTask.Error = ""
	}
}

func applyJobFailurePolicyOverride(jobTask *model.JobTask, policy *workflowconfig.WorkflowFailurePolicy) {
	if jobTask == nil || policy == nil {
		return
	}
	normalized, ok := workflowconfig.NormalizeJobFailurePolicy(*policy)
	if !ok {
		return
	}
	jobTask.FailurePolicy = normalized
}

func applyJobTaskMetadata(jobTask *model.JobTask) {
	job.ApplyTaskIDAnnotation(jobTask)
}
