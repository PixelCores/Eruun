package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

type DatabaseResetJobInfo struct {
	DatabaseComponents []*model.ApplicationComponent
	// RestartComponents is retained for compatibility with legacy job payloads.
	// Database reset execution intentionally ignores this field.
	RestartComponents []*model.ApplicationComponent
	InitSQLURL        string
	ExecutionKey      string
}

const databaseResetReplicaCheckpointVersion = 2

type databaseResetReplicaCheckpoint struct {
	Version          int              `json:"version"`
	ExecutionKey     string           `json:"executionKey"`
	Prepared         bool             `json:"prepared"`
	OriginalReplicas map[string]int32 `json:"originalReplicas,omitempty"`
}

type DatabaseResetJobCtl struct {
	deployNamespacedResourceJobBase
	runtime *jobRuntime
}

type pvcResetTarget struct {
	namespace string
	name      string
	recreate  bool
}

type databaseResetPlan struct {
	component                    *model.ApplicationComponent
	namespace                    string
	name                         string
	statefulSet                  *appsv1.StatefulSet
	pvcTargets                   []pvcResetTarget
	hasInitSQLURL                bool
	originalReplicas             int32
	originalReplicasCheckpointed bool
}

func NewDatabaseResetJobCtl(job *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *DatabaseResetJobCtl {
	base, ok := newDeployNamespacedResourceJobBase("DatabaseResetJobCtl", job, client, store, ack, nil)
	if !ok {
		return nil
	}
	return &DatabaseResetJobCtl{deployNamespacedResourceJobBase: base}
}

func (c *DatabaseResetJobCtl) setRuntime(runtime *jobRuntime) {
	if c == nil {
		return
	}
	c.runtime = runtime
	c.deployNamespacedResourceJobBase.setRuntime(runtime)
}

func (c *DatabaseResetJobCtl) Clean(context.Context) {}

func (c *DatabaseResetJobCtl) SaveInfo(ctx context.Context) error {
	if err := ensureDatabaseResetCheckpointIdentity(c.job); err != nil {
		return fmt.Errorf("prepare database reset replica checkpoint identity: %w", err)
	}
	return saveOrUpdateJobInfo(ctx, c.store, c.job)
}

func (c *DatabaseResetJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "database reset job run error")
}

func (c *DatabaseResetJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.store == nil {
		return fmt.Errorf("store is nil")
	}
	info, err := requiredJobInfo[*DatabaseResetJobInfo](c.job)
	if err != nil {
		return err
	}
	if databaseResetExecutionKey(info) == "" {
		return fmt.Errorf("database reset execution key is missing")
	}
	if len(info.DatabaseComponents) == 0 {
		return fmt.Errorf("database reset requires at least one database component")
	}
	initSQLURL := strings.TrimSpace(info.InitSQLURL)
	plans, err := c.prepareDatabaseResetPlans(ctx, info.DatabaseComponents, initSQLURL)
	if err != nil {
		return err
	}
	if err := c.checkpointDatabaseResetReplicas(ctx, plans); err != nil {
		return err
	}

	for _, plan := range plans {
		component := plan.component
		if err := c.markComponentRuntime(ctx, component, config.ComponentStatusPending, 0, ""); err != nil {
			return err
		}
		if err := c.resetDatabaseComponent(ctx, plan, initSQLURL); err != nil {
			markErr := c.markComponentRuntime(ctx, component, config.ComponentStatusFailed, 0, err.Error())
			return errors.Join(err, markErr)
		}
		if err := c.markComponentRuntime(ctx, component, config.ComponentStatusRunning, desiredDatabaseReplicas(component), ""); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatabaseResetJobCtl) checkpointDatabaseResetReplicas(ctx context.Context, plans []databaseResetPlan) error {
	checkpoint, err := c.loadDatabaseResetReplicaCheckpoint(ctx)
	if err != nil {
		return err
	}
	if checkpoint == nil || !checkpoint.Prepared {
		checkpoint = &databaseResetReplicaCheckpoint{
			Version:          databaseResetReplicaCheckpointVersion,
			ExecutionKey:     databaseResetExecutionKeyFromJob(c.job),
			Prepared:         true,
			OriginalReplicas: make(map[string]int32, len(plans)),
		}
		for i := range plans {
			key := databaseResetReplicaCheckpointKey(plans[i].namespace, plans[i].name)
			replicas := desiredReplicasFromStatefulSet(plans[i].statefulSet)
			checkpoint.OriginalReplicas[key] = replicas
			plans[i].originalReplicas = replicas
			plans[i].originalReplicasCheckpointed = true
		}
		raw, marshalErr := json.Marshal(checkpoint)
		if marshalErr != nil {
			return fmt.Errorf("marshal database reset replica checkpoint: %w", marshalErr)
		}
		c.job.InternalInfo = string(raw)
		if saveErr := c.SaveInfo(ctx); saveErr != nil {
			return fmt.Errorf("persist database reset replica checkpoint: %w", saveErr)
		}
		return nil
	}

	for i := range plans {
		key := databaseResetReplicaCheckpointKey(plans[i].namespace, plans[i].name)
		replicas, ok := checkpoint.OriginalReplicas[key]
		if !ok {
			return fmt.Errorf("database reset replica checkpoint is missing statefulset %s", key)
		}
		if replicas < 0 {
			return fmt.Errorf("database reset replica checkpoint has invalid replicas %d for statefulset %s", replicas, key)
		}
		plans[i].originalReplicas = replicas
		plans[i].originalReplicasCheckpointed = true
	}
	return nil
}

func (c *DatabaseResetJobCtl) loadDatabaseResetReplicaCheckpoint(ctx context.Context) (*databaseResetReplicaCheckpoint, error) {
	candidates, err := loadJobInfos(ctx, c.store, c.job.TaskID, c.job.JobType, resolveJobServiceName(c.job))
	if err != nil {
		return nil, fmt.Errorf("load database reset replica checkpoint: %w", err)
	}
	existing, err := selectDatabaseResetCheckpointJobInfo(c.job, candidates)
	if err != nil {
		return nil, fmt.Errorf("select database reset replica checkpoint: %w", err)
	}
	if existing == nil {
		return nil, nil
	}
	raw := strings.TrimSpace(existing.InternalInfo)
	checkpoint, err := decodeDatabaseResetReplicaCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	c.job.InternalInfo = raw
	return checkpoint, nil
}

func ensureDatabaseResetCheckpointIdentity(job *model.JobTask) error {
	executionKey := databaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return fmt.Errorf("database reset execution key is missing")
	}
	raw := strings.TrimSpace(job.InternalInfo)
	if raw == "" {
		marker := databaseResetReplicaCheckpoint{
			Version:      databaseResetReplicaCheckpointVersion,
			ExecutionKey: executionKey,
			Prepared:     false,
		}
		encoded, err := json.Marshal(&marker)
		if err != nil {
			return fmt.Errorf("marshal database reset replica checkpoint identity: %w", err)
		}
		job.InternalInfo = string(encoded)
		return nil
	}
	checkpoint, err := decodeDatabaseResetReplicaCheckpoint(raw)
	if err != nil {
		return err
	}
	if checkpoint.ExecutionKey != executionKey {
		return fmt.Errorf("database reset replica checkpoint execution key %q does not match job execution key %q", checkpoint.ExecutionKey, executionKey)
	}
	return nil
}

func preservePreparedDatabaseResetCheckpointForSave(job *model.JobTask, existing, next *model.JobInfo) error {
	executionKey := databaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return fmt.Errorf("database reset execution key is missing")
	}
	if existing == nil || next == nil {
		return fmt.Errorf("database reset job info record is missing")
	}
	existingCheckpoint, err := decodeDatabaseResetReplicaCheckpoint(existing.InternalInfo)
	if err != nil {
		return fmt.Errorf("decode existing database reset replica checkpoint: %w", err)
	}
	if existingCheckpoint.ExecutionKey != executionKey {
		return fmt.Errorf("existing database reset replica checkpoint execution key %q does not match job execution key %q", existingCheckpoint.ExecutionKey, executionKey)
	}
	nextCheckpoint, err := decodeDatabaseResetReplicaCheckpoint(next.InternalInfo)
	if err != nil {
		return fmt.Errorf("decode next database reset replica checkpoint: %w", err)
	}
	if nextCheckpoint.ExecutionKey != executionKey {
		return fmt.Errorf("next database reset replica checkpoint execution key %q does not match job execution key %q", nextCheckpoint.ExecutionKey, executionKey)
	}
	if existingCheckpoint.Prepared && !nextCheckpoint.Prepared {
		next.InternalInfo = existing.InternalInfo
		job.InternalInfo = existing.InternalInfo
	}
	return nil
}

func decodeDatabaseResetReplicaCheckpoint(raw string) (*databaseResetReplicaCheckpoint, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("database reset replica checkpoint is empty")
	}
	var checkpoint databaseResetReplicaCheckpoint
	if err := json.Unmarshal([]byte(raw), &checkpoint); err != nil {
		return nil, fmt.Errorf("decode database reset replica checkpoint: %w", err)
	}
	if checkpoint.Version != databaseResetReplicaCheckpointVersion {
		return nil, fmt.Errorf("database reset replica checkpoint version %d is unsupported", checkpoint.Version)
	}
	checkpoint.ExecutionKey = strings.TrimSpace(checkpoint.ExecutionKey)
	if checkpoint.ExecutionKey == "" {
		return nil, fmt.Errorf("database reset replica checkpoint execution key is missing")
	}
	if checkpoint.Prepared && checkpoint.OriginalReplicas == nil {
		return nil, fmt.Errorf("database reset replica checkpoint original replicas are missing")
	}
	if !checkpoint.Prepared && checkpoint.OriginalReplicas != nil {
		return nil, fmt.Errorf("database reset replica checkpoint is not prepared but contains original replicas")
	}
	return &checkpoint, nil
}

func selectDatabaseResetCheckpointJobInfo(job *model.JobTask, candidates []*model.JobInfo) (*model.JobInfo, error) {
	executionKey := databaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return nil, fmt.Errorf("database reset execution key is missing")
	}
	var matched *model.JobInfo
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		checkpoint, err := decodeDatabaseResetReplicaCheckpoint(candidate.InternalInfo)
		if err != nil {
			return nil, fmt.Errorf("inspect database reset replica checkpoint record %d: %w", candidate.ID, err)
		}
		if checkpoint.ExecutionKey != executionKey {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("multiple database reset replica checkpoints found for execution key %q", executionKey)
		}
		matched = candidate
	}
	return matched, nil
}

func databaseResetExecutionKey(info *DatabaseResetJobInfo) string {
	if info == nil {
		return ""
	}
	return strings.TrimSpace(info.ExecutionKey)
}

func databaseResetExecutionKeyFromJob(job *model.JobTask) string {
	if job == nil {
		return ""
	}
	info, ok := job.JobInfo.(*DatabaseResetJobInfo)
	if !ok {
		return ""
	}
	return databaseResetExecutionKey(info)
}

func databaseResetReplicaCheckpointKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func (c *DatabaseResetJobCtl) prepareDatabaseResetPlans(ctx context.Context, components []*model.ApplicationComponent, initSQLURL string) ([]databaseResetPlan, error) {
	plans := make([]databaseResetPlan, 0, len(components))
	hasInitSQLTarget := false
	for _, rawComponent := range components {
		if rawComponent == nil {
			continue
		}
		if rawComponent.HasSourceWorkload() {
			return nil, fmt.Errorf("database reset is disabled for adopted component %s", rawComponent.Name)
		}
		component := normalizeDatabaseResetComponent(rawComponent)
		result := GenerateStoreService(component)
		if result == nil {
			return nil, fmt.Errorf("generate store service for component %s failed", component.Name)
		}
		statefulSet, ok := result.Service.(*appsv1.StatefulSet)
		if !ok || statefulSet == nil {
			return nil, fmt.Errorf("store component %s did not generate a StatefulSet", component.Name)
		}
		namespace := databaseResetNamespaceOrDefault(statefulSet.Namespace, component.Namespace)
		name := strings.TrimSpace(statefulSet.Name)
		if name == "" {
			name = buildStoreSeverName(component.Name, component.ResourceNameKey())
		}
		current, err := c.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get statefulset %s/%s: %w", namespace, name, err)
		}
		targets, err := c.collectPVCResetTargets(ctx, namespace, current, result.AdditionalObjects, desiredDatabaseReplicas(component))
		if err != nil {
			return nil, err
		}
		hasComponentInitSQLURL := statefulSetHasInitSQLURL(current)
		hasInitSQLTarget = hasInitSQLTarget || hasComponentInitSQLURL
		plans = append(plans, databaseResetPlan{
			component:     component,
			namespace:     namespace,
			name:          name,
			statefulSet:   current.DeepCopy(),
			pvcTargets:    targets,
			hasInitSQLURL: hasComponentInitSQLURL,
		})
	}
	if len(plans) == 0 {
		return nil, fmt.Errorf("database reset requires at least one valid database component")
	}
	if initSQLURL != "" && !hasInitSQLTarget {
		return nil, fmt.Errorf("database reset initSqlUrl has no matching init container SQL_URL target")
	}
	if initSQLURL == "" && hasInitSQLTarget {
		klog.InfoS("database reset initSqlUrl is empty; retaining existing StatefulSet SQL_URL values", "databaseComponents", len(plans))
	}
	return plans, nil
}

func statefulSetHasInitSQLURL(statefulSet *appsv1.StatefulSet) bool {
	if statefulSet == nil {
		return false
	}
	for _, container := range statefulSet.Spec.Template.Spec.InitContainers {
		for _, env := range container.Env {
			if env.Name == "SQL_URL" {
				return true
			}
		}
	}
	return false
}

func (c *DatabaseResetJobCtl) resetDatabaseComponent(ctx context.Context, plan databaseResetPlan, initSQLURL string) error {
	component := plan.component
	if err := c.scaleStatefulSet(ctx, plan.namespace, plan.name, 0); err != nil {
		return err
	}
	if err := c.waitStatefulSetPodsGone(ctx, plan.namespace, plan.statefulSet, component); err != nil {
		return err
	}
	if initSQLURL != "" && plan.hasInitSQLURL {
		if err := c.updateStatefulSetInitSQLURL(ctx, plan.namespace, plan.name, initSQLURL); err != nil {
			restoreErr := c.restoreDatabaseReplicas(ctx, plan)
			return errors.Join(err, restoreErr)
		}
	}
	for _, target := range plan.pvcTargets {
		if err := c.deleteAndMaybeRecreatePVC(ctx, target); err != nil {
			return err
		}
	}

	desiredReplicas := desiredDatabaseReplicas(component)
	if err := c.scaleStatefulSet(ctx, plan.namespace, plan.name, desiredReplicas); err != nil {
		return err
	}
	return c.waitDatabaseComponentReady(ctx, plan.namespace, plan.name, component, desiredReplicas)
}

func (c *DatabaseResetJobCtl) updateStatefulSetInitSQLURL(ctx context.Context, namespace, name, initSQLURL string) error {
	err := updateResourceWithRetry(ctx,
		func(getCtx context.Context) (*appsv1.StatefulSet, error) {
			return c.client.AppsV1().StatefulSets(namespace).Get(getCtx, name, metav1.GetOptions{})
		},
		func(updateCtx context.Context, latest *appsv1.StatefulSet) error {
			matched := false
			changed := false
			for containerIndex := range latest.Spec.Template.Spec.InitContainers {
				container := &latest.Spec.Template.Spec.InitContainers[containerIndex]
				for envIndex := range container.Env {
					env := &container.Env[envIndex]
					if env.Name != "SQL_URL" {
						continue
					}
					matched = true
					if strings.TrimSpace(env.Value) == initSQLURL && env.ValueFrom == nil {
						continue
					}
					env.Value = initSQLURL
					env.ValueFrom = nil
					changed = true
				}
			}
			if !matched {
				return fmt.Errorf("statefulset %s/%s no longer has an init container SQL_URL target", namespace, name)
			}
			if !changed {
				return nil
			}
			_, err := c.client.AppsV1().StatefulSets(namespace).Update(updateCtx, latest, metav1.UpdateOptions{})
			return err
		},
	)
	if err != nil {
		return fmt.Errorf("update statefulset %s/%s init SQL URL: %w", namespace, name, err)
	}
	return nil
}

func (c *DatabaseResetJobCtl) restoreDatabaseReplicas(ctx context.Context, plan databaseResetPlan) error {
	if !plan.originalReplicasCheckpointed {
		return fmt.Errorf("database reset replica checkpoint is missing statefulset %s/%s", plan.namespace, plan.name)
	}
	originalReplicas := plan.originalReplicas
	if err := c.scaleStatefulSet(ctx, plan.namespace, plan.name, originalReplicas); err != nil {
		return fmt.Errorf("restore statefulset replicas after init SQL URL update failure: %w", err)
	}
	if originalReplicas == 0 {
		return nil
	}
	if err := c.waitDatabaseComponentReady(ctx, plan.namespace, plan.name, plan.component, originalReplicas); err != nil {
		return fmt.Errorf("wait statefulset ready after init SQL URL update failure: %w", err)
	}
	return nil
}

func normalizeDatabaseResetComponent(component *model.ApplicationComponent) *model.ApplicationComponent {
	if component == nil {
		return &model.ApplicationComponent{Namespace: config.DefaultNamespace}
	}
	cp := *component
	if strings.TrimSpace(cp.Namespace) == "" {
		cp.Namespace = config.DefaultNamespace
	}
	return &cp
}

func databaseResetNamespaceOrDefault(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return config.DefaultNamespace
}

func desiredDatabaseReplicas(component *model.ApplicationComponent) int32 {
	if component != nil && component.Replicas > 0 {
		return component.Replicas
	}
	return 1
}

func (c *DatabaseResetJobCtl) collectPVCResetTargets(ctx context.Context, namespace string, statefulSet *appsv1.StatefulSet, additionalObjects []client.Object, desiredReplicas int32) ([]pvcResetTarget, error) {
	targets := make(map[string]pvcResetTarget)
	addTarget := func(target pvcResetTarget) {
		target.namespace = databaseResetNamespaceOrDefault(target.namespace, namespace)
		target.name = strings.TrimSpace(target.name)
		if target.name == "" {
			return
		}
		key := target.namespace + "/" + target.name
		if existing, ok := targets[key]; ok {
			target.recreate = existing.recreate || target.recreate
		}
		targets[key] = target
	}

	replicas := desiredReplicas
	if statefulSet != nil && statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas > replicas {
		replicas = *statefulSet.Spec.Replicas
	}
	if replicas < 1 {
		replicas = 1
	}

	if statefulSet != nil {
		for _, tmpl := range statefulSet.Spec.VolumeClaimTemplates {
			templateName := strings.TrimSpace(tmpl.Name)
			if templateName == "" {
				continue
			}
			for ordinal := int32(0); ordinal < replicas; ordinal++ {
				addTarget(pvcResetTarget{
					namespace: namespace,
					name:      fmt.Sprintf("%s-%s-%d", templateName, statefulSet.Name, ordinal),
				})
			}
			list, err := c.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, fmt.Errorf("list pvcs in namespace %s: %w", namespace, err)
			}
			for _, pvc := range list.Items {
				if isStatefulSetTemplatePVCName(templateName, statefulSet.Name, pvc.Name) {
					addTarget(pvcResetTarget{namespace: namespace, name: pvc.Name})
				}
			}
		}
	}

	for _, obj := range additionalObjects {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok || pvc == nil {
			continue
		}
		addTarget(pvcResetTarget{
			namespace: databaseResetNamespaceOrDefault(pvc.Namespace, namespace),
			name:      pvc.Name,
			recreate:  true,
		})
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("database reset found no pvc target for statefulset %s/%s", namespace, statefulSet.Name)
	}
	result := make([]pvcResetTarget, 0, len(targets))
	for _, target := range targets {
		result = append(result, target)
	}
	sortPVCResetTargets(result)
	return result, nil
}

func sortPVCResetTargets(targets []pvcResetTarget) {
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].namespace != targets[j].namespace {
			return targets[i].namespace < targets[j].namespace
		}
		return targets[i].name < targets[j].name
	})
}

func isStatefulSetTemplatePVCName(templateName, statefulSetName, pvcName string) bool {
	templateName = strings.TrimSpace(templateName)
	statefulSetName = strings.TrimSpace(statefulSetName)
	pvcName = strings.TrimSpace(pvcName)
	if templateName == "" || statefulSetName == "" || pvcName == "" {
		return false
	}
	prefix := fmt.Sprintf("%s-%s-", templateName, statefulSetName)
	suffix, ok := strings.CutPrefix(pvcName, prefix)
	if !ok || suffix == "" {
		return false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func (c *DatabaseResetJobCtl) scaleStatefulSet(ctx context.Context, namespace, name string, replicas int32) error {
	current, err := c.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get statefulset %s/%s before scale: %w", namespace, name, err)
	}
	current = current.DeepCopy()
	current.Spec.Replicas = &replicas
	if _, err := c.client.AppsV1().StatefulSets(namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("scale statefulset %s/%s to %d: %w", namespace, name, replicas, err)
	}
	return nil
}

func (c *DatabaseResetJobCtl) waitStatefulSetPodsGone(ctx context.Context, namespace string, statefulSet *appsv1.StatefulSet, component *model.ApplicationComponent) error {
	selector := databaseResetPodSelector(statefulSet, component)
	timeout := c.databaseResetTimeout()
	err := wait.PollUntilContextTimeout(ctx, cleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		list, err := c.client.CoreV1().Pods(namespace).List(checkCtx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, err
		}
		return len(list.Items) == 0, nil
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, wait.ErrWaitTimeout) {
		return NewStatusError(config.StatusTimeout, fmt.Errorf("wait database pods gone for component %s timeout", component.Name))
	}
	return err
}

func databaseResetPodSelector(statefulSet *appsv1.StatefulSet, component *model.ApplicationComponent) string {
	if statefulSet != nil && statefulSet.Spec.Selector != nil {
		return metav1.FormatLabelSelector(statefulSet.Spec.Selector)
	}
	return labels.Set{
		config.LabelAppID:         component.AppID,
		config.LabelComponentName: naming.BoundedLabelValue(component.Name),
	}.String()
}

func (c *DatabaseResetJobCtl) deleteAndMaybeRecreatePVC(ctx context.Context, target pvcResetTarget) error {
	existing, err := c.client.CoreV1().PersistentVolumeClaims(target.namespace).Get(ctx, target.name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) && !target.recreate {
			return nil
		}
		return fmt.Errorf("get pvc %s/%s before reset: %w", target.namespace, target.name, err)
	}

	var replacement *corev1.PersistentVolumeClaim
	if target.recreate {
		replacement = existing.DeepCopy()
		cleanObjectMeta(&replacement.ObjectMeta)
		replacement.Finalizers = nil
		replacement.OwnerReferences = nil
		replacement.Spec.VolumeName = ""
	}

	if err := c.client.CoreV1().PersistentVolumeClaims(target.namespace).Delete(ctx, target.name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete pvc %s/%s: %w", target.namespace, target.name, err)
	}
	if err := c.waitPVCDeleted(ctx, target.namespace, target.name); err != nil {
		return err
	}
	if replacement != nil {
		if _, err := c.client.CoreV1().PersistentVolumeClaims(target.namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("recreate pvc %s/%s: %w", target.namespace, target.name, err)
		}
	}
	return nil
}

func (c *DatabaseResetJobCtl) waitPVCDeleted(ctx context.Context, namespace, name string) error {
	timeout := c.databaseResetTimeout()
	err := wait.PollUntilContextTimeout(ctx, cleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		_, err := c.client.CoreV1().PersistentVolumeClaims(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err == nil {
			return false, nil
		}
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, wait.ErrWaitTimeout) {
		return NewStatusError(config.StatusTimeout, fmt.Errorf("wait pvc %s/%s deleted timeout", namespace, name))
	}
	return err
}

func (c *DatabaseResetJobCtl) waitDatabaseComponentReady(ctx context.Context, namespace, name string, component *model.ApplicationComponent, desiredReplicas int32) error {
	timeout := c.databaseResetTimeout()
	if c.resourceWaiter != nil {
		err := c.resourceWaiter.WaitForComponentReady(ctx, component.AppID, naming.BoundedLabelValue(component.Name), desiredReplicas, timeout)
		if err != nil {
			var we *informer.WaitError
			if errors.As(err, &we) {
				return NewStatusError(we.Status, we.Err)
			}
			return err
		}
		return nil
	}
	err := wait.PollUntilContextTimeout(ctx, cleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		current, err := c.client.AppsV1().StatefulSets(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return current.Status.ReadyReplicas >= desiredReplicas, nil
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, wait.ErrWaitTimeout) {
		return NewStatusError(config.StatusTimeout, fmt.Errorf("wait database component %s ready timeout", component.Name))
	}
	return err
}

func buildWorkloadRestartPatch(restartedAt string) ([]byte, error) {
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						config.AnnotationWorkloadRestartAt: restartedAt,
					},
				},
			},
		},
	}
	return json.Marshal(patch)
}

func (c *DatabaseResetJobCtl) markComponentRuntime(ctx context.Context, component *model.ApplicationComponent, status config.ComponentStatus, readyReplicas int32, lastAbnormal string) error {
	if component == nil {
		return nil
	}
	if err := repository.UpdateComponentRuntimeFields(ctx, c.store, component, map[string]interface{}{
		"status":         string(status),
		"ready_replicas": readyReplicas,
		"last_abnormal":  lastAbnormal,
	}); err != nil {
		klog.ErrorS(err, "update component runtime status failed", "appID", component.AppID, "component", component.Name, "status", status)
		return err
	}
	component.Status = string(status)
	component.ReadyReplicas = readyReplicas
	component.LastAbnormal = lastAbnormal
	invalidateComponentsCache(c.runtime, component.AppID, "database reset status sync")
	return nil
}

func (c *DatabaseResetJobCtl) databaseResetTimeout() time.Duration {
	timeout := int64(0)
	if c != nil && c.job != nil {
		timeout = c.job.Timeout
	}
	if timeout <= 0 {
		timeout = config.DeployTimeout
	}
	return time.Duration(timeout) * time.Second
}
