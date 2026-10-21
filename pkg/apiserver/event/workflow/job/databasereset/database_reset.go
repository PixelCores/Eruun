package databasereset

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
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobcleanup"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobdeploy"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinfo"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
	workflowtraits "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

type DatabaseResetJobCtl struct {
	jobexec.DeployNamespacedResourceJobBase
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

func NewDatabaseResetJobCtl(job *model.JobTask, runtime *jobexec.Runtime) *DatabaseResetJobCtl {
	base, ok := jobexec.NewDeployNamespacedResourceJobBase("DatabaseResetJobCtl", job, runtime, nil)
	if !ok {
		return nil
	}
	return &DatabaseResetJobCtl{DeployNamespacedResourceJobBase: base}
}

func (c *DatabaseResetJobCtl) Clean(context.Context) {}

func (c *DatabaseResetJobCtl) SaveInfo(ctx context.Context) error {
	if err := ensureDatabaseResetCheckpointIdentity(c.Job); err != nil {
		return fmt.Errorf("prepare database reset replica checkpoint identity: %w", err)
	}
	return jobinfo.SaveOrUpdateJobInfo(ctx, c.Store, c.Job)
}

func (c *DatabaseResetJobCtl) Run(ctx context.Context) error {
	return c.RunWithStatus(ctx, c.run, "database reset job run error")
}

func (c *DatabaseResetJobCtl) run(ctx context.Context) error {
	if c.Client == nil {
		return fmt.Errorf("client is nil")
	}
	if c.Store == nil {
		return fmt.Errorf("store is nil")
	}
	info, err := jobinfo.RequiredJobInfo[*jobinfo.DatabaseResetJobInfo](c.Job)
	if err != nil {
		return err
	}
	if jobinfo.DatabaseResetExecutionKey(info) == "" {
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
		if err := c.MarkComponentRuntime(ctx, component, config.ComponentStatusPending, 0, "", "database reset status sync"); err != nil {
			return err
		}
		if err := c.resetDatabaseComponent(ctx, plan, initSQLURL); err != nil {
			markErr := c.MarkComponentRuntime(ctx, component, config.ComponentStatusFailed, 0, err.Error(), "database reset status sync")
			return errors.Join(err, markErr)
		}
		if err := c.MarkComponentRuntime(ctx, component, config.ComponentStatusRunning, desiredDatabaseReplicas(component), "", "database reset status sync"); err != nil {
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
		checkpoint = &jobinfo.DatabaseResetReplicaCheckpoint{
			Version:          jobinfo.DatabaseResetReplicaCheckpointVersion,
			ExecutionKey:     jobinfo.DatabaseResetExecutionKeyFromJob(c.Job),
			Prepared:         true,
			OriginalReplicas: make(map[string]int32, len(plans)),
		}
		for i := range plans {
			key := databaseResetReplicaCheckpointKey(plans[i].namespace, plans[i].name)
			replicas := jobdeploy.DesiredReplicasFromStatefulSet(plans[i].statefulSet)
			checkpoint.OriginalReplicas[key] = replicas
			plans[i].originalReplicas = replicas
			plans[i].originalReplicasCheckpointed = true
		}
		raw, marshalErr := json.Marshal(checkpoint)
		if marshalErr != nil {
			return fmt.Errorf("marshal database reset replica checkpoint: %w", marshalErr)
		}
		c.Job.InternalInfo = string(raw)
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

func (c *DatabaseResetJobCtl) loadDatabaseResetReplicaCheckpoint(ctx context.Context) (*jobinfo.DatabaseResetReplicaCheckpoint, error) {
	candidates, err := jobinfo.LoadJobInfos(ctx, c.Store, c.Job.TaskID, c.Job.JobType, jobinfo.ResolveJobServiceName(c.Job))
	if err != nil {
		return nil, fmt.Errorf("load database reset replica checkpoint: %w", err)
	}
	existing, err := jobinfo.SelectDatabaseResetCheckpointJobInfo(c.Job, candidates)
	if err != nil {
		return nil, fmt.Errorf("select database reset replica checkpoint: %w", err)
	}
	if existing == nil {
		return nil, nil
	}
	raw := strings.TrimSpace(existing.InternalInfo)
	checkpoint, err := jobinfo.DecodeDatabaseResetReplicaCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	c.Job.InternalInfo = raw
	return checkpoint, nil
}

func ensureDatabaseResetCheckpointIdentity(job *model.JobTask) error {
	executionKey := jobinfo.DatabaseResetExecutionKeyFromJob(job)
	if executionKey == "" {
		return fmt.Errorf("database reset execution key is missing")
	}
	raw := strings.TrimSpace(job.InternalInfo)
	if raw == "" {
		marker := jobinfo.DatabaseResetReplicaCheckpoint{
			Version:      jobinfo.DatabaseResetReplicaCheckpointVersion,
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
	checkpoint, err := jobinfo.DecodeDatabaseResetReplicaCheckpoint(raw)
	if err != nil {
		return err
	}
	if checkpoint.ExecutionKey != executionKey {
		return fmt.Errorf("database reset replica checkpoint execution key %q does not match job execution key %q", checkpoint.ExecutionKey, executionKey)
	}
	return nil
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
		component := normalizeDatabaseResetComponent(rawComponent)
		result, err := jobdeploy.GenerateStoreService(component, workflowtraits.IdentityProfiles(component))
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, fmt.Errorf("generate store service for component %s failed", component.Name)
		}
		statefulSet, ok := result.Service.(*appsv1.StatefulSet)
		if !ok || statefulSet == nil {
			return nil, fmt.Errorf("store component %s did not generate a StatefulSet", component.Name)
		}
		namespace := jobexec.DatabaseResetNamespaceOrDefault(statefulSet.Namespace, component.Namespace)
		name := strings.TrimSpace(statefulSet.Name)
		if name == "" {
			name = naming.StoreServerName(component.Name, component.ResourceNameKey())
		}
		current, err := c.Client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
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
	err := jobexec.UpdateResourceWithRetry(ctx,
		func(getCtx context.Context) (*appsv1.StatefulSet, error) {
			return c.Client.AppsV1().StatefulSets(namespace).Get(getCtx, name, metav1.GetOptions{})
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
			_, err := c.Client.AppsV1().StatefulSets(namespace).Update(updateCtx, latest, metav1.UpdateOptions{})
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

func desiredDatabaseReplicas(component *model.ApplicationComponent) int32 {
	if component != nil && component.Replicas > 0 {
		return component.Replicas
	}
	return 1
}

func (c *DatabaseResetJobCtl) collectPVCResetTargets(ctx context.Context, namespace string, statefulSet *appsv1.StatefulSet, additionalObjects []client.Object, desiredReplicas int32) ([]pvcResetTarget, error) {
	targets := make(map[string]pvcResetTarget)
	addTarget := func(target pvcResetTarget) {
		target.namespace = jobexec.DatabaseResetNamespaceOrDefault(target.namespace, namespace)
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
			list, err := c.Client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, fmt.Errorf("list pvcs in namespace %s: %w", namespace, err)
			}
			for _, pvc := range list.Items {
				if jobexec.IsStatefulSetTemplatePVCName(templateName, statefulSet.Name, pvc.Name) {
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
			namespace: jobexec.DatabaseResetNamespaceOrDefault(pvc.Namespace, namespace),
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

func (c *DatabaseResetJobCtl) scaleStatefulSet(ctx context.Context, namespace, name string, replicas int32) error {
	current, err := c.Client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get statefulset %s/%s before scale: %w", namespace, name, err)
	}
	current = current.DeepCopy()
	current.Spec.Replicas = &replicas
	if _, err := c.Client.AppsV1().StatefulSets(namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("scale statefulset %s/%s to %d: %w", namespace, name, replicas, err)
	}
	return nil
}

func (c *DatabaseResetJobCtl) waitStatefulSetPodsGone(ctx context.Context, namespace string, statefulSet *appsv1.StatefulSet, component *model.ApplicationComponent) error {
	selector := databaseResetPodSelector(statefulSet, component)
	timeout := c.databaseResetTimeout()
	err := wait.PollUntilContextTimeout(ctx, jobcleanup.CleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		list, err := c.Client.CoreV1().Pods(namespace).List(checkCtx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, err
		}
		return len(list.Items) == 0, nil
	})
	if wait.Interrupted(err) && !errors.Is(err, context.Canceled) {
		return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait database pods gone for component %s timeout", component.Name))
	}
	return err
}

func databaseResetPodSelector(statefulSet *appsv1.StatefulSet, component *model.ApplicationComponent) string {
	if statefulSet != nil && statefulSet.Spec.Selector != nil {
		return metav1.FormatLabelSelector(statefulSet.Spec.Selector)
	}
	return labels.Set{
		spec.LabelAppID:         component.AppID,
		spec.LabelComponentName: naming.BoundedLabelValue(component.Name),
	}.String()
}

func (c *DatabaseResetJobCtl) deleteAndMaybeRecreatePVC(ctx context.Context, target pvcResetTarget) error {
	existing, err := c.Client.CoreV1().PersistentVolumeClaims(target.namespace).Get(ctx, target.name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) && !target.recreate {
			return nil
		}
		return fmt.Errorf("get pvc %s/%s before reset: %w", target.namespace, target.name, err)
	}

	var replacement *corev1.PersistentVolumeClaim
	if target.recreate {
		replacement = existing.DeepCopy()
		jobdeploy.CleanObjectMeta(&replacement.ObjectMeta)
		replacement.Finalizers = nil
		replacement.OwnerReferences = nil
		replacement.Spec.VolumeName = ""
	}

	if err := c.Client.CoreV1().PersistentVolumeClaims(target.namespace).Delete(ctx, target.name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("delete pvc %s/%s: %w", target.namespace, target.name, err)
	}
	if err := c.waitPVCDeleted(ctx, target.namespace, target.name); err != nil {
		return err
	}
	if replacement != nil {
		if _, err := c.Client.CoreV1().PersistentVolumeClaims(target.namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("recreate pvc %s/%s: %w", target.namespace, target.name, err)
		}
	}
	return nil
}

func (c *DatabaseResetJobCtl) waitPVCDeleted(ctx context.Context, namespace, name string) error {
	timeout := c.databaseResetTimeout()
	err := wait.PollUntilContextTimeout(ctx, jobcleanup.CleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		_, err := c.Client.CoreV1().PersistentVolumeClaims(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err == nil {
			return false, nil
		}
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
	if wait.Interrupted(err) && !errors.Is(err, context.Canceled) {
		return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait pvc %s/%s deleted timeout", namespace, name))
	}
	return err
}

func (c *DatabaseResetJobCtl) waitDatabaseComponentReady(ctx context.Context, namespace, name string, component *model.ApplicationComponent, desiredReplicas int32) error {
	timeout := c.databaseResetTimeout()
	if c.ResourceWaiter != nil {
		err := c.ResourceWaiter.WaitForComponentReady(ctx, component.AppID, naming.BoundedLabelValue(component.Name), desiredReplicas, timeout)
		if err != nil {
			var we *informer.WaitError
			if errors.As(err, &we) {
				return jobinfo.NewStatusError(we.Status, we.Err)
			}
			return err
		}
		return nil
	}
	err := wait.PollUntilContextTimeout(ctx, jobcleanup.CleanupPollInterval, timeout, true, func(checkCtx context.Context) (bool, error) {
		current, err := c.Client.AppsV1().StatefulSets(namespace).Get(checkCtx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return current.Status.ReadyReplicas >= desiredReplicas, nil
	})
	if wait.Interrupted(err) && !errors.Is(err, context.Canceled) {
		return jobinfo.NewStatusError(config.StatusTimeout, fmt.Errorf("wait database component %s ready timeout", component.Name))
	}
	return err
}

func (c *DatabaseResetJobCtl) databaseResetTimeout() time.Duration {
	timeout := int64(0)
	if c != nil && c.Job != nil {
		timeout = c.Job.Timeout
	}
	if timeout <= 0 {
		timeout = config.DeployTimeout
	}
	return time.Duration(timeout) * time.Second
}
