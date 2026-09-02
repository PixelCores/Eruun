package job

import (
	"context"
	"encoding/json"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

// DeployAdoptedPodDisruptionBudgetJobCtl reconciles an adopted PDB by the
// exact source identity stored in the application's adoption snapshot.
//
// JobInfo must contain a fully formed *policyv1.PodDisruptionBudget whose
// namespace and name are the original source identity. Native applications are
// intentionally rejected because this controller does not define a generated
// resource lifecycle.
type DeployAdoptedPodDisruptionBudgetJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployAdoptedPodDisruptionBudgetJobCtl(
	job *model.JobTask,
	client kubernetes.Interface,
	store datastore.DataStore,
	ack func(),
	shareLocker locker.Locker,
) *DeployAdoptedPodDisruptionBudgetJobCtl {
	base, ok := newDeployNamespacedResourceJobBase(
		"DeployAdoptedPodDisruptionBudgetJobCtl",
		job,
		client,
		store,
		ack,
		shareLocker,
	)
	if !ok {
		return nil
	}
	return &DeployAdoptedPodDisruptionBudgetJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployAdoptedPodDisruptionBudgetJobCtl) Clean(context.Context) {}

func (c *DeployAdoptedPodDisruptionBudgetJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployAdoptedPodDisruptionBudgetJob run error")
}

func (c *DeployAdoptedPodDisruptionBudgetJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	desired, err := requiredJobInfo[*policyv1.PodDisruptionBudget](c.job)
	if err != nil {
		return err
	}
	if desired.Namespace == "" {
		desired.Namespace = c.job.Namespace
	}
	if desired.Namespace == "" || desired.Name == "" {
		return fmt.Errorf("adopted PodDisruptionBudget source namespace and name are required")
	}
	binding, adopted, err := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"PodDisruptionBudget",
		desired.Namespace,
		desired.Name,
	)
	if err != nil {
		return err
	}
	if !adopted {
		return fmt.Errorf(
			"PodDisruptionBudget %s/%s requires an adopted application source binding",
			desired.Namespace,
			desired.Name,
		)
	}
	return c.reconcileAdoptedPodDisruptionBudget(ctx, desired, binding)
}

func (c *DeployAdoptedPodDisruptionBudgetJobCtl) reconcileAdoptedPodDisruptionBudget(
	ctx context.Context,
	desired *policyv1.PodDisruptionBudget,
	binding *adoptedResourceBinding,
) error {
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}

	cli := c.client.PolicyV1().PodDisruptionBudgets(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedPodDisruptionBudget(ctx, desired, binding)
		}
		return fmt.Errorf(
			"get adopted PodDisruptionBudget %s/%s: %w",
			desired.Namespace,
			desired.Name,
			err,
		)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			c.store,
			binding,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted PodDisruptionBudget binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate := adoptedPodDisruptionBudgetForExistingUpdate(current, desired)
	if adoptedPodDisruptionBudgetEqual(current, candidate) {
		markResourceObserved(ctx, config.ResourcePodDisruptionBudget, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(
		ctx,
		func(ctx context.Context) (*policyv1.PodDisruptionBudget, error) {
			return cli.Get(ctx, desired.Name, metav1.GetOptions{})
		},
		func(ctx context.Context, latest *policyv1.PodDisruptionBudget) error {
			if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
				return err
			}
			candidate := adoptedPodDisruptionBudgetForExistingUpdate(latest, desired)
			if adoptedPodDisruptionBudgetEqual(latest, candidate) {
				return nil
			}
			_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
			return err
		},
	); err != nil {
		return fmt.Errorf(
			"update adopted PodDisruptionBudget %s/%s: %w",
			desired.Namespace,
			desired.Name,
			err,
		)
	}
	markResourceObserved(ctx, config.ResourcePodDisruptionBudget, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployAdoptedPodDisruptionBudgetJobCtl) recreateAdoptedPodDisruptionBudget(
	ctx context.Context,
	desired *policyv1.PodDisruptionBudget,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted PodDisruptionBudget recreation: %w", err)
	}
	var baseline policyv1.PodDisruptionBudget
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted PodDisruptionBudget recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted PodDisruptionBudget recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedPodDisruptionBudgetForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{
		APIVersion: policyv1.SchemeGroupVersion.String(),
		Kind:       "PodDisruptionBudget",
	}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted PodDisruptionBudget recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.PolicyV1().PodDisruptionBudgets(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted PodDisruptionBudget %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedDependencyLocked(
				recreationCtx,
				c.store,
				&recreation.adoptedResourceBinding,
				replacement,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover recreated adopted PodDisruptionBudget binding: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedPodDisruptionBudget(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted PodDisruptionBudget ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf(
			"recreate adopted PodDisruptionBudget %s/%s: %w",
			candidate.Namespace,
			candidate.Name,
			err,
		)
	}
	if err := persistCreatedAdoptedDependency(
		recreationCtx,
		recreation,
		created,
		created,
		c.runtime,
	); err != nil {
		return err
	}
	markResourceObserved(ctx, config.ResourcePodDisruptionBudget, created.Namespace, created.Name)
	return nil
}

func adoptedPodDisruptionBudgetForExistingUpdate(
	current, desired *policyv1.PodDisruptionBudget,
) *policyv1.PodDisruptionBudget {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	updated.Spec = *desired.Spec.DeepCopy()
	return updated
}

func adoptedPodDisruptionBudgetEqual(
	current, candidate *policyv1.PodDisruptionBudget,
) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec, candidate.Spec)
}

// DeployAdoptedNetworkPolicyJobCtl reconciles an adopted NetworkPolicy by the
// exact source identity stored in the application's adoption snapshot.
//
// JobInfo must contain a fully formed *networkingv1.NetworkPolicy whose
// namespace and name are the original source identity. Native applications are
// intentionally rejected because this controller does not define a generated
// resource lifecycle.
type DeployAdoptedNetworkPolicyJobCtl struct {
	deployNamespacedResourceJobBase
}

func NewDeployAdoptedNetworkPolicyJobCtl(
	job *model.JobTask,
	client kubernetes.Interface,
	store datastore.DataStore,
	ack func(),
	shareLocker locker.Locker,
) *DeployAdoptedNetworkPolicyJobCtl {
	base, ok := newDeployNamespacedResourceJobBase(
		"DeployAdoptedNetworkPolicyJobCtl",
		job,
		client,
		store,
		ack,
		shareLocker,
	)
	if !ok {
		return nil
	}
	return &DeployAdoptedNetworkPolicyJobCtl{
		deployNamespacedResourceJobBase: base,
	}
}

func (c *DeployAdoptedNetworkPolicyJobCtl) Clean(context.Context) {}

func (c *DeployAdoptedNetworkPolicyJobCtl) Run(ctx context.Context) error {
	return c.runWithStatus(ctx, c.run, "DeployAdoptedNetworkPolicyJob run error")
}

func (c *DeployAdoptedNetworkPolicyJobCtl) run(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("client is nil")
	}
	desired, err := requiredJobInfo[*networkingv1.NetworkPolicy](c.job)
	if err != nil {
		return err
	}
	if desired.Namespace == "" {
		desired.Namespace = c.job.Namespace
	}
	if desired.Namespace == "" || desired.Name == "" {
		return fmt.Errorf("adopted NetworkPolicy source namespace and name are required")
	}
	binding, adopted, err := adoptedResourceForJob(
		ctx,
		c.store,
		c.job,
		"NetworkPolicy",
		desired.Namespace,
		desired.Name,
	)
	if err != nil {
		return err
	}
	if !adopted {
		return fmt.Errorf(
			"NetworkPolicy %s/%s requires an adopted application source binding",
			desired.Namespace,
			desired.Name,
		)
	}
	return c.reconcileAdoptedNetworkPolicy(ctx, desired, binding)
}

func (c *DeployAdoptedNetworkPolicyJobCtl) reconcileAdoptedNetworkPolicy(
	ctx context.Context,
	desired *networkingv1.NetworkPolicy,
	binding *adoptedResourceBinding,
) error {
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return err
	}
	if !writable {
		return nil
	}

	cli := c.client.NetworkingV1().NetworkPolicies(desired.Namespace)
	current, err := cli.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return c.recreateAdoptedNetworkPolicy(ctx, desired, binding)
		}
		return fmt.Errorf(
			"get adopted NetworkPolicy %s/%s: %w",
			desired.Namespace,
			desired.Name,
			err,
		)
	}
	if err := validateAdoptedSnapshotUID(current.UID, binding); err != nil {
		recovered, recoverErr := recoverPendingAdoptedDependency(
			ctx,
			c.store,
			binding,
			current,
			current,
			c.runtime,
			c.shareLocker,
		)
		if recoverErr != nil {
			return fmt.Errorf("recover recreated adopted NetworkPolicy binding: %w", recoverErr)
		}
		if !recovered {
			return err
		}
	}
	candidate := adoptedNetworkPolicyForExistingUpdate(current, desired)
	if adoptedNetworkPolicyEqual(current, candidate) {
		markResourceObserved(ctx, config.ResourceNetworkPolicy, desired.Namespace, desired.Name)
		return nil
	}
	if err := updateResourceWithRetry(
		ctx,
		func(ctx context.Context) (*networkingv1.NetworkPolicy, error) {
			return cli.Get(ctx, desired.Name, metav1.GetOptions{})
		},
		func(ctx context.Context, latest *networkingv1.NetworkPolicy) error {
			if err := validateAdoptedSnapshotUID(latest.UID, binding); err != nil {
				return err
			}
			candidate := adoptedNetworkPolicyForExistingUpdate(latest, desired)
			if adoptedNetworkPolicyEqual(latest, candidate) {
				return nil
			}
			_, err := cli.Update(ctx, candidate, metav1.UpdateOptions{})
			return err
		},
	); err != nil {
		return fmt.Errorf(
			"update adopted NetworkPolicy %s/%s: %w",
			desired.Namespace,
			desired.Name,
			err,
		)
	}
	markResourceObserved(ctx, config.ResourceNetworkPolicy, desired.Namespace, desired.Name)
	return nil
}

func (c *DeployAdoptedNetworkPolicyJobCtl) recreateAdoptedNetworkPolicy(
	ctx context.Context,
	desired *networkingv1.NetworkPolicy,
	binding *adoptedResourceBinding,
) error {
	recreation, err := prepareAdoptedDependencyRecreation(c.store, binding)
	if err != nil {
		return fmt.Errorf("prepare adopted NetworkPolicy recreation: %w", err)
	}
	var baseline networkingv1.NetworkPolicy
	if err := json.Unmarshal(recreation.resource.Manifest, &baseline); err != nil {
		return fmt.Errorf("decode adopted NetworkPolicy recreation manifest: %w", err)
	}
	if baseline.Name != desired.Name || baseline.Namespace != desired.Namespace {
		return fmt.Errorf(
			"adopted NetworkPolicy recreation manifest identity %s/%s does not match %s/%s",
			baseline.Namespace,
			baseline.Name,
			desired.Namespace,
			desired.Name,
		)
	}
	candidate := adoptedNetworkPolicyForExistingUpdate(&baseline, desired)
	cleanObjectMeta(&candidate.ObjectMeta)
	candidate.TypeMeta = metav1.TypeMeta{
		APIVersion: networkingv1.SchemeGroupVersion.String(),
		Kind:       "NetworkPolicy",
	}
	guard, err := recreation.adoptedResourceBinding.prepareRecreationCandidate(ctx, c.store, candidate, c.runtime, c.shareLocker)
	if err != nil {
		return fmt.Errorf("persist adopted NetworkPolicy recreation claim: %w", err)
	}
	defer guard.release()
	recreationCtx := guard.Context()
	cli := c.client.NetworkingV1().NetworkPolicies(candidate.Namespace)
	created, err := cli.Create(recreationCtx, candidate, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			replacement, getErr := cli.Get(recreationCtx, candidate.Name, metav1.GetOptions{})
			if getErr != nil {
				return fmt.Errorf("get concurrent recreated adopted NetworkPolicy %s/%s: %w", candidate.Namespace, candidate.Name, getErr)
			}
			recovered, recoverErr := recoverPendingAdoptedDependencyLocked(
				recreationCtx,
				c.store,
				&recreation.adoptedResourceBinding,
				replacement,
				replacement,
				c.runtime,
			)
			if recoverErr != nil {
				return fmt.Errorf("recover recreated adopted NetworkPolicy binding: %w", recoverErr)
			}
			if recovered {
				guard.release()
				return c.reconcileAdoptedNetworkPolicy(ctx, desired, &recreation.adoptedResourceBinding)
			}
			return fmt.Errorf(
				"adopted NetworkPolicy ownership conflict for %s/%s: expected missing UID %q, found %q",
				candidate.Namespace,
				candidate.Name,
				recreation.resource.Source.UID,
				replacement.UID,
			)
		}
		return fmt.Errorf(
			"recreate adopted NetworkPolicy %s/%s: %w",
			candidate.Namespace,
			candidate.Name,
			err,
		)
	}
	if err := persistCreatedAdoptedDependency(
		recreationCtx,
		recreation,
		created,
		created,
		c.runtime,
	); err != nil {
		return err
	}
	markResourceObserved(ctx, config.ResourceNetworkPolicy, created.Namespace, created.Name)
	return nil
}

func adoptedNetworkPolicyForExistingUpdate(
	current, desired *networkingv1.NetworkPolicy,
) *networkingv1.NetworkPolicy {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	updated.Labels = adoptedOverlayStringMap(current.Labels, desired.Labels)
	updated.Annotations = adoptedOverlayStringMap(current.Annotations, desired.Annotations)
	updated.Spec = *desired.Spec.DeepCopy()
	return updated
}

func adoptedNetworkPolicyEqual(current, candidate *networkingv1.NetworkPolicy) bool {
	if current == nil || candidate == nil {
		return current == candidate
	}
	return apiequality.Semantic.DeepEqual(current.Labels, candidate.Labels) &&
		apiequality.Semantic.DeepEqual(current.Annotations, candidate.Annotations) &&
		apiequality.Semantic.DeepEqual(current.Spec, candidate.Spec)
}
