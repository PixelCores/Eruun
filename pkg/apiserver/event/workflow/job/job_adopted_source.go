package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PixelCores/Eruun/pkg/apiserver/adoption"
	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/locker"
)

type adoptedResourceBinding struct {
	application *model.Applications
	snapshot    *adoption.Snapshot
	resource    *adoption.ResourceSnapshot
}

type adoptedWorkloadRecreation struct {
	adoptedResourceBinding
	component *model.ApplicationComponent
	store     datastore.DataStore
	tx        datastore.Transactional
}

type adoptedDependencyRecreation struct {
	adoptedResourceBinding
	store datastore.DataStore
	tx    datastore.Transactional
}

type adoptedSecretCiphertextUpdate struct {
	component     *model.ApplicationComponent
	encryptedData *model.JSONStruct
}

const (
	adoptedSecretCiphertextCASMaxAttempts = 3
	adoptedRecreationClaimCASMaxAttempts  = 3
)

var errAdoptedSecretCiphertextConflict = errors.New("adopted secret ciphertext changed concurrently")
var errAdoptedWorkloadComponentConflict = errors.New("adopted workload component changed concurrently")
var errAdoptedRecreationPersistenceUnconfirmed = errors.New("adopted recreation persistence could not be confirmed")

// adoptedSourceForJob returns the source binding for a workload job. A source
// binding is deliberately discovered from persisted component state instead of
// generated names: appId identifies Eruun state while the Kubernetes UID is
// the ownership fact used before every write.
func adoptedSourceForJob(ctx context.Context, store datastore.DataStore, job *model.JobTask, kind string) (*model.ApplicationComponent, bool, error) {
	if store == nil || job == nil || strings.TrimSpace(job.AppID) == "" || strings.TrimSpace(job.Name) == "" {
		return nil, false, nil
	}
	_, snapshot, adopted, err := adoptedApplicationForJob(ctx, store, job)
	if err != nil || !adopted {
		return nil, adopted, err
	}
	entities, err := store.List(ctx, &model.ApplicationComponent{AppID: job.AppID}, &datastore.ListOptions{})
	if err != nil {
		return nil, true, fmt.Errorf("load adopted component source binding: %w", err)
	}
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil || component.Name != job.Name {
			continue
		}
		if !component.HasSourceWorkload() {
			return nil, true, fmt.Errorf("adopted component %s has no complete source workload binding", component.Name)
		}
		if strings.TrimSpace(component.SourceWorkloadAPIVersion) != appsv1.SchemeGroupVersion.String() {
			return nil, true, fmt.Errorf(
				"adopted component %s source apiVersion %s cannot reconcile %s",
				component.Name,
				component.SourceWorkloadAPIVersion,
				kind,
			)
		}
		if !strings.EqualFold(strings.TrimSpace(component.SourceWorkloadKind), kind) {
			return nil, true, fmt.Errorf("adopted component %s source kind %s cannot reconcile %s", component.Name, component.SourceWorkloadKind, kind)
		}
		namespace := adoptedSourceNamespace(component, job.Namespace)
		resource, err := findAdoptedSnapshotResource(
			snapshot,
			component.Name,
			kind,
			namespace,
			component.SourceWorkloadName,
		)
		if err != nil {
			return nil, true, err
		}
		if strings.TrimSpace(resource.ComponentName) != component.Name ||
			!strings.EqualFold(strings.TrimSpace(resource.DependencyRole), "workload") ||
			strings.TrimSpace(resource.Source.UID) != strings.TrimSpace(*component.SourceWorkloadUID) {
			return nil, true, fmt.Errorf(
				"adopted component %s source binding does not match its workload snapshot identity",
				component.Name,
			)
		}
		return component, true, nil
	}
	return nil, true, fmt.Errorf("adopted application %s has no persisted component source binding for %s", job.AppID, job.Name)
}

func adoptedSourceNamespace(component *model.ApplicationComponent, fallback string) string {
	if component != nil && strings.TrimSpace(component.Namespace) != "" {
		return strings.TrimSpace(component.Namespace)
	}
	return strings.TrimSpace(fallback)
}

func validateAdoptedSourceUID(actual types.UID, component *model.ApplicationComponent, kind, namespace, name string) error {
	if component == nil || component.SourceWorkloadUID == nil {
		return fmt.Errorf("adopted %s %s/%s has no source UID binding", kind, namespace, name)
	}
	expected := strings.TrimSpace(*component.SourceWorkloadUID)
	if expected == "" || string(actual) != expected {
		return fmt.Errorf("adopted %s ownership conflict for %s/%s: expected UID %q, found %q", kind, namespace, name, expected, actual)
	}
	return nil
}

// adoptedResourceForJob resolves a dependency by its persisted source
// identity. Adopted jobs must never fall back to generated names because a
// same-name object may belong to another owner.
func adoptedResourceForJob(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	kind, namespace, name string,
) (*adoptedResourceBinding, bool, error) {
	app, snapshot, adopted, err := adoptedApplicationForJob(ctx, store, job)
	if err != nil || !adopted {
		return nil, adopted, err
	}
	resource, err := findAdoptedSnapshotResource(snapshot, job.Name, kind, namespace, name)
	if err != nil && strings.EqualFold(strings.TrimSpace(kind), "Ingress") {
		resource, err = findGeneratedAdoptedIngressResource(snapshot, job, namespace, name)
	}
	if err != nil {
		return nil, true, err
	}
	return &adoptedResourceBinding{application: app, snapshot: snapshot, resource: resource}, true, nil
}

// adoptedApplicationForJob resolves only the application management contract.
// Cluster-scoped jobs use this before looking at a generated resource name:
// adopted applications never take ownership of cluster-scoped RBAC.
func adoptedApplicationForJob(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
) (*model.Applications, *adoption.Snapshot, bool, error) {
	if store == nil || job == nil || strings.TrimSpace(job.AppID) == "" {
		return nil, nil, false, nil
	}
	app := &model.Applications{ID: strings.TrimSpace(job.AppID)}
	if err := store.Get(ctx, app); err != nil {
		if err == datastore.ErrRecordNotExist {
			return nil, nil, false, fmt.Errorf("load application management mode: application %s does not exist", job.AppID)
		}
		return nil, nil, false, fmt.Errorf("load application management mode: %w", err)
	}
	if app.EffectiveManagementMode() != config.ManagementModeAdopted {
		return app, nil, false, nil
	}
	snapshot, err := decodeAdoptionSnapshot(app)
	if err != nil {
		return app, nil, true, err
	}
	return app, snapshot, true, nil
}

func adoptedRecoveryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), config.DelTimeOut)
}

func decodeAdoptionSnapshot(app *model.Applications) (*adoption.Snapshot, error) {
	if app == nil || app.AdoptionSnapshot == nil {
		return nil, fmt.Errorf("adopted application %q has no adoption snapshot", applicationID(app))
	}
	payload, err := json.Marshal(app.AdoptionSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal adoption snapshot: %w", err)
	}
	var snapshot adoption.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode adoption snapshot: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("validate adoption snapshot: %w", err)
	}
	return &snapshot, nil
}

func applicationID(app *model.Applications) string {
	if app == nil {
		return ""
	}
	return app.ID
}

func adoptedRecreationClaimMatches(
	resource *adoption.ResourceSnapshot,
	objectMeta metav1.Object,
) (bool, error) {
	if resource == nil || objectMeta == nil {
		return false, fmt.Errorf("recreated adopted resource metadata is required")
	}
	if resource.PendingRecreation == nil {
		return false, nil
	}
	token := strings.TrimSpace(resource.PendingRecreation.Token)
	if token == "" {
		return false, fmt.Errorf("persisted adopted recreation claim is empty")
	}
	if strings.TrimSpace(objectMeta.GetAnnotations()[config.AnnotationAdoptedRecreationToken]) != token {
		return false, nil
	}
	if objectMeta.GetDeletionTimestamp() != nil {
		return false, fmt.Errorf(
			"recreated adopted %s %s/%s is still terminating",
			resource.Source.Kind,
			resource.Source.Namespace,
			resource.Source.Name,
		)
	}
	return true, nil
}

func adoptedRecreationAlreadyPersisted(
	resource *adoption.ResourceSnapshot,
	objectMeta metav1.Object,
) (bool, error) {
	if resource == nil || objectMeta == nil {
		return false, fmt.Errorf("recreated adopted resource metadata is required")
	}
	uid := strings.TrimSpace(string(objectMeta.GetUID()))
	if uid == "" {
		return false, fmt.Errorf("recreated adopted resource returned an empty UID")
	}
	if resource.PendingRecreation != nil {
		return false, nil
	}
	return strings.TrimSpace(resource.Source.UID) == uid, nil
}

func cloneAdoptionSnapshot(snapshot *adoption.Snapshot) adoption.Snapshot {
	cloned := *snapshot
	cloned.Resources = append([]adoption.ResourceSnapshot(nil), snapshot.Resources...)
	for index := range cloned.Resources {
		if cloned.Resources[index].PendingRecreation != nil {
			claim := *cloned.Resources[index].PendingRecreation
			cloned.Resources[index].PendingRecreation = &claim
		}
	}
	return cloned
}

func adoptedRecreationResourceIndex(
	snapshot *adoption.Snapshot,
	resource *adoption.ResourceSnapshot,
) int {
	if snapshot == nil || resource == nil {
		return -1
	}
	for index := range snapshot.Resources {
		candidate := &snapshot.Resources[index]
		if candidate.Source.APIVersion == resource.Source.APIVersion &&
			candidate.Source.Kind == resource.Source.Kind &&
			candidate.Source.Namespace == resource.Source.Namespace &&
			candidate.Source.Name == resource.Source.Name &&
			candidate.Source.UID == resource.Source.UID {
			return index
		}
	}
	return -1
}

func (b *adoptedResourceBinding) prepareRecreationCandidate(
	ctx context.Context,
	store datastore.DataStore,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
	lockProvider locker.Locker,
) (*adoptedRecreationGuard, error) {
	if b == nil || store == nil || objectMeta == nil {
		return nil, fmt.Errorf("adopted recreation candidate state is incomplete")
	}
	guard, err := acquireAdoptedRecreationGuard(ctx, lockProvider, b)
	if err != nil {
		return nil, err
	}
	releaseGuard := true
	defer func() {
		if releaseGuard {
			guard.release()
		}
	}()
	ctx = guard.Context()
	var token string
	err = jobRuntime.withAdoptionPersistenceContext(ctx, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var lastConflict error
		for attempt := 0; attempt < adoptedRecreationClaimCASMaxAttempts; attempt++ {
			if err := b.reloadForRecreation(ctx, store); err != nil {
				return err
			}
			if err := b.validateCanonicalWorkloadComponent(ctx, store); err != nil {
				return err
			}
			if b.resource.PendingRecreation != nil {
				token = strings.TrimSpace(b.resource.PendingRecreation.Token)
				if token == "" {
					return fmt.Errorf("persisted adopted recreation claim is empty")
				}
				return nil
			}

			snapshot := cloneAdoptionSnapshot(b.snapshot)
			resourceIndex := adoptedRecreationResourceIndex(&snapshot, b.resource)
			if resourceIndex < 0 {
				return fmt.Errorf("adopted recreation snapshot binding disappeared")
			}
			token = uuid.NewString()
			snapshot.Version = adoption.SnapshotVersion
			snapshot.Resources[resourceIndex].PendingRecreation = &adoption.RecreationClaim{Token: token}
			if err := snapshot.Validate(); err != nil {
				return fmt.Errorf("validate adopted recreation claim snapshot: %w", err)
			}
			snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
			if err != nil {
				return fmt.Errorf("encode adopted recreation claim snapshot: %w", err)
			}
			payload, err := json.Marshal(snapshotJSON)
			if err != nil {
				return fmt.Errorf("encode adopted recreation claim persistence payload: %w", err)
			}
			updated, err := store.CompareAndSwap(
				ctx,
				b.application,
				"update_time",
				b.application.UpdateTime,
				map[string]interface{}{"adoption_snapshot": string(payload)},
			)
			if err != nil {
				return fmt.Errorf("persist adopted recreation claim: %w", err)
			}
			if !updated {
				lastConflict = fmt.Errorf("application changed concurrently")
				continue
			}
			b.application.AdoptionSnapshot = snapshotJSON
			b.snapshot = &snapshot
			b.resource = &snapshot.Resources[resourceIndex]
			return nil
		}
		return fmt.Errorf(
			"persist adopted recreation claim after %d attempts: %w",
			adoptedRecreationClaimCASMaxAttempts,
			lastConflict,
		)
	})
	if err != nil {
		return nil, err
	}
	annotations := make(map[string]string, len(objectMeta.GetAnnotations())+1)
	for key, value := range objectMeta.GetAnnotations() {
		annotations[key] = value
	}
	annotations[config.AnnotationAdoptedRecreationToken] = token
	objectMeta.SetAnnotations(annotations)
	releaseGuard = false
	return guard, nil
}

func (b *adoptedResourceBinding) validateCanonicalWorkloadComponent(
	ctx context.Context,
	store datastore.DataStore,
) error {
	if b == nil || b.application == nil || b.resource == nil {
		return fmt.Errorf("adopted recreation workload binding is incomplete")
	}
	source := b.resource.Source
	switch strings.ToLower(strings.TrimSpace(source.Kind)) {
	case "deployment", "statefulset":
	default:
		return nil
	}
	componentName := strings.TrimSpace(b.resource.ComponentName)
	expectedUID := strings.TrimSpace(source.UID)
	if componentName == "" || expectedUID == "" {
		return fmt.Errorf("adopted %s recreation workload binding is incomplete", source.Kind)
	}
	component, err := loadAdoptedWorkloadComponent(ctx, store, &model.ApplicationComponent{
		AppID: b.application.ID,
		Name:  componentName,
	})
	if err != nil {
		return err
	}
	actualUID := ""
	if component.SourceWorkloadUID != nil {
		actualUID = strings.TrimSpace(*component.SourceWorkloadUID)
	}
	if actualUID != expectedUID ||
		!strings.EqualFold(strings.TrimSpace(component.SourceWorkloadKind), strings.TrimSpace(source.Kind)) ||
		strings.TrimSpace(component.SourceWorkloadName) != strings.TrimSpace(source.Name) ||
		strings.TrimSpace(component.SourceWorkloadAPIVersion) != strings.TrimSpace(source.APIVersion) {
		return fmt.Errorf("%w: canonical source workload binding changed", errAdoptedWorkloadComponentConflict)
	}
	return nil
}

func findAdoptedSnapshotResource(
	snapshot *adoption.Snapshot,
	componentName, kind, namespace, name string,
) (*adoption.ResourceSnapshot, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("adoption snapshot is required")
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(snapshot.Namespace)
	}
	var matches []*adoption.ResourceSnapshot
	for index := range snapshot.Resources {
		resource := &snapshot.Resources[index]
		sourceNamespace := strings.TrimSpace(resource.Source.Namespace)
		if sourceNamespace == "" {
			sourceNamespace = strings.TrimSpace(snapshot.Namespace)
		}
		if !strings.EqualFold(strings.TrimSpace(resource.Source.Kind), strings.TrimSpace(kind)) ||
			sourceNamespace != namespace ||
			strings.TrimSpace(resource.Source.Name) != strings.TrimSpace(name) {
			continue
		}
		matches = append(matches, resource)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		for _, resource := range matches {
			if strings.TrimSpace(resource.ComponentName) == strings.TrimSpace(componentName) {
				return resource, nil
			}
		}
		return nil, fmt.Errorf("adopted %s %s/%s has ambiguous snapshot bindings", kind, namespace, name)
	}
	return nil, fmt.Errorf("adopted %s %s/%s is not present in the adoption snapshot", kind, namespace, name)
}

func findGeneratedAdoptedIngressResource(
	snapshot *adoption.Snapshot,
	job *model.JobTask,
	namespace, generatedName string,
) (*adoption.ResourceSnapshot, error) {
	if snapshot == nil || job == nil {
		return nil, fmt.Errorf("adoption snapshot and job are required")
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(snapshot.Namespace)
	}
	componentName := componentNameFromJobInfo(job.JobInfo)
	var matches []*adoption.ResourceSnapshot
	for index := range snapshot.Resources {
		resource := &snapshot.Resources[index]
		sourceNamespace := strings.TrimSpace(resource.Source.Namespace)
		if sourceNamespace == "" {
			sourceNamespace = strings.TrimSpace(snapshot.Namespace)
		}
		if !strings.EqualFold(resource.Source.Kind, "Ingress") ||
			sourceNamespace != namespace ||
			BuildIngressName(resource.Source.Name, job.ResourceAppNameOrID()) != generatedName {
			continue
		}
		if componentName != "" && resource.ComponentName != "" && resource.ComponentName != componentName {
			continue
		}
		matches = append(matches, resource)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("adopted Ingress %s/%s has ambiguous generated-name snapshot bindings", namespace, generatedName)
	}
	return nil, fmt.Errorf("adopted Ingress %s/%s is not present in the adoption snapshot", namespace, generatedName)
}

func adoptedResourceAllowsWrite(binding *adoptedResourceBinding) (bool, error) {
	if binding == nil || binding.resource == nil {
		return false, fmt.Errorf("adopted resource binding is required")
	}
	resource := binding.resource
	if resource.Disposition == adoption.DispositionManaged &&
		resource.Ownership == adoption.OwnershipExclusive {
		return true, nil
	}
	switch resource.Disposition {
	case adoption.DispositionSharedPreserved,
		adoption.DispositionDataProtected,
		adoption.DispositionExcluded:
		return false, nil
	default:
		return false, fmt.Errorf(
			"adopted %s %s/%s is not writable (ownership=%s, disposition=%s)",
			resource.Source.Kind,
			resource.Source.Namespace,
			resource.Source.Name,
			resource.Ownership,
			resource.Disposition,
		)
	}
}

func validateAdoptedSnapshotUID(actual types.UID, binding *adoptedResourceBinding) error {
	if binding == nil || binding.resource == nil {
		return fmt.Errorf("adopted resource binding is required")
	}
	source := binding.resource.Source
	expected := strings.TrimSpace(source.UID)
	if expected == "" || string(actual) != expected {
		return fmt.Errorf(
			"adopted %s ownership conflict for %s/%s: expected UID %q, found %q",
			source.Kind,
			source.Namespace,
			source.Name,
			expected,
			actual,
		)
	}
	return nil
}

func (b *adoptedResourceBinding) reloadCanonicalForRecreation(ctx context.Context, store datastore.DataStore) error {
	if b == nil || b.application == nil || b.resource == nil || store == nil {
		return fmt.Errorf("adopted recreation binding reload state is incomplete")
	}
	appID := strings.TrimSpace(b.application.ID)
	if appID == "" {
		return fmt.Errorf("adopted recreation application ID is required")
	}
	expectedSource := b.resource.Source
	expectedComponentName := strings.TrimSpace(b.resource.ComponentName)

	app := &model.Applications{ID: appID}
	if err := store.Get(ctx, app); err != nil {
		return fmt.Errorf("reload adopted recreation application %s: %w", appID, err)
	}
	if app.EffectiveManagementMode() != config.ManagementModeAdopted {
		return fmt.Errorf("reload adopted recreation application %s: application is no longer adopted", appID)
	}
	snapshot, err := decodeAdoptionSnapshot(app)
	if err != nil {
		return fmt.Errorf("reload adopted recreation snapshot: %w", err)
	}
	resource, err := findAdoptedSnapshotResource(
		snapshot,
		expectedComponentName,
		expectedSource.Kind,
		expectedSource.Namespace,
		expectedSource.Name,
	)
	if err != nil {
		return fmt.Errorf("reload adopted recreation resource binding: %w", err)
	}
	b.application = app
	b.snapshot = snapshot
	b.resource = resource
	return nil
}

func (b *adoptedResourceBinding) reloadForRecreation(ctx context.Context, store datastore.DataStore) error {
	if b == nil || b.resource == nil {
		return fmt.Errorf("adopted recreation binding reload state is incomplete")
	}
	expectedSource := b.resource.Source
	if err := b.reloadCanonicalForRecreation(ctx, store); err != nil {
		return err
	}
	if strings.TrimSpace(b.resource.Source.UID) != strings.TrimSpace(expectedSource.UID) {
		return fmt.Errorf(
			"reload adopted recreation resource binding: %s %s/%s changed concurrently",
			expectedSource.Kind,
			expectedSource.Namespace,
			expectedSource.Name,
		)
	}
	return nil
}

// prepareAdoptedWorkloadRecreation validates the immutable recreation
// baseline and the transactional DB capability before Kubernetes is mutated.
func prepareAdoptedWorkloadRecreation(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	component *model.ApplicationComponent,
	kind, namespace, name string,
) (*adoptedWorkloadRecreation, error) {
	if component == nil || component.SourceWorkloadUID == nil {
		return nil, fmt.Errorf("adopted %s source binding is incomplete", kind)
	}
	binding, adopted, err := adoptedResourceForJob(ctx, store, job, kind, namespace, name)
	if err != nil {
		return nil, err
	}
	if !adopted || binding == nil {
		return nil, fmt.Errorf("adopted %s %s/%s has no application snapshot binding", kind, namespace, name)
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return nil, err
	}
	if !writable {
		return nil, fmt.Errorf("adopted %s %s/%s cannot be recreated from a non-exclusive snapshot", kind, namespace, name)
	}
	expectedUID := strings.TrimSpace(*component.SourceWorkloadUID)
	if expectedUID == "" || binding.resource.Source.UID != expectedUID {
		return nil, fmt.Errorf(
			"adopted %s %s/%s snapshot UID %q does not match component binding %q",
			kind,
			namespace,
			name,
			binding.resource.Source.UID,
			expectedUID,
		)
	}
	if len(binding.resource.Manifest) == 0 {
		return nil, fmt.Errorf("adopted %s %s/%s has no recreation manifest", kind, namespace, name)
	}
	tx, ok := store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf("adopted %s recreation requires transactional datastore support", kind)
	}
	if _, ok := store.(datastore.ConditionalCompareAndSwap); !ok {
		return nil, fmt.Errorf("adopted %s recreation requires conditional compare-and-swap support", kind)
	}
	return &adoptedWorkloadRecreation{
		adoptedResourceBinding: *binding,
		component:              component,
		store:                  store,
		tx:                     tx,
	}, nil
}

func prepareAdoptedDependencyRecreation(
	store datastore.DataStore,
	binding *adoptedResourceBinding,
) (*adoptedDependencyRecreation, error) {
	if store == nil || binding == nil || binding.resource == nil || binding.application == nil || binding.snapshot == nil {
		return nil, fmt.Errorf("adopted dependency recreation binding is incomplete")
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return nil, err
	}
	if !writable {
		return nil, fmt.Errorf(
			"adopted %s %s/%s cannot be recreated from a non-exclusive snapshot",
			binding.resource.Source.Kind,
			binding.resource.Source.Namespace,
			binding.resource.Source.Name,
		)
	}
	if len(binding.resource.Manifest) == 0 {
		return nil, fmt.Errorf(
			"adopted %s %s/%s has no recreation manifest",
			binding.resource.Source.Kind,
			binding.resource.Source.Namespace,
			binding.resource.Source.Name,
		)
	}
	tx, ok := store.(datastore.Transactional)
	if !ok {
		return nil, fmt.Errorf(
			"adopted %s recreation requires transactional datastore support",
			binding.resource.Source.Kind,
		)
	}
	return &adoptedDependencyRecreation{
		adoptedResourceBinding: *binding,
		store:                  store,
		tx:                     tx,
	}, nil
}

func recoverPendingAdoptedWorkload(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	component *model.ApplicationComponent,
	kind, namespace, name string,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
	lockProvider locker.Locker,
) (bool, error) {
	recoveryCtx, recoveryCancel := adoptedRecoveryContext(ctx)
	defer recoveryCancel()

	binding, adopted, err := adoptedResourceForJob(recoveryCtx, store, job, kind, namespace, name)
	if err != nil {
		return false, err
	}
	if !adopted || binding == nil {
		return false, nil
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return false, err
	}
	if !writable {
		return false, nil
	}
	guard, err := acquireAdoptedRecreationGuard(recoveryCtx, lockProvider, binding)
	if err != nil {
		return false, err
	}
	defer guard.release()
	return recoverPendingAdoptedWorkloadLocked(
		guard.Context(),
		store,
		job,
		component,
		kind,
		namespace,
		name,
		created,
		objectMeta,
		jobRuntime,
	)
}

// recoverPendingAdoptedWorkloadLocked reloads and finalizes a live replacement
// while the caller holds the adopted recreation guard for this resource.
func recoverPendingAdoptedWorkloadLocked(
	ctx context.Context,
	store datastore.DataStore,
	job *model.JobTask,
	component *model.ApplicationComponent,
	kind, namespace, name string,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
) (bool, error) {
	binding, adopted, err := adoptedResourceForJob(ctx, store, job, kind, namespace, name)
	if err != nil {
		return false, err
	}
	if !adopted || binding == nil {
		return false, nil
	}
	writable, err := adoptedResourceAllowsWrite(binding)
	if err != nil {
		return false, err
	}
	if !writable {
		return false, nil
	}
	canonicalComponent, err := loadAdoptedWorkloadComponent(ctx, store, component)
	if err != nil {
		return false, err
	}
	alreadyPersisted, err := adoptedRecreationAlreadyPersisted(binding.resource, objectMeta)
	if err != nil {
		return false, err
	}
	if alreadyPersisted {
		canonicalUID := ""
		if canonicalComponent.SourceWorkloadUID != nil {
			canonicalUID = strings.TrimSpace(*canonicalComponent.SourceWorkloadUID)
		}
		if canonicalUID != strings.TrimSpace(string(objectMeta.GetUID())) {
			return false, fmt.Errorf("recreated adopted workload snapshot and component bindings disagree")
		}
		uid := canonicalUID
		component.SourceWorkloadUID = &uid
		component.UpdateTime = canonicalComponent.UpdateTime
		return true, nil
	}

	recreation, err := prepareAdoptedWorkloadRecreation(
		ctx,
		store,
		job,
		canonicalComponent,
		kind,
		namespace,
		name,
	)
	if err != nil {
		return false, err
	}
	matched, err := adoptedRecreationClaimMatches(recreation.resource, objectMeta)
	if err != nil || !matched {
		return false, err
	}
	if err := recreation.persistCreated(ctx, created, objectMeta, jobRuntime); err != nil {
		return false, err
	}
	if recreation.component.SourceWorkloadUID != nil {
		uid := strings.TrimSpace(*recreation.component.SourceWorkloadUID)
		component.SourceWorkloadUID = &uid
		component.UpdateTime = recreation.component.UpdateTime
	}
	return true, nil
}

func recoverPendingAdoptedDependency(
	ctx context.Context,
	store datastore.DataStore,
	binding *adoptedResourceBinding,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
	lockProvider locker.Locker,
) (bool, error) {
	if binding == nil {
		return false, fmt.Errorf("adopted dependency recreation binding is incomplete")
	}
	recoveryCtx, recoveryCancel := adoptedRecoveryContext(ctx)
	defer recoveryCancel()
	guard, err := acquireAdoptedRecreationGuard(recoveryCtx, lockProvider, binding)
	if err != nil {
		return false, err
	}
	defer guard.release()
	return recoverPendingAdoptedDependencyLocked(
		guard.Context(),
		store,
		binding,
		created,
		objectMeta,
		jobRuntime,
	)
}

// recoverPendingAdoptedDependencyLocked reloads and finalizes a live
// replacement while the caller holds its adopted recreation guard.
func recoverPendingAdoptedDependencyLocked(
	ctx context.Context,
	store datastore.DataStore,
	binding *adoptedResourceBinding,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
) (bool, error) {
	if binding == nil {
		return false, fmt.Errorf("adopted dependency recreation binding is incomplete")
	}
	canonicalBinding := *binding
	if err := canonicalBinding.reloadCanonicalForRecreation(ctx, store); err != nil {
		return false, err
	}
	recreation, err := prepareAdoptedDependencyRecreation(store, &canonicalBinding)
	if err != nil {
		return false, err
	}
	writable, err := adoptedResourceAllowsWrite(&recreation.adoptedResourceBinding)
	if err != nil {
		return false, err
	}
	if !writable {
		return false, nil
	}
	alreadyPersisted, err := adoptedRecreationAlreadyPersisted(recreation.resource, objectMeta)
	if err != nil {
		return false, err
	}
	if alreadyPersisted {
		binding.application = recreation.application
		binding.snapshot = recreation.snapshot
		binding.resource = recreation.resource
		return true, nil
	}
	matched, err := adoptedRecreationClaimMatches(recreation.resource, objectMeta)
	if err != nil || !matched {
		return false, err
	}
	if err := recreation.persistCreated(ctx, created, objectMeta, jobRuntime); err != nil {
		return false, err
	}
	binding.application = recreation.application
	binding.snapshot = recreation.snapshot
	binding.resource = recreation.resource
	binding.resource.Source.UID = string(objectMeta.GetUID())
	binding.resource.Source.ResourceVersion = objectMeta.GetResourceVersion()
	binding.resource.PendingRecreation = nil
	return true, nil
}

func recoverPendingAdoptedSecret(
	ctx context.Context,
	store datastore.DataStore,
	binding *adoptedResourceBinding,
	created runtime.Object,
	objectMeta metav1.Object,
	ciphertextUpdates []adoptedSecretCiphertextUpdate,
	jobRuntime *jobRuntime,
	lockProvider locker.Locker,
) (bool, error) {
	if binding == nil {
		return false, fmt.Errorf("adopted secret recreation binding is incomplete")
	}
	recoveryCtx, recoveryCancel := adoptedRecoveryContext(ctx)
	defer recoveryCancel()
	guard, err := acquireAdoptedRecreationGuard(recoveryCtx, lockProvider, binding)
	if err != nil {
		return false, err
	}
	defer guard.release()
	return recoverPendingAdoptedSecretLocked(
		guard.Context(),
		store,
		binding,
		created,
		objectMeta,
		ciphertextUpdates,
		jobRuntime,
	)
}

// recoverPendingAdoptedSecretLocked reloads and finalizes a live Secret
// replacement while the caller holds its adopted recreation guard.
func recoverPendingAdoptedSecretLocked(
	ctx context.Context,
	store datastore.DataStore,
	binding *adoptedResourceBinding,
	created runtime.Object,
	objectMeta metav1.Object,
	ciphertextUpdates []adoptedSecretCiphertextUpdate,
	jobRuntime *jobRuntime,
) (bool, error) {
	if binding == nil {
		return false, fmt.Errorf("adopted secret recreation binding is incomplete")
	}
	canonicalBinding := *binding
	if err := canonicalBinding.reloadCanonicalForRecreation(ctx, store); err != nil {
		return false, err
	}
	recreation, err := prepareAdoptedDependencyRecreation(store, &canonicalBinding)
	if err != nil {
		return false, err
	}
	writable, err := adoptedResourceAllowsWrite(&recreation.adoptedResourceBinding)
	if err != nil {
		return false, err
	}
	if !writable {
		return false, nil
	}
	alreadyPersisted, err := adoptedRecreationAlreadyPersisted(recreation.resource, objectMeta)
	if err != nil {
		return false, err
	}
	if alreadyPersisted {
		binding.application = recreation.application
		binding.snapshot = recreation.snapshot
		binding.resource = recreation.resource
		return true, nil
	}
	matched, err := adoptedRecreationClaimMatches(recreation.resource, objectMeta)
	if err != nil || !matched {
		return false, err
	}
	if err := recreation.persistCreatedSecret(ctx, created, objectMeta, ciphertextUpdates, jobRuntime); err != nil {
		return false, err
	}
	binding.application = recreation.application
	binding.snapshot = recreation.snapshot
	binding.resource = recreation.resource
	binding.resource.Source.UID = string(objectMeta.GetUID())
	binding.resource.Source.ResourceVersion = objectMeta.GetResourceVersion()
	binding.resource.PendingRecreation = nil
	return true, nil
}

func (r *adoptedWorkloadRecreation) persistCreated(
	ctx context.Context,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
) error {
	if r == nil || r.resource == nil || r.application == nil || r.component == nil || r.store == nil || r.tx == nil {
		return fmt.Errorf("adopted workload recreation state is incomplete")
	}
	if objectMeta == nil || strings.TrimSpace(string(objectMeta.GetUID())) == "" {
		return fmt.Errorf("recreated adopted workload returned an empty UID")
	}
	persistCtx, persistCancel := adoptedRecoveryContext(ctx)
	defer persistCancel()
	ctx = persistCtx
	err := jobRuntime.withAdoptionPersistenceContext(ctx, func() error {
		if err := r.adoptedResourceBinding.reloadForRecreation(ctx, r.store); err != nil {
			return err
		}
		component, err := reloadAdoptedWorkloadComponent(ctx, r.store, r.component)
		if err != nil {
			return err
		}
		newUID, snapshotJSON, app, err := prepareRecreatedSnapshotState(
			r.resource,
			r.snapshot,
			r.application,
			created,
			objectMeta,
		)
		if err != nil {
			return err
		}
		if err := r.tx.WithTransaction(ctx, func(tx datastore.DataStore) error {
			if err := compareAndSwapRecreatedWorkloadSourceUID(ctx, tx, component, newUID); err != nil {
				return fmt.Errorf("persist recreated workload source UID: %w", err)
			}
			return compareAndSwapRecreatedAdoptionSnapshot(ctx, tx, app, snapshotJSON)
		}); err != nil {
			return err
		}
		r.component.SourceWorkloadUID = &newUID
		r.component.UpdateTime = component.UpdateTime
		r.application.AdoptionSnapshot = snapshotJSON
		r.application.UpdateTime = app.UpdateTime
		return nil
	})
	if err == nil {
		return nil
	}
	confirmCtx, confirmCancel := adoptedRecoveryContext(ctx)
	defer confirmCancel()
	persisted, confirmErr := r.confirmCreatedPersistence(confirmCtx, objectMeta)
	if confirmErr != nil {
		return errors.Join(
			err,
			errAdoptedRecreationPersistenceUnconfirmed,
			fmt.Errorf("confirm recreated adopted workload persistence: %w", confirmErr),
		)
	}
	if persisted {
		return nil
	}
	return err
}

func (r *adoptedWorkloadRecreation) confirmCreatedPersistence(
	ctx context.Context,
	objectMeta metav1.Object,
) (bool, error) {
	if err := r.adoptedResourceBinding.reloadCanonicalForRecreation(ctx, r.store); err != nil {
		return false, err
	}
	persisted, err := adoptedRecreationAlreadyPersisted(r.resource, objectMeta)
	if err != nil || !persisted {
		return persisted, err
	}
	component, err := loadAdoptedWorkloadComponent(ctx, r.store, r.component)
	if err != nil {
		return false, err
	}
	uid := strings.TrimSpace(string(objectMeta.GetUID()))
	if component.SourceWorkloadUID == nil || strings.TrimSpace(*component.SourceWorkloadUID) != uid {
		return false, fmt.Errorf("recreated adopted workload snapshot and component bindings disagree")
	}
	r.component.SourceWorkloadUID = &uid
	r.component.UpdateTime = component.UpdateTime
	return true, nil
}

func reloadAdoptedWorkloadComponent(
	ctx context.Context,
	store datastore.DataStore,
	expected *model.ApplicationComponent,
) (*model.ApplicationComponent, error) {
	component, err := loadAdoptedWorkloadComponent(ctx, store, expected)
	if err != nil {
		return nil, err
	}
	expectedUID := ""
	if expected.SourceWorkloadUID != nil {
		expectedUID = strings.TrimSpace(*expected.SourceWorkloadUID)
	}
	currentUID := ""
	if component.SourceWorkloadUID != nil {
		currentUID = strings.TrimSpace(*component.SourceWorkloadUID)
	}
	if currentUID == "" || currentUID != expectedUID {
		return nil, fmt.Errorf("%w: source workload UID changed", errAdoptedWorkloadComponentConflict)
	}
	return component, nil
}

func loadAdoptedWorkloadComponent(
	ctx context.Context,
	store datastore.DataStore,
	expected *model.ApplicationComponent,
) (*model.ApplicationComponent, error) {
	if store == nil || expected == nil {
		return nil, fmt.Errorf("reload adopted workload component state is incomplete")
	}
	components, err := repository.FindComponentsByAppID(ctx, store, expected.AppID)
	if err != nil {
		return nil, fmt.Errorf("reload adopted workload component: %w", err)
	}
	for _, component := range components {
		if component == nil ||
			(expected.ID > 0 && component.ID != expected.ID) ||
			(expected.ID <= 0 && component.Name != expected.Name) {
			continue
		}
		currentUID := ""
		if component.SourceWorkloadUID != nil {
			currentUID = strings.TrimSpace(*component.SourceWorkloadUID)
		}
		current := *component
		if currentUID != "" {
			current.SourceWorkloadUID = &currentUID
		} else {
			current.SourceWorkloadUID = nil
		}
		return &current, nil
	}
	return nil, fmt.Errorf("reload adopted workload component: %w", datastore.ErrRecordNotExist)
}

func compareAndSwapRecreatedWorkloadSourceUID(
	ctx context.Context,
	store datastore.DataStore,
	component *model.ApplicationComponent,
	newUID string,
) error {
	if store == nil || component == nil || strings.TrimSpace(newUID) == "" {
		return fmt.Errorf("recreated workload component persistence state is incomplete")
	}
	conditionalStore, ok := store.(datastore.ConditionalCompareAndSwap)
	if !ok {
		return fmt.Errorf("datastore does not support conditional compare and swap")
	}
	conditions := map[string]interface{}{
		"app_id":      component.AppID,
		"update_time": component.UpdateTime,
	}
	if component.ID > 0 {
		conditions["id"] = component.ID
	} else {
		conditions["name"] = component.Name
	}
	if component.SourceWorkloadUID != nil {
		conditions["source_workload_uid"] = strings.TrimSpace(*component.SourceWorkloadUID)
	}
	updated, err := conditionalStore.CompareAndSwapWithConditions(
		ctx,
		component,
		conditions,
		map[string]interface{}{"source_workload_uid": newUID},
	)
	if err != nil {
		return err
	}
	if !updated {
		return errAdoptedWorkloadComponentConflict
	}
	component.SourceWorkloadUID = &newUID
	return nil
}

func (r *adoptedDependencyRecreation) persistCreated(
	ctx context.Context,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
) error {
	if r == nil || r.resource == nil || r.application == nil || r.snapshot == nil || r.store == nil || r.tx == nil {
		return fmt.Errorf("adopted dependency recreation state is incomplete")
	}
	persistCtx, persistCancel := adoptedRecoveryContext(ctx)
	defer persistCancel()
	ctx = persistCtx
	err := jobRuntime.withAdoptionPersistenceContext(ctx, func() error {
		if err := r.adoptedResourceBinding.reloadForRecreation(ctx, r.store); err != nil {
			return err
		}
		_, snapshotJSON, app, err := prepareRecreatedSnapshotState(
			r.resource,
			r.snapshot,
			r.application,
			created,
			objectMeta,
		)
		if err != nil {
			return err
		}
		if err := r.tx.WithTransaction(ctx, func(tx datastore.DataStore) error {
			return compareAndSwapRecreatedAdoptionSnapshot(ctx, tx, app, snapshotJSON)
		}); err != nil {
			return err
		}
		r.application.AdoptionSnapshot = snapshotJSON
		r.application.UpdateTime = app.UpdateTime
		return nil
	})
	if err == nil {
		return nil
	}
	confirmCtx, confirmCancel := adoptedRecoveryContext(ctx)
	defer confirmCancel()
	persisted, confirmErr := r.confirmCreatedPersistence(confirmCtx, objectMeta)
	if confirmErr != nil {
		return errors.Join(
			err,
			errAdoptedRecreationPersistenceUnconfirmed,
			fmt.Errorf("confirm recreated adopted dependency persistence: %w", confirmErr),
		)
	}
	if persisted {
		return nil
	}
	return err
}

func (r *adoptedDependencyRecreation) confirmCreatedPersistence(
	ctx context.Context,
	objectMeta metav1.Object,
) (bool, error) {
	if err := r.adoptedResourceBinding.reloadCanonicalForRecreation(ctx, r.store); err != nil {
		return false, err
	}
	return adoptedRecreationAlreadyPersisted(r.resource, objectMeta)
}

func (r *adoptedDependencyRecreation) persistCreatedSecret(
	ctx context.Context,
	created runtime.Object,
	objectMeta metav1.Object,
	ciphertextUpdates []adoptedSecretCiphertextUpdate,
	jobRuntime *jobRuntime,
) error {
	if r == nil || r.resource == nil || r.application == nil || r.snapshot == nil || r.store == nil || r.tx == nil {
		return fmt.Errorf("adopted secret recreation state is incomplete")
	}
	if len(ciphertextUpdates) == 0 {
		return fmt.Errorf("adopted secret component encryption state is incomplete")
	}
	persistCtx, persistCancel := adoptedRecoveryContext(ctx)
	defer persistCancel()
	ctx = persistCtx
	err := jobRuntime.withAdoptionPersistenceContext(ctx, func() error {
		if err := r.adoptedResourceBinding.reloadForRecreation(ctx, r.store); err != nil {
			return err
		}
		rebasedUpdates, err := rebaseAdoptedSecretCiphertextUpdates(
			ctx,
			r.store,
			r.application.ID,
			r.resource.Source.Name,
			ciphertextUpdates,
		)
		if err != nil {
			return err
		}
		_, snapshotJSON, app, err := prepareRecreatedSnapshotState(
			r.resource,
			r.snapshot,
			r.application,
			created,
			objectMeta,
		)
		if err != nil {
			return err
		}
		if err := r.tx.WithTransaction(ctx, func(tx datastore.DataStore) error {
			for _, update := range rebasedUpdates {
				if err := compareAndSwapAdoptedSecretData(ctx, tx, update.component, update.encryptedData); err != nil {
					return fmt.Errorf("persist recreated adopted secret ciphertext: %w", err)
				}
			}
			return compareAndSwapRecreatedAdoptionSnapshot(ctx, tx, app, snapshotJSON)
		}); err != nil {
			return err
		}
		applyAdoptedSecretCiphertextUpdates(rebasedUpdates)
		r.application.AdoptionSnapshot = snapshotJSON
		r.application.UpdateTime = app.UpdateTime
		return nil
	})
	if err == nil {
		return nil
	}
	confirmCtx, confirmCancel := adoptedRecoveryContext(ctx)
	defer confirmCancel()
	persisted, confirmErr := r.confirmCreatedPersistence(confirmCtx, objectMeta)
	if confirmErr != nil {
		return errors.Join(
			err,
			errAdoptedRecreationPersistenceUnconfirmed,
			fmt.Errorf("confirm recreated adopted secret persistence: %w", confirmErr),
		)
	}
	if persisted {
		return nil
	}
	return err
}

func rebaseAdoptedSecretCiphertextUpdates(
	ctx context.Context,
	store datastore.DataStore,
	appID, sourceName string,
	updates []adoptedSecretCiphertextUpdate,
) ([]adoptedSecretCiphertextUpdate, error) {
	appID = strings.TrimSpace(appID)
	sourceName = strings.TrimSpace(sourceName)
	if store == nil || appID == "" || sourceName == "" || len(updates) == 0 {
		return nil, fmt.Errorf("rebase adopted secret ciphertext state is incomplete")
	}
	entities, err := store.List(
		ctx,
		&model.ApplicationComponent{AppID: appID},
		&datastore.ListOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("reload adopted secret ciphertext components: %w", err)
	}
	componentsByName := make(map[string]*model.ApplicationComponent, len(entities))
	for _, entity := range entities {
		component, ok := entity.(*model.ApplicationComponent)
		if !ok || component == nil || component.AppID != appID {
			continue
		}
		if _, duplicate := componentsByName[component.Name]; duplicate {
			return nil, fmt.Errorf("reload adopted secret ciphertext component %s: duplicate component", component.Name)
		}
		componentsByName[component.Name] = component
	}

	rebased := make([]adoptedSecretCiphertextUpdate, 0, len(updates))
	seen := make(map[string]struct{}, len(updates))
	for _, update := range updates {
		if update.component == nil || update.encryptedData == nil {
			return nil, fmt.Errorf("rebase adopted secret ciphertext update is incomplete")
		}
		componentName := strings.TrimSpace(update.component.Name)
		if _, duplicate := seen[componentName]; duplicate {
			return nil, fmt.Errorf("rebase adopted secret ciphertext component %s: duplicate update", componentName)
		}
		seen[componentName] = struct{}{}
		current := componentsByName[componentName]
		if current == nil || current.AdoptedSecretData == nil {
			return nil, fmt.Errorf("reload adopted secret ciphertext component %s: encrypted data is missing", componentName)
		}
		currentPayload, err := decodeAdoptedSecretEnvelopes(current.AdoptedSecretData)
		if err != nil {
			return nil, fmt.Errorf("reload adopted secret ciphertext component %s: %w", componentName, err)
		}
		desiredPayload, err := decodeAdoptedSecretEnvelopes(update.encryptedData)
		if err != nil {
			return nil, fmt.Errorf("decode desired adopted secret ciphertext component %s: %w", componentName, err)
		}
		desiredSource, exists := desiredPayload[sourceName]
		if !exists {
			return nil, fmt.Errorf("desired adopted secret ciphertext component %s has no source %s", componentName, sourceName)
		}
		currentPayload[sourceName] = desiredSource
		encryptedData, err := model.NewJSONStructByStruct(currentPayload)
		if err != nil {
			return nil, fmt.Errorf("encode rebased adopted secret ciphertext component %s: %w", componentName, err)
		}
		rebased = append(rebased, adoptedSecretCiphertextUpdate{
			component:     current,
			encryptedData: encryptedData,
		})
	}
	return rebased, nil
}

func persistRotatedAdoptedSecretData(
	ctx context.Context,
	store datastore.DataStore,
	appID, sourceName string,
	ciphertextUpdates []adoptedSecretCiphertextUpdate,
	jobRuntime *jobRuntime,
) error {
	appID = strings.TrimSpace(appID)
	sourceName = strings.TrimSpace(sourceName)
	if store == nil || appID == "" || sourceName == "" || len(ciphertextUpdates) == 0 {
		return fmt.Errorf("adopted secret rotation persistence state is incomplete")
	}
	tx, ok := store.(datastore.Transactional)
	if !ok {
		return fmt.Errorf("adopted secret key rotation requires transactional datastore support")
	}

	return jobRuntime.withAdoptionPersistenceContext(ctx, func() error {
		var lastConflict error
		for attempt := 0; attempt < adoptedSecretCiphertextCASMaxAttempts; attempt++ {
			rebasedUpdates, err := rebaseAdoptedSecretCiphertextUpdates(
				ctx,
				store,
				appID,
				sourceName,
				ciphertextUpdates,
			)
			if err != nil {
				return err
			}
			err = tx.WithTransaction(ctx, func(txStore datastore.DataStore) error {
				for _, update := range rebasedUpdates {
					if err := compareAndSwapAdoptedSecretData(ctx, txStore, update.component, update.encryptedData); err != nil {
						return err
					}
				}
				return nil
			})
			if err == nil {
				applyAdoptedSecretCiphertextUpdates(rebasedUpdates)
				syncAdoptedSecretCiphertextUpdates(ciphertextUpdates, rebasedUpdates)
				return nil
			}
			if !errors.Is(err, errAdoptedSecretCiphertextConflict) {
				return err
			}
			lastConflict = err
		}
		return fmt.Errorf(
			"persist rotated adopted secret data after %d attempts: %w",
			adoptedSecretCiphertextCASMaxAttempts,
			lastConflict,
		)
	})
}

func applyAdoptedSecretCiphertextUpdates(updates []adoptedSecretCiphertextUpdate) {
	for _, update := range updates {
		if update.component != nil && update.encryptedData != nil {
			update.component.AdoptedSecretData = update.encryptedData
		}
	}
}

func syncAdoptedSecretCiphertextUpdates(
	targets []adoptedSecretCiphertextUpdate,
	persisted []adoptedSecretCiphertextUpdate,
) {
	persistedByComponent := make(map[string]adoptedSecretCiphertextUpdate, len(persisted))
	for _, update := range persisted {
		if update.component != nil {
			persistedByComponent[update.component.Name] = update
		}
	}
	for _, target := range targets {
		if target.component == nil {
			continue
		}
		update, found := persistedByComponent[target.component.Name]
		if !found || update.component == nil || update.encryptedData == nil {
			continue
		}
		target.component.AdoptedSecretData = update.encryptedData
		target.component.UpdateTime = update.component.UpdateTime
	}
}

func compareAndSwapAdoptedSecretData(
	ctx context.Context,
	store datastore.DataStore,
	component *model.ApplicationComponent,
	encryptedData *model.JSONStruct,
) error {
	if store == nil || component == nil || encryptedData == nil {
		return fmt.Errorf("adopted secret ciphertext persistence state is incomplete")
	}
	payload, err := json.Marshal(encryptedData)
	if err != nil {
		return fmt.Errorf("encode adopted secret ciphertext: %w", err)
	}
	updated, err := store.CompareAndSwap(
		ctx,
		component,
		"update_time",
		component.UpdateTime,
		map[string]interface{}{"adopted_secret_data": string(payload)},
	)
	if err != nil {
		return fmt.Errorf("compare and swap adopted secret ciphertext: %w", err)
	}
	if !updated {
		return fmt.Errorf("compare and swap adopted secret ciphertext: %w", errAdoptedSecretCiphertextConflict)
	}
	return nil
}

func persistCreatedAdoptedDependency(
	ctx context.Context,
	recreation *adoptedDependencyRecreation,
	created runtime.Object,
	objectMeta metav1.Object,
	jobRuntime *jobRuntime,
) error {
	if recreation == nil || recreation.resource == nil || objectMeta == nil {
		return fmt.Errorf("adopted dependency persistence state is incomplete")
	}
	if err := recreation.persistCreated(ctx, created, objectMeta, jobRuntime); err != nil {
		source := recreation.resource.Source
		return fmt.Errorf("persist recreated adopted %s binding; pending claim retained: %w", source.Kind, err)
	}
	return nil
}

func compareAndSwapRecreatedAdoptionSnapshot(
	ctx context.Context,
	store datastore.DataStore,
	app *model.Applications,
	snapshotJSON *model.JSONStruct,
) error {
	if store == nil || app == nil || snapshotJSON == nil {
		return fmt.Errorf("recreated adoption snapshot persistence state is incomplete")
	}
	payload, err := json.Marshal(snapshotJSON)
	if err != nil {
		return fmt.Errorf("encode recreated adoption snapshot: %w", err)
	}
	updated, err := store.CompareAndSwap(
		ctx,
		app,
		"update_time",
		app.UpdateTime,
		map[string]interface{}{"adoption_snapshot": string(payload)},
	)
	if err != nil {
		return fmt.Errorf("persist recreated adoption snapshot: %w", err)
	}
	if !updated {
		return fmt.Errorf("persist recreated adoption snapshot: application changed concurrently")
	}
	return nil
}

func prepareRecreatedSnapshotState(
	resource *adoption.ResourceSnapshot,
	savedSnapshot *adoption.Snapshot,
	savedApp *model.Applications,
	created runtime.Object,
	objectMeta metav1.Object,
) (string, *model.JSONStruct, *model.Applications, error) {
	if resource == nil || savedSnapshot == nil || savedApp == nil || created == nil {
		return "", nil, nil, fmt.Errorf("recreated adopted resource state is incomplete")
	}
	if objectMeta == nil || strings.TrimSpace(string(objectMeta.GetUID())) == "" {
		return "", nil, nil, fmt.Errorf("recreated adopted resource returned an empty UID")
	}
	matched, err := adoptedRecreationClaimMatches(resource, objectMeta)
	if err != nil {
		return "", nil, nil, err
	}
	if !matched {
		return "", nil, nil, fmt.Errorf(
			"recreated adopted %s %s/%s does not match its persisted recreation claim",
			resource.Source.Kind,
			resource.Source.Namespace,
			resource.Source.Name,
		)
	}
	expectedNamespace := strings.TrimSpace(resource.Source.Namespace)
	if expectedNamespace == "" {
		expectedNamespace = strings.TrimSpace(savedSnapshot.Namespace)
	}
	if objectMeta.GetNamespace() != expectedNamespace || objectMeta.GetName() != resource.Source.Name {
		return "", nil, nil, fmt.Errorf(
			"recreated adopted %s identity %s/%s does not match persisted %s/%s",
			resource.Source.Kind,
			objectMeta.GetNamespace(),
			objectMeta.GetName(),
			expectedNamespace,
			resource.Source.Name,
		)
	}
	unstructuredObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(created)
	if err != nil {
		return "", nil, nil, fmt.Errorf("convert recreated adopted resource: %w", err)
	}
	digest, err := adoption.DigestObject(&unstructured.Unstructured{Object: unstructuredObject})
	if err != nil {
		return "", nil, nil, fmt.Errorf("digest recreated adopted resource: %w", err)
	}

	snapshot := cloneAdoptionSnapshot(savedSnapshot)
	index := adoptedRecreationResourceIndex(&snapshot, resource)
	if index < 0 {
		return "", nil, nil, fmt.Errorf("recreated adopted resource snapshot binding disappeared")
	}
	newUID := string(objectMeta.GetUID())
	if newUID == strings.TrimSpace(resource.Source.UID) {
		return "", nil, nil, fmt.Errorf("recreated adopted resource returned the existing source UID")
	}
	snapshot.Version = adoption.SnapshotVersion
	snapshot.Resources[index].Source.UID = newUID
	snapshot.Resources[index].Source.ResourceVersion = objectMeta.GetResourceVersion()
	snapshot.Resources[index].Source.SpecDigest = digest
	snapshot.Resources[index].PendingRecreation = nil
	if err := snapshot.Validate(); err != nil {
		return "", nil, nil, fmt.Errorf("validate recreated adoption snapshot: %w", err)
	}
	snapshotJSON, err := model.NewJSONStructByStruct(snapshot)
	if err != nil {
		return "", nil, nil, fmt.Errorf("encode recreated adoption snapshot: %w", err)
	}
	app := *savedApp
	app.AdoptionSnapshot = snapshotJSON
	return newUID, snapshotJSON, &app, nil
}

// adoptedDeploymentForExistingUpdate starts from the live object. Only the
// fields Eruun can express without re-creating the controller are overlaid;
// selectors and all unknown live fields remain intact.
func adoptedDeploymentForExistingUpdate(current, desired *appsv1.Deployment) *appsv1.Deployment {
	updated := current.DeepCopy()
	if desired == nil {
		return updated
	}
	if desired.Spec.Replicas != nil {
		replicas := *desired.Spec.Replicas
		updated.Spec.Replicas = &replicas
	}
	updated.Labels = adoptedManagedStringMap(current.Labels, desired.Labels, eruunSystemLabelKeys)
	updated.Annotations = adoptedManagedStringMap(current.Annotations, desired.Annotations, eruunWorkloadTemplateAnnotationKeys)
	mergeAdoptedPodTemplate(&updated.Spec.Template, &desired.Spec.Template)
	preserveAdoptedSelectorLabels(&updated.Spec.Template, current.Spec.Template.Labels, current.Spec.Selector)
	return updated
}

func adoptedDeploymentNeedsUpdate(current, desired *appsv1.Deployment) bool {
	if current == nil || desired == nil {
		return false
	}
	updated := adoptedDeploymentForExistingUpdate(current, desired)
	return !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) ||
		!apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) ||
		!apiequality.Semantic.DeepEqual(current.Spec, updated.Spec)
}

// adoptedStatefulSetForExistingUpdate preserves all StatefulSet identity and
// storage fields. The normal job builder synthesizes serviceName, selector,
// pod-management and VCT values from Eruun naming, so a difference in those
// generated values is not proof that the user requested an immutable-field
// migration. Until an explicit migration intent is carried by the job contract,
// the live source identity is authoritative and those fields are never written.
func adoptedStatefulSetForExistingUpdate(current, desired *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if current == nil {
		return nil, fmt.Errorf("statefulset live object is required")
	}
	if desired == nil {
		return current.DeepCopy(), nil
	}
	updated := current.DeepCopy()
	if desired.Spec.Replicas != nil {
		replicas := *desired.Spec.Replicas
		updated.Spec.Replicas = &replicas
	}
	updated.Labels = adoptedManagedStringMap(current.Labels, desired.Labels, eruunSystemLabelKeys)
	updated.Annotations = adoptedManagedStringMap(current.Annotations, desired.Annotations, eruunWorkloadTemplateAnnotationKeys)
	mergeAdoptedPodTemplate(&updated.Spec.Template, &desired.Spec.Template)
	preserveAdoptedSelectorLabels(&updated.Spec.Template, current.Spec.Template.Labels, current.Spec.Selector)
	return updated, nil
}

func adoptedStatefulSetNeedsUpdate(current, desired *appsv1.StatefulSet) (bool, error) {
	updated, err := adoptedStatefulSetForExistingUpdate(current, desired)
	if err != nil {
		return false, err
	}
	return !apiequality.Semantic.DeepEqual(current.Labels, updated.Labels) ||
		!apiequality.Semantic.DeepEqual(current.Annotations, updated.Annotations) ||
		!apiequality.Semantic.DeepEqual(current.Spec, updated.Spec), nil
}

func preserveAdoptedSelectorLabels(template *corev1.PodTemplateSpec, currentLabels map[string]string, selector *metav1.LabelSelector) {
	if template == nil {
		return
	}
	selectorKeys := make(map[string]struct{})
	if selector != nil {
		for key := range selector.MatchLabels {
			selectorKeys[key] = struct{}{}
		}
		for _, requirement := range selector.MatchExpressions {
			selectorKeys[requirement.Key] = struct{}{}
		}
	}
	for key := range selectorKeys {
		value, exists := currentLabels[key]
		if !exists {
			delete(template.Labels, key)
			continue
		}
		if template.Labels == nil {
			template.Labels = make(map[string]string, len(selectorKeys))
		}
		template.Labels[key] = value
	}
	// managed-by is a standard label commonly selected by Services and
	// NetworkPolicies even when it is not part of the workload selector. Keep
	// the live value so adopting a Helm- or operator-managed workload cannot
	// disconnect those dependencies during the next rollout.
	if value, exists := currentLabels[config.LabelManagedBy]; exists {
		if template.Labels == nil {
			template.Labels = make(map[string]string, 1)
		}
		template.Labels[config.LabelManagedBy] = value
	}
}

// mergeAdoptedPodTemplate only overlays values that have an unambiguous
// Eruun meaning. In particular it preserves all unspecified fields and avoids
// replacing a user-managed sidecar or security context.
func mergeAdoptedPodTemplate(live, desired *corev1.PodTemplateSpec) {
	if live == nil || desired == nil {
		return
	}
	live.Labels = adoptedManagedStringMap(live.Labels, desired.Labels, eruunSystemLabelKeys)
	live.Annotations = adoptedManagedStringMap(live.Annotations, desired.Annotations, eruunWorkloadTemplateAnnotationKeys)
	// The converter defines the first source container as the Eruun main
	// component. An explicit adoption mapping can rename the component, so its
	// generated container name is not a safe identity and may even collide with
	// a live sidecar name.
	if len(live.Spec.Containers) > 0 && len(desired.Spec.Containers) > 0 {
		mergeAdoptedContainer(&live.Spec.Containers[0], &desired.Spec.Containers[0], true)
	}
	for i := 1; i < len(desired.Spec.Containers); i++ {
		desiredContainer := &desired.Spec.Containers[i]
		for j := 1; j < len(live.Spec.Containers); j++ {
			if live.Spec.Containers[j].Name == desiredContainer.Name {
				mergeAdoptedContainer(&live.Spec.Containers[j], desiredContainer, false)
				break
			}
		}
	}
	for i := range desired.Spec.InitContainers {
		desiredContainer := &desired.Spec.InitContainers[i]
		for j := range live.Spec.InitContainers {
			if live.Spec.InitContainers[j].Name == desiredContainer.Name {
				mergeAdoptedContainer(&live.Spec.InitContainers[j], desiredContainer, false)
				break
			}
		}
	}
}

func mergeAdoptedContainer(live, desired *corev1.Container, mergeLiteralEnv bool) {
	if live == nil || desired == nil {
		return
	}
	if strings.TrimSpace(desired.Image) != "" {
		live.Image = desired.Image
	}
	if !mergeLiteralEnv {
		return
	}
	index := make(map[string]int, len(live.Env))
	for envIndex := range live.Env {
		if name := strings.TrimSpace(live.Env[envIndex].Name); name != "" {
			index[name] = envIndex
		}
	}
	for _, desiredEnv := range desired.Env {
		name := strings.TrimSpace(desiredEnv.Name)
		if name == "" {
			continue
		}
		if envIndex, found := index[name]; found {
			live.Env[envIndex] = *desiredEnv.DeepCopy()
			continue
		}
		live.Env = append(live.Env, *desiredEnv.DeepCopy())
		index[name] = len(live.Env) - 1
	}
}

func adoptedManagedStringMap(current, desired map[string]string, allowed []string) map[string]string {
	if len(current) == 0 && len(desired) == 0 {
		return nil
	}
	updated := make(map[string]string, len(current)+len(allowed))
	for key, value := range current {
		updated[key] = value
	}
	for _, key := range allowed {
		if value, ok := desired[key]; ok {
			updated[key] = value
		}
	}
	return updated
}

func adoptedOverlayStringMap(current, desired map[string]string) map[string]string {
	if len(current) == 0 && len(desired) == 0 {
		return nil
	}
	updated := make(map[string]string, len(current)+len(desired))
	for key, value := range current {
		updated[key] = value
	}
	for key, value := range desired {
		updated[key] = value
	}
	return updated
}
