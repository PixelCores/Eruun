// Package adoption defines the shared contract for taking control of
// explicitly selected existing Kubernetes resources. It is consumed by the
// namespace import, application lifecycle, and workflow runtime paths.
package adoption

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

const (
	legacySnapshotVersion = 1
	SnapshotVersion       = 2
)

const (
	DispositionManaged         = "managed"
	DispositionSharedPreserved = "shared-preserved"
	DispositionDataProtected   = "data-protected"
	DispositionExcluded        = "excluded"
	DispositionBlocked         = "blocked"

	OwnershipExclusive     = "exclusive"
	OwnershipShared        = "shared"
	OwnershipDataProtected = "data-protected"
	OwnershipExternal      = "external"
)

// Snapshot is the versioned, canonical adoption state stored on an
// application. It is deliberately not a second ownership entity: Kubernetes
// UID remains the ownership fact, while this document captures the approved
// dependency graph and recreation baseline.
type Snapshot struct {
	Version         int                `json:"version"`
	Namespace       string             `json:"namespace"`
	PlanFingerprint string             `json:"planFingerprint,omitempty"`
	Resources       []ResourceSnapshot `json:"resources"`
}

type ResourceIdentity struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	Namespace       string `json:"namespace,omitempty"`
	Name            string `json:"name"`
	UID             string `json:"uid,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
	SpecDigest      string `json:"specDigest"`
}

type ResourceSnapshot struct {
	Source            ResourceIdentity `json:"source"`
	ComponentName     string           `json:"componentName,omitempty"`
	DependencyRole    string           `json:"dependencyRole"`
	Ownership         string           `json:"ownership"`
	Disposition       string           `json:"disposition"`
	Manifest          json.RawMessage  `json:"manifest,omitempty"`
	SecretKeys        []string         `json:"secretKeys,omitempty"`
	PendingRecreation *RecreationClaim `json:"pendingRecreation,omitempty"`
}

// RecreationClaim is a write-ahead marker for one missing adopted resource.
// The source UID remains the ownership baseline; the opaque token only binds a
// replacement object to the persisted intent across retries and restarts.
type RecreationClaim struct {
	Token string `json:"token"`
}

func NewSnapshot(namespace string, resources []ResourceSnapshot) Snapshot {
	out := Snapshot{
		Version:   SnapshotVersion,
		Namespace: strings.TrimSpace(namespace),
		Resources: append([]ResourceSnapshot(nil), resources...),
	}
	out.Sort()
	return out
}

func (s *Snapshot) Sort() {
	if s == nil {
		return
	}
	sort.SliceStable(s.Resources, func(i, j int) bool {
		left := s.Resources[i].Source
		right := s.Resources[j].Source
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		return left.Name < right.Name
	})
	for index := range s.Resources {
		sort.Strings(s.Resources[index].SecretKeys)
	}
}

func (s Snapshot) Validate() error {
	if s.Version != legacySnapshotVersion && s.Version != SnapshotVersion {
		return fmt.Errorf("unsupported adoption snapshot version %d", s.Version)
	}
	if strings.TrimSpace(s.Namespace) == "" {
		return fmt.Errorf("adoption snapshot namespace is required")
	}
	seenUIDs := make(map[string]string)
	for _, resource := range s.Resources {
		source := resource.Source
		if strings.TrimSpace(source.APIVersion) == "" ||
			strings.TrimSpace(source.Kind) == "" ||
			strings.TrimSpace(source.Name) == "" ||
			strings.TrimSpace(source.SpecDigest) == "" {
			return fmt.Errorf("adoption snapshot resource identity is incomplete")
		}
		if uid := strings.TrimSpace(source.UID); uid != "" {
			key := strings.ToLower(source.Kind) + "/" + uid
			if existing, ok := seenUIDs[key]; ok {
				return fmt.Errorf("duplicate adoption source UID %s for %s and %s", uid, existing, source.Name)
			}
			seenUIDs[key] = source.Name
		}
		if resource.PendingRecreation == nil {
			continue
		}
		if s.Version != SnapshotVersion {
			return fmt.Errorf("adoption snapshot version %d cannot carry pending recreation state", s.Version)
		}
		if strings.TrimSpace(resource.PendingRecreation.Token) == "" ||
			strings.TrimSpace(source.UID) == "" {
			return fmt.Errorf("adoption snapshot pending recreation state is incomplete")
		}
		if resource.Ownership != OwnershipExclusive || resource.Disposition != DispositionManaged {
			return fmt.Errorf(
				"adoption snapshot pending recreation is not allowed for ownership=%s disposition=%s",
				resource.Ownership,
				resource.Disposition,
			)
		}
		if len(resource.Manifest) == 0 {
			return fmt.Errorf("adoption snapshot pending recreation has no manifest")
		}
	}
	return nil
}

// ResourceSnapshotFromObject strips server-owned runtime fields from the
// recreation baseline. Secret payloads and value-derived hashes are never
// retained; Secret value drift is detected by the source resourceVersion.
func ResourceSnapshotFromObject(
	object *unstructured.Unstructured,
	componentName, dependencyRole, ownership, disposition string,
) (ResourceSnapshot, error) {
	if object == nil {
		return ResourceSnapshot{}, fmt.Errorf("adoption source object is nil")
	}
	digest, err := DigestObject(object)
	if err != nil {
		return ResourceSnapshot{}, err
	}

	normalized := object.DeepCopy()
	unstructured.RemoveNestedField(normalized.Object, "status")
	stripRuntimeMetadata(normalized)

	var secretKeys []string
	if strings.EqualFold(normalized.GetKind(), "Secret") {
		secretKeys = secretKeyNames(normalized)
		normalized = secretSnapshotManifest(normalized)
	}
	manifest, err := json.Marshal(normalized.Object)
	if err != nil {
		return ResourceSnapshot{}, fmt.Errorf("marshal adoption snapshot %s/%s: %w", normalized.GetKind(), normalized.GetName(), err)
	}
	return ResourceSnapshot{
		Source: ResourceIdentity{
			APIVersion:      object.GetAPIVersion(),
			Kind:            object.GetKind(),
			Namespace:       object.GetNamespace(),
			Name:            object.GetName(),
			UID:             string(object.GetUID()),
			ResourceVersion: object.GetResourceVersion(),
			SpecDigest:      digest,
		},
		ComponentName:  strings.TrimSpace(componentName),
		DependencyRole: strings.TrimSpace(dependencyRole),
		Ownership:      strings.TrimSpace(ownership),
		Disposition:    strings.TrimSpace(disposition),
		Manifest:       manifest,
		SecretKeys:     secretKeys,
	}, nil
}

// secretSnapshotManifest deliberately uses a strict allowlist. In particular,
// annotations are not persisted because kubectl's last-applied annotation can
// contain a complete Secret manifest (including reversible base64 payloads).
// Secret values themselves are stored only in the encrypted component field.
func secretSnapshotManifest(secret *unstructured.Unstructured) *unstructured.Unstructured {
	manifest := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": secret.GetAPIVersion(),
		"kind":       secret.GetKind(),
		"metadata": map[string]interface{}{
			"name":      secret.GetName(),
			"namespace": secret.GetNamespace(),
		},
	}}
	if labels := secret.GetLabels(); len(labels) > 0 {
		manifest.SetLabels(labels)
	}
	if secretType, found, _ := unstructured.NestedString(secret.Object, "type"); found {
		_ = unstructured.SetNestedField(manifest.Object, secretType, "type")
	}
	if immutable, found, _ := unstructured.NestedBool(secret.Object, "immutable"); found {
		_ = unstructured.SetNestedField(manifest.Object, immutable, "immutable")
	}
	return manifest
}

// DigestObject includes every declarative field for ordinary resources. Secret
// digests intentionally cover only metadata, type, immutable, and key names so
// API and persisted snapshots cannot be used as offline value-guessing oracles.
// Secret value drift is carried by ResourceIdentity.ResourceVersion.
func DigestObject(object *unstructured.Unstructured) (string, error) {
	if object == nil {
		return "", fmt.Errorf("adoption source object is nil")
	}
	normalized := object.DeepCopy()
	unstructured.RemoveNestedField(normalized.Object, "status")
	stripRuntimeMetadata(normalized)
	var digestInput interface{} = normalized.Object
	if strings.EqualFold(normalized.GetKind(), "Secret") {
		keys := secretKeyNames(normalized)
		manifest := secretSnapshotManifest(normalized)
		digestInput = struct {
			Manifest   map[string]interface{} `json:"manifest"`
			SecretKeys []string               `json:"secretKeys"`
		}{
			Manifest:   manifest.Object,
			SecretKeys: keys,
		}
	}
	payload, err := json.Marshal(digestInput)
	if err != nil {
		return "", fmt.Errorf("marshal adoption source digest: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func stripRuntimeMetadata(object *unstructured.Unstructured) {
	if object == nil {
		return
	}
	for _, field := range []string{
		"managedFields",
		"resourceVersion",
		"uid",
		"generation",
		"creationTimestamp",
		"deletionTimestamp",
		"deletionGracePeriodSeconds",
		"selfLink",
	} {
		unstructured.RemoveNestedField(object.Object, "metadata", field)
	}
	annotations := object.GetAnnotations()
	if _, found := annotations[config.AnnotationAdoptedRecreationToken]; found {
		delete(annotations, config.AnnotationAdoptedRecreationToken)
		if len(annotations) == 0 {
			object.SetAnnotations(nil)
		} else {
			object.SetAnnotations(annotations)
		}
	}
}

func secretKeyNames(object *unstructured.Unstructured) []string {
	seen := map[string]struct{}{}
	for _, field := range []string{"data", "stringData"} {
		values, found, _ := unstructured.NestedStringMap(object.Object, field)
		if !found {
			continue
		}
		for key := range values {
			seen[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// SecretData returns a defensive copy of raw Kubernetes Secret values. Callers
// must immediately encrypt it and must not persist or log the returned map.
func SecretData(secret *corev1.Secret) map[string][]byte {
	if secret == nil || len(secret.Data) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(secret.Data))
	for key, value := range secret.Data {
		out[key] = append([]byte(nil), value...)
	}
	return out
}
