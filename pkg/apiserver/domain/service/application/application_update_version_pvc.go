package application

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

type persistentStorageRef struct {
	storage spec.StorageTraitSpec
	source  string
}

type persistentStorageIndex struct {
	byScopedName  map[string]persistentStorageRef
	byScopedMount map[string][]persistentStorageRef
}

func preserveVersionUpdatePVCIdentities(componentMap map[string]*model.ApplicationComponent, specs []apisv1.ComponentUpdateSpec, fullRecreate bool) ([]apisv1.ComponentUpdateSpec, error) {
	if len(specs) == 0 {
		return specs, nil
	}
	normalized := make([]apisv1.ComponentUpdateSpec, len(specs))
	copy(normalized, specs)

	for idx := range normalized {
		specUpdate := &normalized[idx]
		action, err := parseVersionUpdateComponentAction(*specUpdate)
		if err != nil {
			return nil, err
		}
		if action != config.ComponentActionUpdate || specUpdate.Traits == nil {
			continue
		}
		componentName := strings.TrimSpace(specUpdate.Name)
		if componentName == "" {
			continue
		}
		component, exists := componentMap[strings.ToLower(componentName)]
		if !exists || component == nil {
			continue
		}
		var currentTraits apisv1.Traits
		if err := decodeJSONStruct(component.Traits, &currentTraits); err != nil {
			return nil, err
		}
		allowStatefulSetRecreate := versionUpdateRecreatesStatefulSet(component, fullRecreate)
		guardedTraits, err := preserveTraitPVCIdentities(component.Name, component.ComponentType, currentTraits, *specUpdate.Traits, allowStatefulSetRecreate)
		if err != nil {
			return nil, err
		}
		specUpdate.Traits = &guardedTraits
	}
	return normalized, nil
}

func preserveTraitPVCIdentities(componentName string, componentType config.JobType, current, desired apisv1.Traits, allowStatefulSetRecreate bool) (apisv1.Traits, error) {
	index := buildPersistentStorageIndex(current)
	if len(index.byScopedName) == 0 && len(index.byScopedMount) == 0 {
		return desired, nil
	}

	guard := func(scope string, storages []spec.StorageTraitSpec, sourcePrefix string) error {
		for storageIndex := range storages {
			storage := &storages[storageIndex]
			if storage.Type != config.StorageTypePersistent {
				continue
			}
			previous, ok, err := matchPersistentStorageRef(index, scope, *storage)
			if err != nil {
				return fmt.Errorf("%w: component %s %s[%d] has ambiguous persistent storage identity: %v",
					bcode.ErrApplicationConfig, componentName, sourcePrefix, storageIndex, err)
			}
			if !ok {
				continue
			}
			field, err := preserveStoragePVCIdentity(componentType, storage, previous, allowStatefulSetRecreate)
			if err != nil {
				internalErr := fmt.Errorf("%w: component %s %s[%d] %s",
					bcode.ErrApplicationConfig, componentName, sourcePrefix, storageIndex, err)
				return bcode.WithSafeClientMessage(internalErr, storagePVCIdentityClientMessage(componentName, componentType, field))
			}
		}
		return nil
	}

	if err := guard("main", desired.Storage, "traits.storage"); err != nil {
		return apisv1.Traits{}, err
	}
	for initIndex := range desired.Init {
		scope := namedContainerScope("init", desired.Init[initIndex].Name, initIndex)
		if err := guard(scope, desired.Init[initIndex].Traits.Storage, fmt.Sprintf("traits.init[%d].traits.storage", initIndex)); err != nil {
			return apisv1.Traits{}, err
		}
	}
	for sidecarIndex := range desired.Sidecar {
		scope := namedContainerScope("sidecar", desired.Sidecar[sidecarIndex].Name, sidecarIndex)
		if err := guard(scope, desired.Sidecar[sidecarIndex].Traits.Storage, fmt.Sprintf("traits.sidecar[%d].traits.storage", sidecarIndex)); err != nil {
			return apisv1.Traits{}, err
		}
	}
	return desired, nil
}

func storagePVCIdentityClientMessage(componentName string, componentType config.JobType, field string) string {
	if componentType != config.StoreJob {
		return fmt.Sprintf("component %s changes persistent storage mode; explicit PVC data migration is required", componentName)
	}
	return fmt.Sprintf(
		"component %s changes StatefulSet immutable field %s; explicit StatefulSet/PVC migration or recreation is required",
		componentName, field)
}

func buildPersistentStorageIndex(traits apisv1.Traits) persistentStorageIndex {
	index := persistentStorageIndex{
		byScopedName:  make(map[string]persistentStorageRef),
		byScopedMount: make(map[string][]persistentStorageRef),
	}
	addStoragesToPersistentStorageIndex(&index, "main", traits.Storage, "traits.storage")
	for initIndex, init := range traits.Init {
		addStoragesToPersistentStorageIndex(&index, namedContainerScope("init", init.Name, initIndex), init.Traits.Storage, fmt.Sprintf("traits.init[%d].traits.storage", initIndex))
	}
	for sidecarIndex, sidecar := range traits.Sidecar {
		addStoragesToPersistentStorageIndex(&index, namedContainerScope("sidecar", sidecar.Name, sidecarIndex), sidecar.Traits.Storage, fmt.Sprintf("traits.sidecar[%d].traits.storage", sidecarIndex))
	}
	return index
}

func addStoragesToPersistentStorageIndex(index *persistentStorageIndex, scope string, storages []spec.StorageTraitSpec, sourcePrefix string) {
	for storageIndex, storage := range storages {
		if storage.Type != config.StorageTypePersistent {
			continue
		}
		ref := persistentStorageRef{
			storage: storage,
			source:  fmt.Sprintf("%s[%d]", sourcePrefix, storageIndex),
		}
		if nameKey := scopedStorageNameKey(scope, storage.Name); nameKey != "" {
			if _, exists := index.byScopedName[nameKey]; !exists {
				index.byScopedName[nameKey] = ref
			}
		}
		if mountKey := scopedStorageMountKey(scope, storage.MountPath, storage.SubPath, storage.SubPathExpr); mountKey != "" {
			index.byScopedMount[mountKey] = append(index.byScopedMount[mountKey], ref)
		}
	}
}

func matchPersistentStorageRef(index persistentStorageIndex, scope string, storage spec.StorageTraitSpec) (persistentStorageRef, bool, error) {
	if nameKey := scopedStorageNameKey(scope, storage.Name); nameKey != "" {
		if ref, ok := index.byScopedName[nameKey]; ok {
			return ref, true, nil
		}
	}
	mountKey := scopedStorageMountKey(scope, storage.MountPath, storage.SubPath, storage.SubPathExpr)
	if mountKey == "" {
		return persistentStorageRef{}, false, nil
	}
	candidates := index.byScopedMount[mountKey]
	switch len(candidates) {
	case 0:
		return persistentStorageRef{}, false, nil
	case 1:
		return candidates[0], true, nil
	default:
		sources := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			sources = append(sources, candidate.source)
		}
		return persistentStorageRef{}, false, fmt.Errorf("matches %s", strings.Join(sources, ", "))
	}
}

func preserveStoragePVCIdentity(componentType config.JobType, desired *spec.StorageTraitSpec, previous persistentStorageRef, allowStatefulSetRecreate bool) (string, error) {
	if previous.storage.TmpCreate != desired.TmpCreate {
		if componentType == config.StoreJob && allowStatefulSetRecreate {
			return "", nil
		}
		return "volumeClaimTemplates", fmt.Errorf("changes storage mode from %s to %s; PVC data migration is required",
			storageModeName(previous.storage), storageModeName(*desired))
	}
	if desired.TmpCreate {
		if componentType == config.StoreJob && !allowStatefulSetRecreate && strings.TrimSpace(previous.storage.Name) != strings.TrimSpace(desired.Name) {
			return "volumeClaimTemplates.name", fmt.Errorf("changes StatefulSet volumeClaimTemplate name from %q to %q; PVC data migration is required",
				strings.TrimSpace(previous.storage.Name), strings.TrimSpace(desired.Name))
		}
		return "", nil
	}
	if claimName := standalonePVCClaimName(previous.storage); claimName != "" {
		desired.ClaimName = claimName
	}
	return "", nil
}

func storageModeName(storage spec.StorageTraitSpec) string {
	if storage.TmpCreate {
		return "volumeClaimTemplate"
	}
	return "standalone PVC"
}

func standalonePVCClaimName(storage spec.StorageTraitSpec) string {
	if storage.TmpCreate {
		return ""
	}
	claimName := strings.TrimSpace(storage.ClaimName)
	if claimName != "" {
		return claimName
	}
	return strings.TrimSpace(storage.Name)
}

func namedContainerScope(kind, name string, index int) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = fmt.Sprintf("%d", index)
	}
	return kind + ":" + strings.ToLower(name)
}

func scopedStorageNameKey(scope, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return scope + "\x00name\x00" + strings.ToLower(name)
}

func scopedStorageMountKey(scope, mountPath, subPath, subPathExpr string) string {
	mountPath = strings.TrimSpace(mountPath)
	if mountPath == "" {
		return ""
	}
	return scope + "\x00mount\x00" + mountPath + "\x00" + strings.TrimSpace(subPath) + "\x00" + strings.TrimSpace(subPathExpr)
}
