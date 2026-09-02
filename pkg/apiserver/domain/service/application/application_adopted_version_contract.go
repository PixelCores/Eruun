package application

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	spec "github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

// validateAdoptedVersionUpdateCompatibility keeps the DB contract aligned
// with the source-aware reconciler. Image, replicas and merge-style literal
// env updates are supported by the workload controller. Full Properties and
// arbitrary Traits replacements are rejected before persistence; the only
// Traits delta currently accepted is the requested size of an existing
// standalone PVC, whose live controller performs the final online-expansion
// checks.
func validateAdoptedVersionUpdateCompatibility(
	component *model.ApplicationComponent,
	update apisv1.ComponentUpdateSpec,
) error {
	if component == nil {
		return fmt.Errorf("%w: adopted component is nil", bcode.ErrApplicationManagementMode)
	}
	if update.Properties != nil {
		equal, err := componentPropertiesEqual(component.Properties, *update.Properties)
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf(
				"%w: adopted component %q properties changes require an explicit managed-field contract",
				bcode.ErrApplicationManagementMode,
				component.Name,
			)
		}
	}
	if update.Traits == nil {
		return nil
	}
	equal, err := componentTraitsEqual(component.Traits, *update.Traits)
	if err != nil || equal {
		return err
	}
	var current apisv1.Traits
	if err := decodeJSONStruct(component.Traits, &current); err != nil {
		return err
	}
	guarded, err := preserveTraitPVCIdentities(component.Name, component.ComponentType, current, *update.Traits, false)
	if err != nil {
		return err
	}
	if err := normalizeAdoptedStandalonePVCSizeChanges(current, &guarded); err != nil {
		return fmt.Errorf("%w: component %s %v", bcode.ErrApplicationManagementMode, component.Name, err)
	}
	if !reflect.DeepEqual(current, guarded) {
		return fmt.Errorf(
			"%w: adopted component %q traits change is unsupported; only existing standalone PVC growth is allowed",
			bcode.ErrApplicationManagementMode,
			component.Name,
		)
	}
	return nil
}

func normalizeAdoptedStandalonePVCSizeChanges(current apisv1.Traits, desired *apisv1.Traits) error {
	if desired == nil {
		return nil
	}
	index := buildPersistentStorageIndex(current)
	normalize := func(scope string, storages []spec.StorageTraitSpec) error {
		for storageIndex := range storages {
			storage := &storages[storageIndex]
			if storage.Type != config.StorageTypePersistent {
				continue
			}
			previous, found, err := matchPersistentStorageRef(index, scope, *storage)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if previous.storage.TmpCreate || storage.TmpCreate {
				if strings.TrimSpace(previous.storage.Size) != strings.TrimSpace(storage.Size) {
					return fmt.Errorf(
						"changes StatefulSet volumeClaimTemplate %q size; data migration is required",
						previous.storage.Name,
					)
				}
				continue
			}
			if strings.TrimSpace(previous.storage.Size) != strings.TrimSpace(storage.Size) {
				currentSize, err := resource.ParseQuantity(strings.TrimSpace(previous.storage.Size))
				if err != nil {
					return fmt.Errorf("has invalid current PVC size %q: %w", previous.storage.Size, err)
				}
				requestedSize, err := resource.ParseQuantity(strings.TrimSpace(storage.Size))
				if err != nil {
					return fmt.Errorf("has invalid requested PVC size %q: %w", storage.Size, err)
				}
				if requestedSize.Cmp(currentSize) <= 0 {
					return fmt.Errorf(
						"PVC %q size must grow from %s, requested %s",
						standalonePVCClaimName(previous.storage),
						currentSize.String(),
						requestedSize.String(),
					)
				}
			}
			// The live PVC reconciler validates that the new value is a strict
			// expansion and that StorageClass expansion is supported. Remove
			// only this accepted delta before comparing the remaining Traits.
			storage.Size = previous.storage.Size
		}
		return nil
	}
	if err := normalize("main", desired.Storage); err != nil {
		return err
	}
	for index := range desired.Init {
		scope := namedContainerScope("init", desired.Init[index].Name, index)
		if err := normalize(scope, desired.Init[index].Traits.Storage); err != nil {
			return err
		}
	}
	for index := range desired.Sidecar {
		scope := namedContainerScope("sidecar", desired.Sidecar[index].Name, index)
		if err := normalize(scope, desired.Sidecar[index].Traits.Storage); err != nil {
			return err
		}
	}
	return nil
}
