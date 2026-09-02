package application

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apisv1 "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func rewritePropertiesForTemplate(props *apisv1.Properties, rewriteMap *templateRewriteMap) {
	rewritePropertiesForTemplateSkippingEnv(props, rewriteMap, nil)
}

func rewritePropertiesForTemplateSkippingEnv(props *apisv1.Properties, rewriteMap *templateRewriteMap, skipEnv map[string]struct{}) {
	if props == nil || rewriteMap == nil {
		return
	}
	// Service selectors can target custom pod labels from properties.labels.
	rewriteStringMapValues(props.Labels, rewriteMap.rewriteValue)
	rewriteEnvMapValuesSkippingKeys(props.Env, rewriteMap, skipEnv)
	rewriteStringSlice(props.Command, rewriteMap.rewriteText)
}

func rewriteStringMapValues(values map[string]string, rewrite func(string) string) {
	rewriteStringMapValuesSkippingKeys(values, rewrite, nil)
}

func rewriteServiceSelectorValues(values map[string]string, rewriteMap *templateRewriteMap, originalPodLabels, rewrittenPodLabels map[string]string) {
	if len(values) == 0 || rewriteMap == nil {
		return
	}
	for key, value := range values {
		if key == config.LabelComponentName {
			if mapped, ok := rewriteMap.exactValue(value); ok {
				values[key] = mapped
			}
			continue
		}
		if shouldRewriteExactServiceSelectorValue(key) {
			if mapped, ok := rewriteMap.serviceValue(value); ok {
				values[key] = mapped
				continue
			}
		}
		originalLabelValue, ok := originalPodLabels[key]
		if !ok || originalLabelValue != value {
			continue
		}
		if labelValue, ok := rewrittenPodLabels[key]; ok {
			values[key] = labelValue
		}
	}
}

func shouldRewriteExactServiceSelectorValue(key string) bool {
	switch strings.TrimSpace(key) {
	case "name", "mysql-pod-role":
		return true
	default:
		return false
	}
}

func rewriteEnvMapValuesSkippingKeys(values map[string]string, rewriteMap *templateRewriteMap, skipKeys map[string]struct{}) {
	if len(values) == 0 || rewriteMap == nil {
		return
	}
	for key, value := range values {
		if _, ok := skipKeys[key]; ok {
			continue
		}
		if shouldRewriteExactServiceEnvValue(key) {
			if mapped, ok := rewriteMap.serviceValue(value); ok {
				values[key] = mapped
				continue
			}
		}
		values[key] = rewriteMap.rewriteText(value)
	}
}

func shouldRewriteExactServiceEnvValue(key string) bool {
	switch strings.TrimSpace(key) {
	case "MASTER_ROLE_NAME", "SLAVE_ROLE_NAME":
		return true
	default:
		return false
	}
}

func rewriteStringMapValuesSkippingKeys(values map[string]string, rewrite func(string) string, skipKeys map[string]struct{}) {
	if len(values) == 0 || rewrite == nil {
		return
	}
	for key, value := range values {
		if _, ok := skipKeys[key]; ok {
			continue
		}
		values[key] = rewrite(value)
	}
}

func rewriteStringSlice(values []string, rewrite func(string) string) {
	if len(values) == 0 || rewrite == nil {
		return
	}
	for i, value := range values {
		values[i] = rewrite(value)
	}
}

// rewriteTraitsForTemplate 通过模版重写特征等元数据
func rewriteTraitsForTemplate(traits *apisv1.Traits, oldName, newName, baseName, namespace string, rewriteMap *templateRewriteMap, originalPodLabels, rewrittenPodLabels map[string]string, initEnvOverrideKeys map[int]map[string]struct{}) error {
	return rewriteTraitsForTemplateWithPersistentStorageIdentities(
		traits,
		oldName,
		newName,
		baseName,
		namespace,
		rewriteMap,
		originalPodLabels,
		rewrittenPodLabels,
		initEnvOverrideKeys,
		make(map[string]spec.StorageTraitSpec),
		true,
	)
}

func rewriteTraitsForTemplateWithPersistentStorageIdentities(traits *apisv1.Traits, oldName, newName, baseName, namespace string, rewriteMap *templateRewriteMap, originalPodLabels, rewrittenPodLabels map[string]string, initEnvOverrideKeys map[int]map[string]struct{}, persistentStorageIdentities map[string]spec.StorageTraitSpec, collectTopLevelStorageIdentities bool) error {
	if traits == nil {
		return nil
	}

	rewriteNameCandidate := func(name string) string {
		if rewriteMap != nil {
			if mapped, ok := rewriteMap.exactValue(name); ok {
				return mapped
			}
		}
		if rewritten, ok := rewriteHyphenDelimitedName(name, oldName, newName); ok {
			return rewritten
		}
		return name
	}

	rewriteServiceReference := func(name, field string) (string, error) {
		if rewriteMap == nil {
			return name, nil
		}
		if mapped, ok := rewriteMap.serviceValue(name); ok {
			return mapped, nil
		}
		if candidates, ok := rewriteMap.ambiguousServiceCandidates(name); ok {
			return "", fmt.Errorf("%w: %s references ambiguous template service %q; candidates after clone: %s",
				bcode.ErrApplicationConfig, field, name, strings.Join(candidates, ", "))
		}
		return name, nil
	}

	rewriteStorageName := func(name string) (string, bool) {
		if name == "" || name == oldName {
			return newName, true
		}
		if rewritten, ok := rewriteHyphenDelimitedName(name, oldName, newName); ok {
			return rewritten, true
		}
		return name, false
	}

	rewriteTmpCreateStorageName := func(name string) (string, bool) {
		if name == "" || name == oldName {
			return newName, true
		}
		return name, false
	}

	volumePrefix := strings.TrimSpace(baseName)
	if volumePrefix == "" {
		volumePrefix = newName
	}

	for i := range traits.Storage {
		storage := &traits.Storage[i]
		originalName := strings.TrimSpace(storage.Name)
		if !collectTopLevelStorageIdentities && storage.Type == config.StorageTypePersistent {
			if parentStorage, ok := persistentStorageIdentities[originalName]; ok {
				inheritPersistentStorageIdentity(storage, parentStorage)
				if storage.SourceName != "" {
					storage.SourceName = rewriteNameCandidate(storage.SourceName)
				}
				continue
			}
		}
		if storage.Type == config.StorageTypePersistent {
			rewriteName := rewriteStorageName
			if storage.TmpCreate {
				rewriteName = rewriteTmpCreateStorageName
			}
			if rewritten, ok := rewriteName(storage.Name); ok {
				storage.Name = rewritten
			} else if !storage.TmpCreate && volumePrefix != "" && !(strings.HasPrefix(storage.Name, volumePrefix+"-") || storage.Name == volumePrefix) {
				storage.Name = fmt.Sprintf("%s-%s", volumePrefix, storage.Name)
			}
		} else {
			if storage.Name == "" || storage.Name == oldName {
				storage.Name = newName
			} else {
				storage.Name = rewriteNameCandidate(storage.Name)
			}
		}
		if storage.ClaimName != "" {
			storage.ClaimName = rewriteNameCandidate(storage.ClaimName)
		}
		if storage.SourceName != "" {
			storage.SourceName = rewriteNameCandidate(storage.SourceName)
		}
		if collectTopLevelStorageIdentities && storage.Type == config.StorageTypePersistent && originalName != "" {
			persistentStorageIdentities[originalName] = *storage
		}
	}

	for i := range traits.Ingress {
		ingress := &traits.Ingress[i]
		if ingress.Name == "" || ingress.Name == oldName {
			ingress.Name = newName
		} else {
			ingress.Name = rewriteNameCandidate(ingress.Name)
		}
		if ingress.Namespace == "" {
			ingress.Namespace = namespace
		}
		for j := range ingress.Routes {
			field := fmt.Sprintf("component %s traits.ingress[%d].routes[%d].backend.serviceName", newName, i, j)
			serviceName, err := rewriteServiceReference(ingress.Routes[j].Backend.ServiceName, field)
			if err != nil {
				return err
			}
			ingress.Routes[j].Backend.ServiceName = serviceName
		}
	}

	serviceTargets := make(map[string]int, len(traits.Service))
	for i := range traits.Service {
		service := &traits.Service[i]
		if service.Name != "" {
			if mapped, ok := rewriteMap.serviceValue(service.Name); ok {
				service.Name = mapped
			} else {
				service.Name = rewriteTemplateServiceNameForTrait(service.Name, oldName, newName, baseName, service.Type)
			}
			if err := validateTemplateServiceName(service.Name, fmt.Sprintf("component %s traits.service[%d].name", newName, i)); err != nil {
				return err
			}
			if previousIndex, ok := serviceTargets[service.Name]; ok {
				return fmt.Errorf("%w: component %s traits.service[%d].name rewrites to duplicate service name %q already used by traits.service[%d].name",
					bcode.ErrApplicationConfig, newName, i, service.Name, previousIndex)
			}
			serviceTargets[service.Name] = i
		}
		rewriteStringMapValues(service.Labels, rewriteMap.rewriteValue)
		rewriteServiceSelectorValues(service.Selector, rewriteMap, originalPodLabels, rewrittenPodLabels)
		service.ExternalName = rewriteMap.rewriteText(service.ExternalName)
	}

	for i := range traits.RBAC {
		policy := &traits.RBAC[i]
		// RBAC 资源保持名称不变，但命名空间与组件命名空间对齐（为空则用默认命名空间）。
		if namespace != "" {
			policy.Namespace = namespace
		} else if policy.Namespace == "" {
			policy.Namespace = config.DefaultNamespace
		}
	}

	for i := range traits.EnvFrom {
		traits.EnvFrom[i].SourceName = rewriteNameCandidate(traits.EnvFrom[i].SourceName)
	}

	for i := range traits.Envs {
		env := &traits.Envs[i]
		if env.ValueFrom.Secret != nil {
			env.ValueFrom.Secret.Name = rewriteNameCandidate(env.ValueFrom.Secret.Name)
		}
		if env.ValueFrom.Config != nil {
			env.ValueFrom.Config.Name = rewriteNameCandidate(env.ValueFrom.Config.Name)
		}
	}

	for i := range traits.Init {
		initTrait := &traits.Init[i]
		if initTrait.Name == "" || initTrait.Name == oldName {
			initTrait.Name = fmt.Sprintf("%s-init-%d", newName, i+1)
		}
		rewritePropertiesForTemplateSkippingEnv(&initTrait.Properties, rewriteMap, initEnvOverrideKeys[i])
		if err := rewriteTraitsForTemplateWithPersistentStorageIdentities(&initTrait.Traits, oldName, newName, baseName, namespace, rewriteMap, nil, nil, nil, persistentStorageIdentities, false); err != nil {
			return err
		}
	}

	for i := range traits.Sidecar {
		sidecar := &traits.Sidecar[i]
		if sidecar.Name == "" || sidecar.Name == oldName {
			sidecar.Name = fmt.Sprintf("%s-sidecar-%d", newName, i+1)
		}
		rewriteStringMapValues(sidecar.Env, rewriteMap.rewriteText)
		rewriteStringSlice(sidecar.Command, rewriteMap.rewriteText)
		rewriteStringSlice(sidecar.Args, rewriteMap.rewriteText)
		if err := rewriteTraitsForTemplateWithPersistentStorageIdentities(&sidecar.Traits, oldName, newName, baseName, namespace, rewriteMap, nil, nil, nil, persistentStorageIdentities, false); err != nil {
			return err
		}
	}
	return nil
}

func inheritPersistentStorageIdentity(storage *spec.StorageTraitSpec, parentStorage spec.StorageTraitSpec) {
	storage.Name = parentStorage.Name
	storage.TmpCreate = parentStorage.TmpCreate
	storage.ClaimName = parentStorage.ClaimName
	storage.Size = parentStorage.Size
	storage.StorageClass = parentStorage.StorageClass
}
