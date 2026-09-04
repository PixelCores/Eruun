package resourceimport

import (
	"fmt"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
)

func assignResourcesToApps(namespace string, resources []*importResource) (map[string][]*importResource, map[string]string, map[string]string, []string) {
	sharedAppID := sharedAppIDForNamespace(namespace)
	prefixVotes, warnings := assignInitialResourceOwners(namespace, resources, sharedAppID)
	workloadsByApp, warnings := buildImportWorkloadReferences(resources, warnings)
	assignReferenceDerivedOwners(namespace, resources, sharedAppID, workloadsByApp)
	grouped := groupImportResources(resources, sharedAppID)
	appNames, appAliases := buildImportAppPresentation(grouped, prefixVotes, sharedAppID)
	return grouped, appNames, appAliases, warnings
}

func assignInitialResourceOwners(namespace string, resources []*importResource, sharedAppID string) (map[string]map[string]int, []string) {
	warnings := make([]string, 0)
	prefixVotes := make(map[string]map[string]int)

	for _, res := range resources {
		labelImportAppKey := strings.TrimSpace(res.labels[config.LabelImportAppKey])
		labelAppID := strings.TrimSpace(res.labels[config.LabelAppID])
		clusterScoped := isClusterScopedImportKind(res.kindKey)
		prefix := ""
		parsedAppID := ""
		parsed := false
		if supportsNameBasedAppInference(res.kindKey) {
			prefix, parsedAppID, _, parsed = parseStrictResourceName(res.name)
		}
		if clusterScoped {
			if labelImportAppKey != "" && !strings.EqualFold(labelImportAppKey, sharedAppID) {
				warnings = append(warnings, fmt.Sprintf("resource %s/%s import app key %q ignored for cluster-scoped resource in namespace %q", res.kind, res.name, labelImportAppKey, namespace))
			}
			if labelAppID != "" && !strings.EqualFold(labelAppID, sharedAppID) {
				warnings = append(warnings, fmt.Sprintf("resource %s/%s label appId %q ignored for cluster-scoped resource in namespace %q", res.kind, res.name, labelAppID, namespace))
			}
			res.appID = sharedAppID
		} else if labelImportAppKey != "" {
			res.appID = labelImportAppKey
			res.explicitAppID = true
			if labelAppID != "" && !strings.EqualFold(labelImportAppKey, labelAppID) {
				warnings = append(warnings, fmt.Sprintf("resource %s/%s import app key %q differs from label appId %q; import app key wins", res.kind, res.name, labelImportAppKey, labelAppID))
			}
			if parsed && parsedAppID != "" && !strings.EqualFold(parsedAppID, labelImportAppKey) {
				warnings = append(warnings, fmt.Sprintf("resource %s/%s import app key %q differs from parsed appId %q; import app key wins", res.kind, res.name, labelImportAppKey, parsedAppID))
			}
		} else if labelAppID != "" {
			res.appID = labelAppID
			res.explicitAppID = true
			if parsed && parsedAppID != "" && !strings.EqualFold(parsedAppID, labelAppID) {
				warnings = append(warnings, fmt.Sprintf("resource %s/%s label appId %q differs from parsed appId %q; label wins", res.kind, res.name, labelAppID, parsedAppID))
			}
		} else if parsed {
			res.appID = parsedAppID
		} else {
			res.appID = sharedAppID
		}

		if res.componentName == "" {
			if existing := strings.TrimSpace(res.labels[config.LabelComponentName]); existing != "" {
				res.componentName = existing
			} else if res.kindKey == importKindDeployments ||
				res.kindKey == importKindStatefulSets ||
				res.kindKey == importKindDaemonSets ||
				res.kindKey == importKindJobs ||
				res.kindKey == importKindCronJobs ||
				res.kindKey == importKindConfigMaps ||
				res.kindKey == importKindSecrets {
				res.componentName = res.name
			}
		}

		if res.appID != sharedAppID && prefix != "" {
			if _, ok := prefixVotes[res.appID]; !ok {
				prefixVotes[res.appID] = make(map[string]int)
			}
			prefixVotes[res.appID][prefix]++
		}
	}
	return prefixVotes, warnings
}

func buildImportWorkloadReferences(resources []*importResource, warnings []string) (map[string][]workloadRef, []string) {
	workloadsByApp := make(map[string][]workloadRef)
	for _, res := range resources {
		if res.kindKey != importKindDeployments &&
			res.kindKey != importKindStatefulSets &&
			res.kindKey != importKindDaemonSets &&
			res.kindKey != importKindJobs &&
			res.kindKey != importKindCronJobs {
			continue
		}
		ref, err := buildWorkloadRef(res)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("extract workload refs from %s/%s failed: %v", res.kind, res.name, err))
			continue
		}
		workloadsByApp[ref.appID] = append(workloadsByApp[ref.appID], ref)
	}
	return workloadsByApp, warnings
}

func assignReferenceDerivedOwners(namespace string, resources []*importResource, sharedAppID string, workloadsByApp map[string][]workloadRef) {
	configMapRefOwners, configMapRefComponents := collectReferenceOwners(workloadsByApp, importKindConfigMaps)
	pvcRefOwners, pvcRefComponents := collectReferenceOwners(workloadsByApp, importKindPersistentVolumeClaims)
	pvcPrefixOwners, pvcPrefixComponents := collectPVCPrefixOwners(workloadsByApp)
	secretRefOwners, secretRefComponents := collectReferenceOwners(workloadsByApp, importKindSecrets)
	serviceAccountOwners, serviceAccountComponents := collectServiceAccountOwners(workloadsByApp)

	serviceByName := make(map[string]*importResource)
	for _, res := range resources {
		switch res.kindKey {
		case importKindServices:
			serviceByName[res.name] = res
			if res.explicitAppID {
				continue
			}
			if res.appID != sharedAppID {
				continue
			}
			owners, components := matchServiceOwners(res, workloadsByApp)
			if len(owners) == 1 {
				for owner := range owners {
					res.appID = owner
				}
				if len(components) == 1 {
					for c := range components {
						res.componentName = c
					}
				}
			}
		case importKindConfigMaps:
			if res.explicitAppID || res.appID != sharedAppID {
				continue
			}
			owners := configMapRefOwners[res.name]
			if len(owners) == 1 {
				for owner := range owners {
					res.appID = owner
					res.componentName = pickSingleComponent(configMapRefComponents[componentRefKey(owner, res.name)], res.componentName)
				}
			}
		case importKindPersistentVolumeClaims:
			if res.explicitAppID {
				continue
			}
			owners := pvcRefOwners[res.name]
			prefixComponentsByOwner := map[string]map[string]struct{}{}
			if len(owners) == 0 {
				owners, prefixComponentsByOwner = matchPVCPrefixOwners(res.name, pvcPrefixOwners, pvcPrefixComponents)
			}
			if len(owners) == 1 {
				for owner := range owners {
					res.appID = owner
					componentCandidates := pvcRefComponents[componentRefKey(owner, res.name)]
					if len(componentCandidates) == 0 {
						componentCandidates = prefixComponentsByOwner[owner]
					}
					res.componentName = pickSingleComponent(componentCandidates, res.componentName)
				}
			}
		case importKindSecrets:
			if res.explicitAppID || res.appID != sharedAppID {
				continue
			}
			owners := secretRefOwners[res.name]
			if len(owners) == 1 {
				for owner := range owners {
					res.appID = owner
					res.componentName = pickSingleComponent(secretRefComponents[componentRefKey(owner, res.name)], res.componentName)
				}
			}
		case importKindServiceAccounts:
			if res.explicitAppID || res.appID != sharedAppID {
				continue
			}
			owners := serviceAccountOwners[res.name]
			if len(owners) == 1 {
				for owner := range owners {
					res.appID = owner
					res.componentName = pickSingleComponent(serviceAccountComponents[componentRefKey(owner, res.name)], res.componentName)
				}
			}
		}
	}

	for _, res := range resources {
		if res.kindKey != importKindIngresses {
			continue
		}
		if res.explicitAppID || res.appID != sharedAppID {
			continue
		}
		owners, components := matchIngressOwners(res, serviceByName)
		if len(owners) != 1 {
			continue
		}
		for owner := range owners {
			res.appID = owner
		}
		if len(components) == 1 {
			for c := range components {
				res.componentName = c
			}
		}
	}

	for _, res := range resources {
		if res.kindKey != importKindRoleBindings {
			continue
		}
		if res.explicitAppID || res.appID != sharedAppID {
			continue
		}
		owners, componentCandidates, _, _ := matchRoleBindingOwners(namespace, res, serviceAccountOwners, serviceAccountComponents)
		if len(owners) == 1 {
			for owner := range owners {
				res.appID = owner
				res.componentName = pickSingleComponent(componentCandidates[componentRefKey(owner, res.name)], res.componentName)
			}
		}
	}

	for _, res := range resources {
		if res.kindKey != importKindClusterRoleBindings {
			continue
		}
		if res.explicitAppID || res.appID != sharedAppID {
			continue
		}
		owners, componentCandidates, _ := matchClusterRoleBindingOwners(namespace, res, serviceAccountOwners, serviceAccountComponents)
		if len(owners) == 1 {
			for owner := range owners {
				res.appID = owner
				res.componentName = pickSingleComponent(componentCandidates[componentRefKey(owner, res.name)], res.componentName)
			}
		}
	}
}

func groupImportResources(resources []*importResource, sharedAppID string) map[string][]*importResource {
	grouped := make(map[string][]*importResource)
	for _, res := range resources {
		if strings.TrimSpace(res.appID) == "" {
			res.appID = sharedAppID
		}
		grouped[res.appID] = append(grouped[res.appID], res)
	}
	return grouped
}

func buildImportAppPresentation(grouped map[string][]*importResource, prefixVotes map[string]map[string]int, sharedAppID string) (map[string]string, map[string]string) {
	appNames := make(map[string]string, len(grouped))
	appAliases := make(map[string]string, len(grouped))
	for appID := range grouped {
		if appID == sharedAppID {
			appNames[appID] = boundedRFC1123AppName(sharedAppID)
			appAliases[appID] = sharedAppID
			continue
		}
		prefix := pickTopPrefix(prefixVotes[appID])
		if prefix == "" {
			prefix = appID
		}
		nameRaw := appID
		if !strings.EqualFold(prefix, appID) {
			nameRaw = fmt.Sprintf("%s-%s", prefix, appID)
		}
		appNames[appID] = boundedRFC1123AppName(nameRaw)
		appAliases[appID] = utils.ToRFC1123Name(prefix)
	}
	return appNames, appAliases
}
