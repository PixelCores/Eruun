package naming

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	maxResourceNameLength                = 63
	maxControllerOwnedResourceNameLength = 52
	hashSuffixLength                     = 10
	defaultComponentName                 = "component"
	defaultAppSegment                    = "app"
	defaultLabelValue                    = "label"
)

// WebServiceName builds a deterministic deployment name for stateless components.
func WebServiceName(name, appName string) string {
	return buildResourceName(maxResourceNameLength, appName, name)
}

// ServiceName builds a deterministic Service name for components.
func ServiceName(name, appName string) string {
	return buildResourceName(maxResourceNameLength, appName, name)
}

// StoreServerName builds a StatefulSet name for store components.
func StoreServerName(name, appName string) string {
	return buildResourceName(maxControllerOwnedResourceNameLength, appName, name)
}

// IngressName builds an ingress resource name tied to the component/app pair.
func IngressName(name, appName string) string {
	return buildResourceName(maxResourceNameLength, appName, name)
}

// PVCName formats PVC names as <appName>-<traitName> with normalized segments.
func PVCName(traitName, appName string) string {
	return buildResourceName(maxResourceNameLength, appName, normalizeSegment(traitName, "data"))
}

// JobName builds a deterministic Job name for instant tasks.
func JobName(name, appName string) string {
	return buildResourceName(maxResourceNameLength, appName, name)
}

// CronJobName builds a deterministic CronJob name for scheduled tasks.
func CronJobName(name, appName string) string {
	return buildResourceName(maxControllerOwnedResourceNameLength, appName, name)
}

// ApplicationResourceKey normalizes the app segment used in generated names.
// Passing templateEnabled includes version for catalog-style keys; runtime
// workload naming should pass templateEnabled=false.
func ApplicationResourceKey(appName, version string, templateEnabled bool) string {
	app := normalizeSegment(appName, defaultAppSegment)
	if !templateEnabled {
		return app
	}
	version = normalizeSegment(version, "")
	if version == "" {
		return app
	}
	return fmt.Sprintf("%s-%s", app, version)
}

// BoundedLabelValue normalizes a Kubernetes label value and keeps it within the
// 63-character label value limit with a stable hash suffix when needed.
func BoundedLabelValue(value string) string {
	return boundedRFC1123Name(value, maxResourceNameLength, defaultLabelValue)
}

// NormalizeLabelValue converts user-provided Kubernetes label values to the
// normalized form Eruun uses for generated resource labels. Empty label
// values are valid Kubernetes values and are preserved.
func NormalizeLabelValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return boundedRFC1123Name(value, maxResourceNameLength, defaultLabelValue)
}

// NormalizeInvalidLabelValue preserves Kubernetes-valid label values and only
// normalizes values that Kubernetes would reject.
func NormalizeInvalidLabelValue(value string) string {
	if len(validation.IsValidLabelValue(value)) == 0 {
		return value
	}
	return NormalizeLabelValue(value)
}

// NormalizeLabelValues copies a label map and normalizes its values while
// preserving label keys.
func NormalizeLabelValues(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(values))
	for key, value := range values {
		normalized[key] = NormalizeLabelValue(value)
	}
	return normalized
}

func buildResourceName(maxLen int, appName, componentName string) string {
	component := normalizeSegment(componentName, defaultComponentName)
	app := normalizeSegment(appName, defaultAppSegment)
	return boundedRFC1123Name(resourceSubject(app, component), maxLen, defaultComponentName)
}

func resourceSubject(app, component string) string {
	if app == "" {
		return component
	}
	if component == "" {
		return app
	}
	if component == app || strings.HasPrefix(component, app+"-") {
		return component
	}
	return fmt.Sprintf("%s-%s", app, component)
}

func normalizeSegment(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	normalized := utils.ToRFC1123Name(value)
	if normalized == "" {
		return fallback
	}
	return normalized
}

func boundedRFC1123Name(value string, maxLen int, fallback string) string {
	if maxLen < 1 {
		maxLen = maxResourceNameLength
	}
	normalized := utils.ToRFC1123Name(value)
	if normalized == "" {
		normalized = fallback
	}
	if len(normalized) <= maxLen {
		return normalized
	}

	hashPart := stableHashSuffix(normalized, hashSuffixLength)
	prefixLen := maxLen - len(hashPart) - 1
	if prefixLen < 1 {
		if len(hashPart) > maxLen {
			return hashPart[:maxLen]
		}
		return hashPart
	}
	prefix := strings.Trim(normalized[:prefixLen], "-")
	if prefix == "" {
		prefix = fallback
		if len(prefix) > prefixLen {
			prefix = prefix[:prefixLen]
		}
	}
	return fmt.Sprintf("%s-%s", prefix, hashPart)
}

func stableHashSuffix(value string, size int) string {
	if size <= 0 {
		return ""
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(value))
	hash := strconv.FormatUint(hasher.Sum64(), 36)
	if len(hash) >= size {
		return hash[:size]
	}
	return strings.Repeat("0", size-len(hash)) + hash
}
