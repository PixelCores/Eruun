package spec

import "sort"

// Labels Eruun writes to identify the component that owns a resource and
// its share metadata.
const (
	LabelManagedBy     = "app.kubernetes.io/managed-by"
	LabelAppID         = "eruun.io/app-id"
	LabelComponentID   = "eruun.io/component-id"
	LabelComponentName = "eruun.io/component-name"
	LabelShareName     = "eruun.io/share-name"
	LabelShareStrategy = "eruun.io/share-strategy"
)

var reservedComponentLabelKeys = map[string]struct{}{
	LabelManagedBy:     {},
	LabelAppID:         {},
	LabelComponentID:   {},
	LabelComponentName: {},
	LabelShareName:     {},
	LabelShareStrategy: {},
}

// IsReservedComponentLabel reports whether user-supplied resource labels may
// not set key, because Eruun owns its value.
func IsReservedComponentLabel(key string) bool {
	_, ok := reservedComponentLabelKeys[key]
	return ok
}

// ReservedComponentLabelsIn returns the reserved keys present in labels in
// lexical order.
func ReservedComponentLabelsIn(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}
	reserved := make([]string, 0, len(labels))
	for key := range labels {
		if IsReservedComponentLabel(key) {
			reserved = append(reserved, key)
		}
	}
	sort.Strings(reserved)
	return reserved
}
