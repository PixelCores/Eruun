package validation

import (
	"sort"

	applicationservice "github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
)

func reservedComponentLabelsIn(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}
	reservedKeys := applicationservice.ReservedComponentLabelKeys()
	reserved := make([]string, 0, len(labels))
	for key := range labels {
		if _, ok := reservedKeys[key]; ok {
			reserved = append(reserved, key)
		}
	}
	sort.Strings(reserved)
	return reserved
}
