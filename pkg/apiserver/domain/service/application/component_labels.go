package application

import (
	"sort"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

var reservedComponentLabelKeys = map[string]struct{}{
	config.LabelManagedBy:     {},
	config.LabelAppID:         {},
	config.LabelComponentID:   {},
	config.LabelComponentName: {},
	config.LabelImportAppKey:  {},
	config.LabelShareName:     {},
	config.LabelShareStrategy: {},
}

func reservedComponentLabelsIn(labels map[string]string) []string {
	if len(labels) == 0 {
		return nil
	}

	reserved := make([]string, 0, len(labels))
	for key := range labels {
		if _, ok := reservedComponentLabelKeys[key]; ok {
			reserved = append(reserved, key)
		}
	}
	sort.Strings(reserved)
	return reserved
}
