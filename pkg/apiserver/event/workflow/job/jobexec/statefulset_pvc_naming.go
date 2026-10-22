package jobexec

import (
	"fmt"
	"strings"
)

func IsStatefulSetTemplatePVCName(templateName, statefulSetName, pvcName string) bool {
	templateName = strings.TrimSpace(templateName)
	statefulSetName = strings.TrimSpace(statefulSetName)
	pvcName = strings.TrimSpace(pvcName)
	if templateName == "" || statefulSetName == "" || pvcName == "" {
		return false
	}
	prefix := fmt.Sprintf("%s-%s-", templateName, statefulSetName)
	suffix, ok := strings.CutPrefix(pvcName, prefix)
	if !ok || suffix == "" {
		return false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
