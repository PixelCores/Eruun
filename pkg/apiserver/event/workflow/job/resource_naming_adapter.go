package job

import (
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/naming"
)

// Keep naming wrappers in this file so callers in job package stay concise.
func buildWebServiceName(name, appID string) string { return naming.WebServiceName(name, appID) }
func buildServiceName(name, appID string) string    { return naming.ServiceName(name, appID) }
func buildStoreSeverName(name, appID string) string { return naming.StoreServerName(name, appID) }
func buildJobName(name, appID string) string        { return naming.JobName(name, appID) }
func buildCronJobName(name, appID string) string    { return naming.CronJobName(name, appID) }

// BuildIngressName returns a normalized ingress resource name for the given component/app.
func BuildIngressName(name, appID string) string { return naming.IngressName(name, appID) }
