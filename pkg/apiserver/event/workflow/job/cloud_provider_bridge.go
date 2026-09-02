package job

import (
	wfcloudjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob"
	wfcloudcontract "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/cloudjob/contracts"
)

type CloudJobInfo = wfcloudcontract.CloudJobInfo
type CloudJobRequest = wfcloudcontract.CloudJobRequest
type CloudJobResult = wfcloudcontract.CloudJobResult

type CloudRuntime = wfcloudcontract.CloudRuntime
type CloudActionProgress = wfcloudcontract.CloudActionProgress
type CloudAction = wfcloudcontract.CloudAction
type CloudActionRegistry = wfcloudcontract.CloudActionRegistry
type CloudProvider = wfcloudcontract.CloudProvider

var (
	errCloudProviderNotFound = wfcloudjob.ErrCloudProviderNotFound
	errCloudActionNotFound   = wfcloudjob.ErrCloudActionNotFound
)

func RegisterCloudProvider(provider CloudProvider) {
	wfcloudjob.RegisterCloudProvider(provider)
}

func getCloudProvider(name string) (CloudProvider, bool) {
	return wfcloudjob.GetCloudProvider(name)
}

func normalizeProviderName(name string) string {
	return wfcloudjob.NormalizeProviderName(name)
}

func resetCloudProvidersForTest() {
	wfcloudjob.ResetCloudProvidersForTest()
}

func restoreBuiltinCloudProvidersForTest() {
	wfcloudjob.RestoreBuiltinCloudProvidersForTest()
}
