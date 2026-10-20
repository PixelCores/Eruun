package job

import (
	"testing"

	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobexec"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobinstant"
	"github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job/jobtest"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

func newObservedInstantJobCtl(t *testing.T, task *model.JobTask, client kubernetes.Interface, store datastore.DataStore, ack func()) *jobinstant.InstantJobCtl {
	t.Helper()
	return jobinstant.NewInstantJobCtl(task, &jobexec.Runtime{Client: client, Store: store, Ack: ack, ResourceWaiter: jobtest.StartJobTestObserver(t, client)})
}
