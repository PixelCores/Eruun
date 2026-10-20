package jobtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

func CountClientActions(client *fake.Clientset, verb, resource string) int {
	count := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == verb && action.GetResource().Resource == resource {
			count++
		}
	}
	return count
}

func Int32Ptr(v int32) *int32 { return &v }

func StartWorkloadTestObserver(t *testing.T, client kubernetes.Interface) *informer.KubernetesWorkloadObserver {
	t.Helper()
	observer := informer.NewKubernetesWorkloadObserver(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, observer.Start(ctx))
	return observer
}
