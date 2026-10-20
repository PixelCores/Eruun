package jobtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

func StartJobTestObserver(t *testing.T, client kubernetes.Interface) *informer.KubernetesWorkloadObserver {
	t.Helper()
	observer := informer.NewKubernetesWorkloadObserver(client)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, observer.Start(ctx))
	return observer
}
