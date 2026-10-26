package conversion

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
)

func TestConvertKubeObjectsOrdersComponentsByBucketAndSkipsInvalidObjects(t *testing.T) {
	objects, err := decodeKubeObjects(`
apiVersion: batch/v1
kind: CronJob
metadata: {name: same}
spec:
  schedule: "* * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers: [{name: worker, image: busybox:1}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: same}
spec:
  template:
    spec:
      containers: [{name: server, image: nginx:1}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: same}
data: {key: value}
---
apiVersion: batch/v1
kind: Job
metadata: {name: same}
spec:
  template:
    spec:
      containers: [{name: worker, image: busybox:1}]
---
apiVersion: v1
kind: Secret
metadata: {name: same}
stringData: {key: value}
---
apiVersion: v1
kind: ConfigMap
metadata: {}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: skipped}
spec: {}
`)
	require.NoError(t, err)
	duplicate := objects[2].DeepCopy()
	input := append([]*unstructured.Unstructured{nil, duplicate}, objects...)
	components, warnings, err := convertKubeObjects(input, testConversionConfig)
	require.NoError(t, err)
	// ConfigMaps and Secrets come first, then workloads, Jobs and CronJobs; same-
	// named and duplicate inputs each keep their own component.
	wantTypes := []config.JobType{config.ConfJob, config.ConfJob, config.SecretJob, config.ServerJob, config.InstantJob, config.ScheduledJob}
	require.Len(t, components, len(wantTypes))
	for i, component := range components {
		require.Equal(t, "same", component.Name)
		require.Equal(t, wantTypes[i], component.ComponentType, "component %d", i)
	}
	require.Contains(t, warnings, "configmap missing metadata.name; skipped")
	require.Contains(t, warnings, "deployment skipped has no containers; skipped")
}

func TestConvertKubeObjectsReturnsNoPartialComponentsOnError(t *testing.T) {
	objects, err := decodeKubeObjects(`
apiVersion: v1
kind: ConfigMap
metadata: {name: valid}
---
apiVersion: v1
kind: Secret
metadata: {name: invalid}
data: {key: /w==}
`)
	require.NoError(t, err)
	components, _, err := convertKubeObjects(objects, testConversionConfig)
	require.ErrorContains(t, err, "non-UTF-8 binary data")
	require.Nil(t, components)
}
