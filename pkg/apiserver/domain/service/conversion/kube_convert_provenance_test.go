package conversion

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConvertKubeObjectsWithSourcesKeepsIdentityAcrossBucketsAndSkippedObjects(t *testing.T) {
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
	components, sources, warnings, err := ConvertKubeObjectsWithSources(input)
	require.NoError(t, err)
	require.Len(t, components, 6)
	require.Len(t, sources, len(components))
	wantSources := []*unstructured.Unstructured{duplicate, objects[2], objects[4], objects[1], objects[3], objects[0]}
	for i, source := range sources {
		require.Same(t, wantSources[i], source)
		require.Equal(t, source.GetName(), components[i].Name)
	}
	require.Contains(t, warnings, "configmap missing metadata.name; skipped")
	require.Contains(t, warnings, "deployment skipped has no containers; skipped")

	plainComponents, plainWarnings, err := ConvertKubeObjectsToComponents(input)
	require.NoError(t, err)
	require.Equal(t, components, plainComponents)
	require.Equal(t, warnings, plainWarnings)
}

func TestConvertKubeObjectsWithSourcesReturnsNoPartialProvenanceOnError(t *testing.T) {
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
	components, sources, _, err := ConvertKubeObjectsWithSources(objects)
	require.ErrorContains(t, err, "non-UTF-8 binary data")
	require.Nil(t, components)
	require.Nil(t, sources)
}
