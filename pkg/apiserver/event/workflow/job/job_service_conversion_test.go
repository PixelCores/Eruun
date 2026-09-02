package job_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/conversion"
	workflowjob "github.com/PixelCores/Eruun/pkg/apiserver/event/workflow/job"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
)

func TestGenerateServiceFromConvertedEruunResources_RebindsSelectorToTargetComponent(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  labels:
    app.kubernetes.io/managed-by: eruun
    eruun.io/app-id: source-app
    eruun.io/component-id: "41"
    eruun.io/component-name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      eruun.io/app-id: source-app
      eruun.io/component-name: api
  template:
    metadata:
      labels:
        app.kubernetes.io/managed-by: eruun
        eruun.io/app-id: source-app
        eruun.io/component-id: "41"
        eruun.io/component-name: api
    spec:
      containers:
        - name: api
          image: nginx:1.27
---
apiVersion: v1
kind: Service
metadata:
  name: api
  namespace: default
spec:
  selector:
    eruun.io/app-id: source-app
    eruun.io/component-id: "41"
    eruun.io/component-name: api
  ports:
    - port: 80
      targetPort: 80
`
	converter := conversion.NewConversionServiceWithValidation(nil)

	resp, err := converter.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})

	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Len(t, resp.Components, 1)
	component := resp.Components[0]
	require.Len(t, component.Traits.Service, 1)
	require.Equal(t, "source-app", component.Traits.Service[0].Selector[config.LabelAppID])
	require.Equal(t, "41", component.Traits.Service[0].Selector[config.LabelComponentID])
	require.Equal(t, "api", component.Traits.Service[0].Selector[config.LabelComponentName])

	targetComponent := &model.ApplicationComponent{
		ID:        99,
		AppID:     "target-app",
		Name:      component.Name,
		Namespace: component.Namespace,
	}
	properties := model.Properties(component.Properties)
	podLabels := workflowjob.BuildLabels(targetComponent, &properties)
	generatedService := workflowjob.GenerateServiceFromTrait(targetComponent, &properties, component.Traits.Service[0])
	require.NotNil(t, generatedService)
	require.NotNil(t, generatedService.Spec)
	require.Equal(t, "target-app", generatedService.Spec.Selector[config.LabelAppID])
	require.Equal(t, "99", generatedService.Spec.Selector[config.LabelComponentID])
	require.Equal(t, "api", generatedService.Spec.Selector[config.LabelComponentName])
	for key, value := range generatedService.Spec.Selector {
		require.Equal(t, value, podLabels[key], "service selector %s must match the generated pod labels", key)
	}
}
