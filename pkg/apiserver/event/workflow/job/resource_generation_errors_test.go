package job

import (
	"encoding/json"
	"testing"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	traitsPlu "github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
	"github.com/stretchr/testify/require"
)

func TestResourceGeneratorsReturnTraitConflict(t *testing.T) {
	traitsPlu.RegisterAllProcessors()
	storage := spec.StorageTraitSpec{Name: "data", Type: "persistent", MountPath: "/data", Size: "1Gi"}
	conflict := storage
	conflict.Size = "2Gi"
	traits, err := model.NewJSONStructByStruct(spec.Traits{
		Storage: []spec.StorageTraitSpec{storage},
		Sidecar: []spec.SidecarTraitsSpec{{Name: "helper", Image: "busybox:1.36", Traits: spec.Traits{Storage: []spec.StorageTraitSpec{conflict}}}},
	})
	require.NoError(t, err)
	for _, tt := range []struct {
		name     string
		generate func(*model.ApplicationComponent, *model.Properties) (*GenerateServiceResult, error)
	}{
		{"deployment", GenerateWebService},
		{"statefulset", func(c *model.ApplicationComponent, _ *model.Properties) (*GenerateServiceResult, error) {
			return GenerateStoreService(c)
		}},
		{"instant", func(c *model.ApplicationComponent, p *model.Properties) (*GenerateServiceResult, error) {
			return GenerateInstantJob(c, p, "")
		}},
		{"one time", func(c *model.ApplicationComponent, p *model.Properties) (*GenerateServiceResult, error) {
			return GenerateOneTimeJob(c, p, "", 123)
		}},
		{"cron", func(c *model.ApplicationComponent, p *model.Properties) (*GenerateServiceResult, error) {
			return GenerateScheduledCronJob(c, p, "0 * * * *")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			component := &model.ApplicationComponent{Name: "api", Namespace: "default", Image: "nginx:1.27", Traits: traits}
			result, err := tt.generate(component, &model.Properties{})
			require.Nil(t, result)
			require.ErrorContains(t, err, "conflicting additional object PersistentVolumeClaim/default/data")
		})
	}
}

func TestParsePropertiesReturnsDecodeError(t *testing.T) {
	for _, tt := range []struct {
		name      string
		input     *model.JSONStruct
		wantError bool
	}{
		{name: "nil"},
		{name: "empty", input: &model.JSONStruct{}},
		{name: "valid", input: &model.JSONStruct{"env": map[string]interface{}{"MODE": "test"}}},
		{name: "wrong field type", input: &model.JSONStruct{"ports": "invalid"}, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			properties, err := ParseProperties(tt.input)
			if tt.wantError {
				var typeError *json.UnmarshalTypeError
				require.ErrorAs(t, err, &typeError)
				require.Empty(t, properties)
			} else {
				require.NoError(t, err)
				if tt.name == "valid" {
					require.Equal(t, "test", properties.Env["MODE"])
				}
			}
		})
	}
	_, err := ParseProperties(&model.JSONStruct{"unsupported": make(chan int)})
	var unsupported *json.UnsupportedTypeError
	require.ErrorAs(t, err, &unsupported)
}
